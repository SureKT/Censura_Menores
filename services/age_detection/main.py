import io
import json
import math
import os
import time
import uuid
from datetime import datetime, timezone

import torch
from minio import Minio
from PIL import Image
from torchvision import transforms
from kafka import KafkaConsumer, KafkaProducer

from model import build_model


APP_NAME = "age-detection"
INPUT_TOPIC = "cmd.age_detection"
OUTPUT_TOPIC = "evt.age_detection.completed"
DLQ_TOPIC   = "events.dead_letter"
GROUP_ID = "age-detection-group"

# ── Configuracion de inferencia ───────────────────────────────────────────────
# Umbral conservador: clasificar como menor si edad predicha < MINOR_THRESHOLD.
# 22 en lugar de 18 añade margen de seguridad frente a subestimaciones del modelo.
MINOR_THRESHOLD = int(os.getenv("MINOR_THRESHOLD", "22"))

# Test-Time Augmentation: promedia N pasadas con distintas transformaciones.
# Reduce la varianza de la prediccion en imagenes dificiles (angulo, iluminacion).
TTA_PASSES = int(os.getenv("TTA_PASSES", "5"))

_MEAN = [0.485, 0.456, 0.406]
_STD  = [0.229, 0.224, 0.225]

# Las 5 transformaciones TTA — de menor a mayor perturbacion
_TTA_TRANSFORMS = [
    transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(_MEAN, _STD),
    ]),
    transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.RandomHorizontalFlip(p=1.0),
        transforms.ToTensor(),
        transforms.Normalize(_MEAN, _STD),
    ]),
    transforms.Compose([
        transforms.Resize((256, 256)),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(_MEAN, _STD),
    ]),
    transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ColorJitter(brightness=0.2, contrast=0.1),
        transforms.ToTensor(),
        transforms.Normalize(_MEAN, _STD),
    ]),
    transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.Grayscale(num_output_channels=3),
        transforms.ToTensor(),
        transforms.Normalize(_MEAN, _STD),
    ]),
]

# Transform para inferencia sin TTA (TTA_PASSES=1)
TRANSFORM = _TTA_TRANSFORMS[0]


def predict_age_tta(model: torch.nn.Module, pil_crop: Image.Image) -> tuple[int, float]:
    """
    Predice la edad promediando TTA_PASSES augmentaciones distintas.
    Con TTA_PASSES=1 equivale a inferencia estandar (sin overhead).
    """
    raws = []
    passes = min(TTA_PASSES, len(_TTA_TRANSFORMS))
    for t in _TTA_TRANSFORMS[:passes]:
        tensor = t(pil_crop).unsqueeze(0)
        with torch.no_grad():
            raws.append(model(tensor).squeeze().item())
    raw_mean = sum(raws) / len(raws)
    age = int(max(0, min(120, round(raw_mean))))
    confidence = round(1.0 / (1.0 + math.exp(-abs(age - MINOR_THRESHOLD) / 4.0)), 4)
    return age, confidence


def send_to_dlq(producer, original_msg: dict, error: Exception):
    try:
        producer.send(DLQ_TOPIC, {
            "event": {
                "event_id": str(uuid.uuid4()),
                "event_type": DLQ_TOPIC,
                "event_version": "v1",
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "source": APP_NAME,
            },
            "error": {"type": type(error).__name__, "message": str(error)},
            "original_message": original_msg,
        }).get(timeout=5)
    except Exception as dlq_exc:
        print(f"[{APP_NAME}] No se pudo enviar a DLQ: {dlq_exc}")


def load_model(path: str):
    if not os.path.exists(path):
        print(f"[age_detection] Modelo {path} no encontrado. Fallback: todas las caras se marcan adulto.")
        return None
    model = build_model()
    model.load_state_dict(torch.load(path, map_location="cpu", weights_only=True))
    model.eval()
    return model


def create_minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def fetch_image(minio_client: Minio, bucket: str, object_key: str) -> Image.Image:
    response = minio_client.get_object(bucket, object_key)
    data = response.read()
    response.close()
    response.release_conn()
    buf = io.BytesIO(data)
    img = Image.open(buf)
    img.load()
    return img.convert("RGB")


def crop_face(img: Image.Image, face: dict, padding: float = 0.1) -> Image.Image:
    iw, ih = img.size
    x, y, fw, fh = face["x"], face["y"], face["width"], face["height"]
    pad = int(padding * max(fw, fh))
    x1 = max(0, x - pad)
    y1 = max(0, y - pad)
    x2 = min(iw, x + fw + pad)
    y2 = min(ih, y + fh + pad)
    return img.crop((x1, y1, x2, y2))


def process_message(cmd: dict, model, minio_client: Minio) -> list[dict]:
    payload = cmd["payload"]
    img = fetch_image(minio_client, payload["bucket"], payload["object_key"])

    results = []
    for face in payload.get("faces", []):
        crop = crop_face(img, face)
        if model is None:
            results.append({**face, "estimated_age": 30, "is_minor": False, "confidence": 0.0})
        else:
            age, confidence = predict_age_tta(model, crop)
            results.append({**face, "estimated_age": age, "is_minor": age < MINOR_THRESHOLD, "confidence": confidence})
    return results


def build_output_event(cmd: dict, faces: list[dict]) -> dict:
    trace = cmd["event"]["trace"]
    payload = cmd["payload"]
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": OUTPUT_TOPIC,
            "event_version": "v1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "trace": {"request_id": trace["request_id"], "image_id": trace["image_id"]},
            "source": APP_NAME,
        },
        "payload": {
            "bucket": payload["bucket"],
            "object_key": payload["object_key"],
            "faces": faces,
            "total_faces": len(faces),
        },
    }


def run():
    model_path = os.getenv("MODEL_PATH", "age_model.pth")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    print("[age_detection] Cargando modelo...")
    model = load_model(model_path)
    print(f"[age_detection] Modelo {'cargado' if model else 'NO cargado, usando fallback'}.")
    print(f"[age_detection] MINOR_THRESHOLD={MINOR_THRESHOLD}  TTA_PASSES={TTA_PASSES}")

    minio_client = create_minio_client()

    producer = consumer = None
    for attempt in range(12):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=bootstrap,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            break
        except Exception as exc:
            print(f"[age_detection] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError("[age_detection] No se pudo conectar a Kafka.")

    print("[age_detection] Escuchando cmd.age_detection...")
    for msg in consumer:
        t_start = time.time()
        faces = None
        try:
            faces = process_message(msg.value, model, minio_client)
        except Exception as exc:
            print(f"[age_detection] Error procesando imagen: {exc}. Publicando evento sin estimación de edad.")
            faces = [
                {**face, "estimated_age": 30, "is_minor": False, "confidence": -1.0}
                for face in msg.value.get("payload", {}).get("faces", [])
            ]

        try:
            output = build_output_event(msg.value, faces)
            producer.send(OUTPUT_TOPIC, output).get(timeout=10)
            rid = output["event"]["trace"]["request_id"]
            minors = sum(1 for f in faces if f["is_minor"])
            elapsed_ms = (time.time() - t_start) * 1000
            print(f"[age_detection] {rid} → {len(faces)} caras, {minors} menores ({elapsed_ms:.0f}ms)")
        except Exception as exc:
            print(f"[age_detection] Error publicando evento: {exc}")
            send_to_dlq(producer, msg.value, exc)
            time.sleep(1)


if __name__ == "__main__":
    run()
