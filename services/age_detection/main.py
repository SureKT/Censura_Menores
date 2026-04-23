import io
import json
import os
import time
import uuid
from datetime import datetime, timezone

import torch
from minio import Minio
from PIL import Image
from torchvision import transforms
from kafka import KafkaConsumer, KafkaProducer

from model import build_model, predict_age


APP_NAME = "age-detection"
INPUT_TOPIC = "cmd.age_detection"
OUTPUT_TOPIC = "evt.age_detection.completed"
GROUP_ID = "age-detection-group"

TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])


def load_model(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Modelo no encontrado: {path}. Entrena primero con: python train.py"
        )
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
        tensor = TRANSFORM(crop).unsqueeze(0)
        age, confidence = predict_age(model, tensor)
        results.append({**face, "estimated_age": age, "is_minor": age < 18, "confidence": confidence})
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
    print("[age_detection] Modelo cargado.")

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
        faces = None
        try:
            faces = process_message(msg.value, model, minio_client)
        except Exception as exc:
            print(f"[age_detection] Error procesando imagen: {exc}. Publicando evento sin estimación de edad.")
            # Pasar las caras originales sin estimación (adultos por defecto para no pixelar en falso)
            faces = [
                {**face, "estimated_age": 30, "is_minor": False, "confidence": -1.0}
                for face in msg.value.get("payload", {}).get("faces", [])
            ]

        try:
            output = build_output_event(msg.value, faces)
            producer.send(OUTPUT_TOPIC, output).get(timeout=10)
            rid = output["event"]["trace"]["request_id"]
            minors = sum(1 for f in faces if f["is_minor"])
            print(f"[age_detection] {rid} → {len(faces)} caras, {minors} menores")
        except Exception as exc:
            print(f"[age_detection] Error publicando evento: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
