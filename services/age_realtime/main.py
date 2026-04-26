"""Servicio ligero de clasificacion de edad para flujo en tiempo real.

Consume `cmd.realtime.classification` con crops de cara ya recortados (base64 JPEG/PNG),
ejecuta MobileNetV2 y publica `evt.realtime.classification.completed`.

No toca PostgreSQL ni MinIO: todo en memoria. Pensado para latencias bajas.
"""

import base64
import io
import json
import os
import time
import uuid
from datetime import datetime, timezone

import torch
from PIL import Image
from torchvision import transforms
from kafka import KafkaConsumer, KafkaProducer

from model import build_model, predict_age


APP_NAME = "age-realtime"
INPUT_TOPIC = "cmd.realtime.classification"
OUTPUT_TOPIC = "evt.realtime.classification.completed"
DLQ_TOPIC = "events.dead_letter"
GROUP_ID = "age-realtime-group"

TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])


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
        })
    except Exception as dlq_exc:
        print(f"[{APP_NAME}] No se pudo enviar a DLQ: {dlq_exc}")


def load_model(path: str):
    """Carga el modelo entrenado; si no existe, devuelve None (modo fallback heuristico)."""
    if not os.path.exists(path):
        print(f"[{APP_NAME}] Modelo {path} no encontrado. Fallback heuristico activo.")
        return None
    model = build_model()
    model.load_state_dict(torch.load(path, map_location="cpu", weights_only=True))
    model.eval()
    return model


def classify(image_b64: str, model) -> tuple[int, bool, float]:
    """Decodifica el crop y devuelve (edad, is_minor, confidence)."""
    raw = base64.b64decode(image_b64)
    img = Image.open(io.BytesIO(raw)).convert("RGB")
    if model is None:
        # Fallback: asumir adulto con baja confianza. Evita falsos positivos sin modelo.
        return 30, False, 0.0
    tensor = TRANSFORM(img).unsqueeze(0)
    age, confidence = predict_age(model, tensor)
    return age, age < 18, confidence


def build_output_event(cmd: dict, age: int, is_minor: bool, confidence: float) -> dict:
    payload_in = cmd.get("payload", {})
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": OUTPUT_TOPIC,
            "event_version": "v1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "source": APP_NAME,
        },
        "payload": {
            "session_id": payload_in.get("session_id"),
            "face_token": payload_in.get("face_token"),
            "estimated_age": age,
            "is_minor": is_minor,
            "confidence": confidence,
        },
    }


def run():
    model_path = os.getenv("MODEL_PATH", "age_model.pth")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    print(f"[{APP_NAME}] Cargando modelo...")
    model = load_model(model_path)
    print(f"[{APP_NAME}] Modelo {'cargado' if model else 'NO cargado, usando fallback'}.")

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
                auto_offset_reset="latest",  # tiempo real: no rebotar eventos viejos
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            break
        except Exception as exc:
            print(f"[{APP_NAME}] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError(f"[{APP_NAME}] No se pudo conectar a Kafka.")

    print(f"[{APP_NAME}] Escuchando {INPUT_TOPIC}...")
    for msg in consumer:
        try:
            payload = msg.value.get("payload", {})
            image_b64 = payload.get("image_b64", "")
            if not image_b64:
                raise ValueError("payload.image_b64 vacio")
            age, is_minor, confidence = classify(image_b64, model)
            output = build_output_event(msg.value, age, is_minor, confidence)
            producer.send(OUTPUT_TOPIC, output)
            sid = payload.get("session_id", "?")
            tok = payload.get("face_token", "?")
            print(f"[{APP_NAME}] {sid}/{tok} -> edad={age} menor={is_minor} conf={confidence}")
        except Exception as exc:
            print(f"[{APP_NAME}] Error procesando mensaje: {exc}")
            send_to_dlq(producer, msg.value, exc)


if __name__ == "__main__":
    run()
