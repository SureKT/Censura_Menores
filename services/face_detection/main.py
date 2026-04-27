import json
import os

# Desactivar telemetría/sync de Ultralytics ANTES de importarla
os.environ["YOLO_CONFIG_DIR"] = "/app/.ultralytics"
os.environ["YOLO_TELEMETRY"] = "False"

import time
import urllib.request
import uuid
from datetime import datetime, timezone

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from ultralytics import YOLO


APP_NAME = "face-detection"
INPUT_TOPIC = "cmd.face_detection"
OUTPUT_TOPIC = "evt.face_detection.completed"
DLQ_TOPIC   = "events.dead_letter"
GROUP_ID = "face-detection-group"

YOLO_MODEL_PATH = os.getenv("YOLO_MODEL_PATH", "/app/yolov8n-face.pt")
YOLO_CONFIDENCE = float(os.getenv("YOLO_CONFIDENCE", "0.5"))

# URLs de descarga en orden de preferencia
YOLO_MODEL_URLS = [
    "https://github.com/akanametov/yolo-face/releases/download/1.0.0/yolov8n-face.pt",
    "https://huggingface.co/arnabdhar/YOLOv8-Face-Detection/resolve/main/model.pt",
]


def create_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def load_model() -> YOLO:
    if not os.path.exists(YOLO_MODEL_PATH):
        print(f"[face_detection] Descargando modelo YOLOv8-face en {YOLO_MODEL_PATH}...")
        last_exc = None
        for url in YOLO_MODEL_URLS:
            try:
                urllib.request.urlretrieve(url, YOLO_MODEL_PATH)
                print(f"[face_detection] Modelo descargado desde {url}")
                break
            except Exception as exc:
                print(f"[face_detection] Fallo en {url}: {exc}")
                last_exc = exc
        else:
            raise RuntimeError(f"No se pudo descargar el modelo YOLO: {last_exc}") from last_exc
    model = YOLO(YOLO_MODEL_PATH)
    print(f"[face_detection] YOLOv8-face cargado (conf={YOLO_CONFIDENCE}).")
    return model


def load_image_from_minio(minio_client: Minio, bucket: str, object_key: str) -> np.ndarray:
    response = minio_client.get_object(bucket, object_key)
    try:
        image_bytes = response.read()
    finally:
        response.close()
        response.release_conn()
    nparr = np.frombuffer(image_bytes, np.uint8)
    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError(f"No se pudo decodificar la imagen: {object_key}")
    return image


def detect_faces(model: YOLO, image: np.ndarray) -> list[dict]:
    results = model(image, conf=YOLO_CONFIDENCE, verbose=False)
    faces = []
    for result in results:
        for box in result.boxes.xyxy.tolist():
            x1, y1, x2, y2 = box
            faces.append(
                {
                    "x": int(x1),
                    "y": int(y1),
                    "width": int(x2 - x1),
                    "height": int(y2 - y1),
                }
            )
    return faces


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


def build_output_event(cmd_event: dict, faces: list[dict]) -> dict:
    trace = cmd_event["event"]["trace"]
    payload = cmd_event["payload"]
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": OUTPUT_TOPIC,
            "event_version": "v1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "trace": {
                "request_id": trace["request_id"],
                "image_id": trace["image_id"],
            },
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
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    model = load_model()
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
            print(f"[face_detection] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError("[face_detection] No se pudo conectar a Kafka.")

    print(f"[face_detection] Escuchando {INPUT_TOPIC}...")
    for msg in consumer:
        try:
            cmd_event = msg.value
            payload = cmd_event["payload"]
            image = load_image_from_minio(minio_client, payload["bucket"], payload["object_key"])
            faces = detect_faces(model, image)
            output_event = build_output_event(cmd_event, faces)
            producer.send(OUTPUT_TOPIC, output_event).get(timeout=10)
            request_id = cmd_event["event"]["trace"]["request_id"]
            print(f"[face_detection] {len(faces)} rostros detectados para request_id={request_id}")
        except Exception as exc:
            print(f"[face_detection] Error procesando mensaje: {exc}")
            send_to_dlq(producer, msg.value, exc)
            time.sleep(1)


if __name__ == "__main__":
    run()
