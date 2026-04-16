import json
import io
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from PIL import Image


APP_NAME = "pixelation"
INPUT_TOPIC = "cmd.pixelation"
OUTPUT_TOPIC = "evt.pixelation.completed"
GROUP_ID = "pixelation-group"


def create_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def pixelate_region(image: Image.Image, x: int, y: int, width: int, height: int):
    if width <= 0 or height <= 0:
        return
    img_w, img_h = image.size
    left = max(0, min(x, img_w - 1))
    top = max(0, min(y, img_h - 1))
    right = max(left + 1, min(x + width, img_w))
    bottom = max(top + 1, min(y + height, img_h))
    region = image.crop((left, top, right, bottom))
    downscaled = region.resize((max(1, region.width // 12), max(1, region.height // 12)))
    pixelated = downscaled.resize(region.size, Image.Resampling.NEAREST)
    image.paste(pixelated, (left, top, right, bottom))


def process_image(cmd_event: dict, minio_client: Minio) -> tuple[str, str, str, int]:
    payload = cmd_event["payload"]
    input_bucket = payload["bucket"]
    input_key = payload["object_key"]
    output_bucket = os.getenv("MINIO_PROCESSED_BUCKET", "imagenes-procesadas")
    output_key = f"processed/{uuid.uuid4()}-{os.path.basename(input_key)}"

    response = minio_client.get_object(input_bucket, input_key)
    try:
        image_bytes = response.read()
    finally:
        response.close()
        response.release_conn()

    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    faces = payload.get("faces", [])
    pixelated_faces = 0
    for face in faces:
        if face.get("is_minor"):
            pixelate_region(
                image,
                int(face.get("x", 0)),
                int(face.get("y", 0)),
                int(face.get("width", 0)),
                int(face.get("height", 0)),
            )
            pixelated_faces += 1

    output_buffer = io.BytesIO()
    image.save(output_buffer, format="JPEG", quality=90)
    output_bytes = output_buffer.getvalue()
    minio_client.put_object(
        output_bucket,
        output_key,
        io.BytesIO(output_bytes),
        len(output_bytes),
        content_type="image/jpeg",
    )
    return input_bucket, output_bucket, output_key, pixelated_faces


def build_output_event(cmd_event: dict) -> dict:
    trace = cmd_event["event"]["trace"]
    payload = cmd_event["payload"]
    faces = payload.get("faces", [])
    pixelated_faces = [face for face in faces if face.get("is_minor")]
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
            "pixelated_faces_count": len(pixelated_faces),
            "total_faces": payload.get("total_faces", len(faces)),
            "minors_count": payload.get("minors_count", len(pixelated_faces)),
        },
    }


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    minio_client = create_minio_client()
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=bootstrap,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("[pixelation] Escuchando cmd.pixelation...")
    for msg in consumer:
        try:
            input_bucket, output_bucket, output_key, pixelated_faces = process_image(
                msg.value, minio_client
            )
            output_event = build_output_event(msg.value)
            output_event["payload"]["input_bucket"] = input_bucket
            output_event["payload"]["output_bucket"] = output_bucket
            output_event["payload"]["output_object_key"] = output_key
            output_event["payload"]["pixelated_faces_count"] = pixelated_faces
            producer.send(OUTPUT_TOPIC, output_event).get(timeout=10)
            print(
                f"[pixelation] Evento emitido para request_id="
                f"{output_event['event']['trace']['request_id']}"
            )
        except Exception as exc:
            print(f"[pixelation] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
