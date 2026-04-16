import io
import json
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, File, HTTPException, UploadFile
from kafka import KafkaProducer
from minio import Minio


APP_NAME = "api-ingesta"
RAW_TOPIC = "images.raw"


def build_event(bucket: str, object_key: str, content_type: str, size_bytes: int) -> dict:
    request_id = str(uuid.uuid4())
    image_id = str(uuid.uuid4())
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": RAW_TOPIC,
            "event_version": "v1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "trace": {
                "request_id": request_id,
                "image_id": image_id,
            },
            "source": APP_NAME,
        },
        "payload": {
            "bucket": bucket,
            "object_key": object_key,
            "content_type": content_type,
            "size_bytes": size_bytes,
        },
    }


def create_kafka_producer() -> KafkaProducer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def create_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


app = FastAPI(title="API Ingesta")
producer = create_kafka_producer()
minio_client = create_minio_client()
raw_bucket = os.getenv("MINIO_RAW_BUCKET", "imagenes-raw")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": APP_NAME}


@app.post("/images")
async def upload_image(file: UploadFile = File(...)) -> dict:
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos de imagen.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Archivo vacio.")

    object_key = f"raw/{uuid.uuid4()}-{file.filename}"
    minio_client.put_object(
        raw_bucket,
        object_key,
        io.BytesIO(content),
        len(content),
        content_type=file.content_type,
    )

    event = build_event(
        bucket=raw_bucket,
        object_key=object_key,
        content_type=file.content_type,
        size_bytes=len(content),
    )
    producer.send(RAW_TOPIC, event).get(timeout=10)

    return {
        "message": "Imagen recibida y publicada en images.raw",
        "request_id": event["event"]["trace"]["request_id"],
        "image_id": event["event"]["trace"]["image_id"],
        "bucket": raw_bucket,
        "object_key": object_key,
    }
