import base64
import io
import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from contextlib import contextmanager
from psycopg2 import pool as pg_pool
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from minio import Minio


APP_NAME = "api-ingesta"
OUTPUT_TOPIC = "cmd.face_detection"
REALTIME_TOPIC = "cmd.realtime.classification"
MAX_REALTIME_BYTES = 300 * 1024  # 300 KB por crop de cara


def create_kafka_producer() -> KafkaProducer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    for attempt in range(12):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as exc:
            print(f"[api1] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    raise RuntimeError("[api1] No se pudo conectar a Kafka.")


def create_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


_pool: pg_pool.ThreadedConnectionPool | None = None


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pg_pool.ThreadedConnectionPool(
            1, 10,
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "bda_imagenes"),
            user=os.getenv("POSTGRES_USER", "bda_user"),
            password=os.getenv("POSTGRES_PASSWORD", "bda_pass"),
        )
    return _pool


@contextmanager
def get_db_connection():
    conn = _get_pool().getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def build_face_detection_command(
    request_id: str, image_id: str, bucket: str, object_key: str, content_type: str, size_bytes: int
) -> dict:
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": OUTPUT_TOPIC,
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


app = FastAPI(title="API Ingesta")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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

    max_bytes = int(os.getenv("MAX_IMAGE_BYTES", str(10 * 1024 * 1024)))
    if len(content) > max_bytes:
        raise HTTPException(status_code=413, detail=f"Imagen demasiado grande (máx {max_bytes // (1024*1024)} MB).")

    # 1. Generar IDs
    request_id = str(uuid.uuid4())
    image_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # 2. Subir imagen original a MinIO
    object_key = f"raw/{request_id}-{file.filename}"
    minio_client.put_object(
        raw_bucket,
        object_key,
        io.BytesIO(content),
        len(content),
        content_type=file.content_type,
    )
    url_original = f"{raw_bucket}/{object_key}"

    # 3. Insertar registro en BD
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO Solicitud (
                    GUID_Solicitud, URL_Imagen_Original, Inicio_Solicitud, Estado
                ) VALUES (%s, %s, %s, %s)
                """,
                (request_id, url_original, now, "CREADA"),
            )
        conn.commit()

    # 4. Publicar cmd.face_detection directamente
    cmd = build_face_detection_command(
        request_id=request_id,
        image_id=image_id,
        bucket=raw_bucket,
        object_key=object_key,
        content_type=file.content_type,
        size_bytes=len(content),
    )
    producer.send(OUTPUT_TOPIC, cmd).get(timeout=10)

    print(f"[api1] Solicitud {request_id} registrada → cmd.face_detection publicado.")

    return {
        "message": "Imagen recibida, registrada y enviada a detección de caras.",
        "request_id": request_id,
        "image_id": image_id,
        "bucket": raw_bucket,
        "object_key": object_key,
    }


def build_realtime_command(session_id: str, face_token: str, image_b64: str) -> dict:
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": REALTIME_TOPIC,
            "event_version": "v1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "source": APP_NAME,
        },
        "payload": {
            "session_id": session_id,
            "face_token": face_token,
            "image_b64": image_b64,
        },
    }


@app.post("/realtime/faces", status_code=202)
async def submit_realtime_face(
    session_id: str = Form(...),
    face_token: str = Form(...),
    image: UploadFile = File(...),
) -> dict:
    """Recibe un crop de cara desde la camara del navegador y lo publica a Kafka.

    El frontend hace tracking client-side y solo invoca este endpoint cuando aparece
    una cara NUEVA (un face_token nuevo). La respuesta se entrega asincronamente via
    SSE en API2 (/realtime/stream/{session_id}).
    """
    if not image.content_type or not image.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Solo se aceptan crops de imagen.")

    content = await image.read()
    if not content:
        raise HTTPException(status_code=400, detail="Crop vacio.")
    if len(content) > MAX_REALTIME_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Crop demasiado grande (max {MAX_REALTIME_BYTES // 1024} KB).",
        )

    image_b64 = base64.b64encode(content).decode("ascii")
    cmd = build_realtime_command(session_id, face_token, image_b64)
    producer.send(REALTIME_TOPIC, cmd)

    return {
        "accepted": True,
        "session_id": session_id,
        "face_token": face_token,
    }
