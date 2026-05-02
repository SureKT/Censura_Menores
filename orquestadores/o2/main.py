import io
import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from contextlib import contextmanager
from psycopg2 import pool as pg_pool
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from PIL import Image


APP_NAME = "orq-analisis"
INPUT_TOPIC = "evt.face_detection.completed"
OUTPUT_TOPIC = "cmd.age_detection"
DLQ_TOPIC   = "events.dead_letter"
GROUP_ID = "o2-group"

_pool: pg_pool.SimpleConnectionPool | None = None


def _get_pool() -> pg_pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = pg_pool.SimpleConnectionPool(
            1, 3,
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


def create_minio_client() -> Minio:
    endpoint   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure     = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


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


def crop_and_upload_face(
    minio_client: Minio,
    image: Image.Image,
    face: dict,
    guid: str,
    face_id: int,
    out_bucket: str,
) -> str:
    """Recorta una cara con 10% de padding y la sube a MinIO. Devuelve la URL."""
    img_w, img_h = image.size
    x, y, w, h = face.get("x", 0), face.get("y", 0), face.get("width", 0), face.get("height", 0)
    pad_x = int(w * 0.10)
    pad_y = int(h * 0.10)
    left   = max(0, x - pad_x)
    top    = max(0, y - pad_y)
    right  = min(img_w, x + w + pad_x)
    bottom = min(img_h, y + h + pad_y)

    crop = image.crop((left, top, right, bottom))
    buf = io.BytesIO()
    crop.save(buf, format="JPEG", quality=90)
    buf_bytes = buf.getvalue()

    object_key = f"caras/{guid}/{face_id}.jpg"
    minio_client.put_object(
        out_bucket,
        object_key,
        io.BytesIO(buf_bytes),
        len(buf_bytes),
        content_type="image/jpeg",
    )
    return f"{out_bucket}/{object_key}"


def build_age_command(face_event: dict) -> dict:
    trace = face_event["event"]["trace"]
    payload = face_event["payload"]
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
            "faces": payload.get("faces", []),
            "total_faces": payload.get("total_faces", 0),
        },
    }


def process_message(face_event: dict, producer: KafkaProducer, minio_client: Minio):
    t_start = time.time()
    trace   = face_event["event"]["trace"]
    payload = face_event["payload"]
    guid    = trace["request_id"]
    faces   = payload.get("faces", [])
    total_faces = payload.get("total_faces", len(faces))
    now     = datetime.now(timezone.utc)
    out_bucket = os.getenv("MINIO_PROCESSED_BUCKET", "imagenes-procesadas")

    # Descargar imagen original para generar crops de cada cara
    response = minio_client.get_object(payload["bucket"], payload["object_key"])
    try:
        image_bytes = response.read()
    finally:
        response.close()
        response.release_conn()
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(Id_Imagen), 0) FROM Imagenes WHERE GUID_Solicitud = %s",
                (guid,),
            )
            current_max = cur.fetchone()[0]

            for idx, face in enumerate(faces, start=1):
                face_id = current_max + idx
                try:
                    url_cara = crop_and_upload_face(
                        minio_client, image, face, guid, face_id, out_bucket
                    )
                except Exception as crop_exc:
                    print(f"[o2] {guid}: error guardando cara {face_id}: {crop_exc}")
                    url_cara = None

                cur.execute(
                    """
                    INSERT INTO Imagenes (
                        GUID_Solicitud, Id_Imagen, URL_Imagen,
                        Imagen_X, Imagen_Y, Imagen_Ancho, Imagen_Alto
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        guid,
                        face_id,
                        url_cara,
                        face.get("x", 0),
                        face.get("y", 0),
                        face.get("width", 0),
                        face.get("height", 0),
                    ),
                )

            cur.execute(
                """
                UPDATE Solicitud
                SET Inicio_Deteccion_Caras = COALESCE(Inicio_Deteccion_Caras, %s),
                    Fin_Deteccion_Caras = %s,
                    Inicio_Edad = %s,
                    Estado = %s
                WHERE GUID_Solicitud = %s
                """,
                (now, now, now, "CARAS_DETECTADAS", guid),
            )
        conn.commit()

    output_cmd = build_age_command(face_event)
    producer.send(OUTPUT_TOPIC, output_cmd).get(timeout=10)

    elapsed_ms = (time.time() - t_start) * 1000
    print(f"[o2] {guid}: {total_faces} caras registradas y recortadas → cmd.age_detection ({elapsed_ms:.0f}ms)")


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
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
            print(f"[o2] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError("[o2] No se pudo conectar a Kafka.")

    print("[o2] Escuchando evt.face_detection.completed...")
    for msg in consumer:
        try:
            process_message(msg.value, producer, minio_client)
        except Exception as exc:
            print(f"[o2] Error procesando mensaje: {exc}")
            send_to_dlq(producer, msg.value, exc)
            time.sleep(1)


if __name__ == "__main__":
    run()
