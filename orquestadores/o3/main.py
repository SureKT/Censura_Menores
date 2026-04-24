import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from contextlib import contextmanager
from psycopg2 import pool as pg_pool
from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "orq-decision"
DLQ_TOPIC   = "events.dead_letter"
INPUT_TOPIC = "evt.age_detection.completed"
PIXELATION_TOPIC = "cmd.pixelation"
STORAGE_TOPIC = "cmd.storage"
GROUP_ID = "o3-group"

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


def build_command(age_event: dict, target_topic: str, minors_count: int) -> dict:
    trace = age_event["event"]["trace"]
    payload = age_event["payload"]
    return {
        "event": {
            "event_id": str(uuid.uuid4()),
            "event_type": target_topic,
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
            "minors_count": minors_count,
        },
    }


def process_message(age_event: dict, producer: KafkaProducer):
    trace = age_event["event"]["trace"]
    guid = trace["request_id"]
    faces = age_event["payload"].get("faces", [])
    minors_count = sum(1 for f in faces if f.get("is_minor"))
    now = datetime.now(timezone.utc)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Actualizar Mayor_18 y score en cada fila de Imagenes
            for idx, face in enumerate(faces, start=1):
                mayor_18 = not face.get("is_minor", False)
                # score: confianza de la prediccion (el stub no la provee; se usa 0.9)
                score = face.get("confidence", 0.9)
                cur.execute(
                    """
                    UPDATE Imagenes
                    SET Mayor_18 = %s, score = %s
                    WHERE GUID_Solicitud = %s
                      AND Id_Imagen = (
                          SELECT Id_Imagen FROM Imagenes
                          WHERE GUID_Solicitud = %s
                          ORDER BY Id_Imagen
                          OFFSET %s LIMIT 1
                      )
                    """,
                    (mayor_18, score, guid, guid, idx - 1),
                )

            if minors_count > 0:
                cur.execute(
                    """
                    UPDATE Solicitud
                    SET Fin_edad = %s,
                        Inicio_Pixelado = %s,
                        Estado = %s
                    WHERE GUID_Solicitud = %s
                    """,
                    (now, now, "PENDIENTE_PIXELADO", guid),
                )
            else:
                cur.execute(
                    """
                    UPDATE Solicitud
                    SET Fin_edad = %s,
                        Estado = %s
                    WHERE GUID_Solicitud = %s
                    """,
                    (now, "PENDIENTE_STORAGE", guid),
                )
        conn.commit()

    target_topic = PIXELATION_TOPIC if minors_count > 0 else STORAGE_TOPIC
    output_cmd = build_command(age_event, target_topic, minors_count)
    producer.send(target_topic, output_cmd).get(timeout=10)
    print(f"[o3] {guid} -> {target_topic} ({minors_count} menores)")


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

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
            print(f"[o3] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError("[o3] No se pudo conectar a Kafka.")

    print("[o3] Escuchando evt.age_detection.completed...")
    for msg in consumer:
        try:
            process_message(msg.value, producer)
        except Exception as exc:
            print(f"[o3] Error procesando mensaje: {exc}")
            send_to_dlq(producer, msg.value, exc)
            time.sleep(1)


if __name__ == "__main__":
    run()
