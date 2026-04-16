import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "orq-finalizacion"
INPUT_TOPICS = ["evt.pixelation.completed", "cmd.storage"]
OUTPUT_TOPIC = "evt.storage.completed"
GROUP_ID = "o4-group"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bda_imagenes"),
        user=os.getenv("POSTGRES_USER", "bda_user"),
        password=os.getenv("POSTGRES_PASSWORD", "bda_pass"),
    )


def build_storage_event(input_event: dict, pixelation_applied: bool) -> dict:
    trace = input_event["event"]["trace"]
    payload = input_event["payload"]
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
            "input_bucket": payload.get("bucket", "imagenes-raw"),
            "input_object_key": payload.get("object_key", ""),
            "output_bucket": "imagenes-procesadas",
            "output_object_key": payload.get("object_key", ""),
            "pixelated_faces_count": payload.get("pixelated_faces_count", 0),
            "total_faces_count": payload.get("total_faces", 0),
            "pixelation_applied": pixelation_applied,
        },
    }


def process_message(input_event: dict, producer: KafkaProducer):
    trace = input_event["event"]["trace"]
    event_type = input_event["event"]["event_type"]
    payload = input_event["payload"]
    now = datetime.now(timezone.utc)
    pixelation_applied = event_type == "evt.pixelation.completed"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT Id_Solicitud
                FROM Solicitud
                WHERE GUID_Solicitud = %s
                ORDER BY Id_Solicitud DESC
                LIMIT 1
                """,
                (trace["request_id"],),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No existe Solicitud para request_id={trace['request_id']}")
            solicitud_id = row[0]

            if pixelation_applied:
                cur.execute(
                    """
                    UPDATE Solicitud
                    SET Fin_Pixelado = COALESCE(Fin_Pixelado, %s),
                        Inicio_Almacenamiento_Solicitud = COALESCE(Inicio_Almacenamiento_Solicitud, %s),
                        Fin_Almacenamiento_Solicitud = %s,
                        Fin_Solicitud = %s,
                        Estado = %s
                    WHERE Id_Solicitud = %s
                    """,
                    (now, now, now, now, "COMPLETED", solicitud_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE Solicitud
                    SET Inicio_Almacenamiento_Solicitud = COALESCE(Inicio_Almacenamiento_Solicitud, %s),
                        Fin_Almacenamiento_Solicitud = %s,
                        Fin_Solicitud = %s,
                        Estado = %s
                    WHERE Id_Solicitud = %s
                    """,
                    (now, now, now, "COMPLETED", solicitud_id),
                )
        conn.commit()

    storage_event = build_storage_event(input_event, pixelation_applied)
    producer.send(OUTPUT_TOPIC, storage_event).get(timeout=10)
    print(f"[o4] request_id={trace['request_id']} finalizado desde {event_type}")


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=bootstrap,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("[o4] Escuchando evt.pixelation.completed y cmd.storage...")
    for msg in consumer:
        try:
            process_message(msg.value, producer)
        except Exception as exc:
            print(f"[o4] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
