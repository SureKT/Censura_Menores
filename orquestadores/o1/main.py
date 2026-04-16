import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "orq-entrada"
INPUT_TOPIC = "images.raw"
OUTPUT_TOPIC = "cmd.face_detection"
GROUP_ID = "o1-group"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bda_imagenes"),
        user=os.getenv("POSTGRES_USER", "bda_user"),
        password=os.getenv("POSTGRES_PASSWORD", "bda_pass"),
    )


def build_output_command(raw_event: dict) -> dict:
    trace = raw_event["event"]["trace"]
    payload = raw_event["payload"]
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
            "content_type": payload.get("content_type", "image/jpeg"),
            "size_bytes": payload.get("size_bytes", 0),
        },
    }


def process_message(raw_event: dict, producer: KafkaProducer):
    trace = raw_event["event"]["trace"]
    payload = raw_event["payload"]
    now = datetime.now(timezone.utc)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(Id_Solicitud), 0) + 1 FROM Solicitud")
            next_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO Solicitud (
                    Id_Solicitud, GUID_Solicitud, Id_Fichero, Inicio_Solicitud, Estado
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    next_id,
                    trace["request_id"],
                    payload["object_key"],
                    now,
                    "RECIBIDO",
                ),
            )
        conn.commit()

    output_cmd = build_output_command(raw_event)
    producer.send(OUTPUT_TOPIC, output_cmd).get(timeout=10)
    print(f"[o1] Solicitud {trace['request_id']} registrada y comando publicado.")


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

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

    print("[o1] Escuchando images.raw...")
    for msg in consumer:
        try:
            process_message(msg.value, producer)
        except Exception as exc:
            print(f"[o1] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
