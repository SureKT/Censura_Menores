import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "orq-analisis"
INPUT_TOPIC = "evt.face_detection.completed"
OUTPUT_TOPIC = "cmd.age_detection"
GROUP_ID = "o2-group"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bda_imagenes"),
        user=os.getenv("POSTGRES_USER", "bda_user"),
        password=os.getenv("POSTGRES_PASSWORD", "bda_pass"),
    )


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


def process_message(face_event: dict, producer: KafkaProducer):
    trace = face_event["event"]["trace"]
    payload = face_event["payload"]
    faces = payload.get("faces", [])
    total_faces = payload.get("total_faces", len(faces))
    now = datetime.now(timezone.utc)

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

            cur.execute("SELECT COALESCE(MAX(Id_Imagen), 0) FROM Imagenes")
            current_max_img_id = cur.fetchone()[0]
            for idx, _ in enumerate(faces, start=1):
                cur.execute(
                    """
                    INSERT INTO Imagenes (Id_Imagen, Id_Solicitud, Estado)
                    VALUES (%s, %s, %s)
                    """,
                    (current_max_img_id + idx, solicitud_id, "DETECTADA"),
                )

            cur.execute(
                """
                UPDATE Solicitud
                SET Inicio_Deteccion_Caras = COALESCE(Inicio_Deteccion_Caras, %s),
                    Fin_Deteccion_Caras = %s,
                    Num_Imagenes_Total = %s,
                    Inicio_Edad = %s,
                    Estado = %s
                WHERE Id_Solicitud = %s
                """,
                (now, now, total_faces, now, "EN_ANALISIS_EDAD", solicitud_id),
            )
        conn.commit()

    output_cmd = build_age_command(face_event)
    producer.send(OUTPUT_TOPIC, output_cmd).get(timeout=10)
    print(f"[o2] request_id={trace['request_id']} actualizado y cmd.age_detection publicado.")


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

    print("[o2] Escuchando evt.face_detection.completed...")
    for msg in consumer:
        try:
            process_message(msg.value, producer)
        except Exception as exc:
            print(f"[o2] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
