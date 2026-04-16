import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "orq-decision"
INPUT_TOPIC = "evt.age_detection.completed"
PIXELATION_TOPIC = "cmd.pixelation"
STORAGE_TOPIC = "cmd.storage"
GROUP_ID = "o3-group"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bda_imagenes"),
        user=os.getenv("POSTGRES_USER", "bda_user"),
        password=os.getenv("POSTGRES_PASSWORD", "bda_pass"),
    )


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
    faces = age_event["payload"].get("faces", [])
    minors_count = sum(1 for face in faces if face.get("is_minor"))
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

            for idx, face in enumerate(faces, start=1):
                estado_imagen = "MENOR" if face.get("is_minor") else "ADULTO"
                cur.execute(
                    """
                    UPDATE Imagenes
                    SET Estado = %s
                    WHERE Id_Solicitud = %s
                      AND Id_Imagen = (
                        SELECT Id_Imagen
                        FROM Imagenes
                        WHERE Id_Solicitud = %s
                        ORDER BY Id_Imagen
                        OFFSET %s LIMIT 1
                      )
                    """,
                    (estado_imagen, solicitud_id, solicitud_id, idx - 1),
                )

            if minors_count > 0:
                cur.execute(
                    """
                    UPDATE Solicitud
                    SET Fin_Edad = %s,
                        Num_Imagenes_Pixeladas = %s,
                        Inicio_Pixelado = %s,
                        Estado = %s
                    WHERE Id_Solicitud = %s
                    """,
                    (now, minors_count, now, "PENDIENTE_PIXELADO", solicitud_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE Solicitud
                    SET Fin_Edad = %s,
                        Num_Imagenes_Pixeladas = 0,
                        Estado = %s
                    WHERE Id_Solicitud = %s
                    """,
                    (now, "PENDIENTE_STORAGE", solicitud_id),
                )
        conn.commit()

    target_topic = PIXELATION_TOPIC if minors_count > 0 else STORAGE_TOPIC
    output_cmd = build_command(age_event, target_topic, minors_count)
    producer.send(target_topic, output_cmd).get(timeout=10)
    print(f"[o3] request_id={trace['request_id']} -> {target_topic}")


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

    print("[o3] Escuchando evt.age_detection.completed...")
    for msg in consumer:
        try:
            process_message(msg.value, producer)
        except Exception as exc:
            print(f"[o3] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
