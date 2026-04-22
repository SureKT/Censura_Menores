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
    guid = trace["request_id"]
    faces = payload.get("faces", [])
    total_faces = payload.get("total_faces", len(faces))
    now = datetime.now(timezone.utc)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Id_Imagen se numera desde 1 dentro de cada solicitud
            cur.execute(
                "SELECT COALESCE(MAX(Id_Imagen), 0) FROM Imagenes WHERE GUID_Solicitud = %s",
                (guid,),
            )
            current_max = cur.fetchone()[0]

            for idx, face in enumerate(faces, start=1):
                cur.execute(
                    """
                    INSERT INTO Imagenes (
                        GUID_Solicitud, Id_Imagen,
                        Imagen_X, Imagen_Y, Imagen_Ancho, Imagen_Alto
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        guid,
                        current_max + idx,
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
                (now, now, now, "EN_ANALISIS_EDAD", guid),
            )
        conn.commit()

    output_cmd = build_age_command(face_event)
    producer.send(OUTPUT_TOPIC, output_cmd).get(timeout=10)
    print(f"[o2] {guid}: {total_faces} caras registradas, cmd.age_detection publicado.")


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
            print(f"[o2] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError("[o2] No se pudo conectar a Kafka.")

    print("[o2] Escuchando evt.face_detection.completed...")
    for msg in consumer:
        try:
            process_message(msg.value, producer)
        except Exception as exc:
            print(f"[o2] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
