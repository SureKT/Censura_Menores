import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "age-detection"
INPUT_TOPIC = "cmd.age_detection"
OUTPUT_TOPIC = "evt.age_detection.completed"
GROUP_ID = "age-detection-group"


def estimate_face_age(index: int) -> int:
    # Stub deterministico para pruebas de orquestacion.
    return 14 if index % 2 == 0 else 26


def build_output_event(cmd_event: dict) -> dict:
    trace = cmd_event["event"]["trace"]
    payload = cmd_event["payload"]
    input_faces = payload.get("faces", [])
    faces_with_age = []
    for idx, face in enumerate(input_faces):
        estimated_age = estimate_face_age(idx)
        faces_with_age.append(
            {
                **face,
                "estimated_age": estimated_age,
                "is_minor": estimated_age < 18,
            }
        )

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
            "faces": faces_with_age,
            "total_faces": len(faces_with_age),
        },
    }


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

    print("[age_detection] Escuchando cmd.age_detection...")
    for msg in consumer:
        try:
            output_event = build_output_event(msg.value)
            producer.send(OUTPUT_TOPIC, output_event).get(timeout=10)
            print(
                f"[age_detection] Evento emitido para request_id="
                f"{output_event['event']['trace']['request_id']}"
            )
        except Exception as exc:
            print(f"[age_detection] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
