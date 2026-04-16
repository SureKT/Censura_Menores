import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


APP_NAME = "pixelation"
INPUT_TOPIC = "cmd.pixelation"
OUTPUT_TOPIC = "evt.pixelation.completed"
GROUP_ID = "pixelation-group"


def build_output_event(cmd_event: dict) -> dict:
    trace = cmd_event["event"]["trace"]
    payload = cmd_event["payload"]
    faces = payload.get("faces", [])
    pixelated_faces = [face for face in faces if face.get("is_minor")]
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
            "pixelated_faces_count": len(pixelated_faces),
            "total_faces": payload.get("total_faces", len(faces)),
            "minors_count": payload.get("minors_count", len(pixelated_faces)),
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

    print("[pixelation] Escuchando cmd.pixelation...")
    for msg in consumer:
        try:
            output_event = build_output_event(msg.value)
            producer.send(OUTPUT_TOPIC, output_event).get(timeout=10)
            print(
                f"[pixelation] Evento emitido para request_id="
                f"{output_event['event']['trace']['request_id']}"
            )
        except Exception as exc:
            print(f"[pixelation] Error procesando mensaje: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    run()
