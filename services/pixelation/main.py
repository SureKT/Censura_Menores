import json
import io
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from PIL import Image, ImageDraw, ImageFont


APP_NAME    = "pixelation"
INPUT_TOPIC = "cmd.pixelation"
OUTPUT_TOPIC = "evt.pixelation.completed"
DLQ_TOPIC   = "events.dead_letter"
GROUP_ID    = "pixelation-group"

# Colores para bounding boxes: (R, G, B)
COLOR_MINOR = (220, 50,  50)   # rojo — menor
COLOR_ADULT = (50,  200, 50)   # verde — adulto


def create_minio_client() -> Minio:
    endpoint   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure     = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def pixelate_region(image: Image.Image, x: int, y: int, width: int, height: int):
    if width <= 0 or height <= 0:
        return
    img_w, img_h = image.size
    left   = max(0, min(x, img_w - 1))
    top    = max(0, min(y, img_h - 1))
    right  = max(left + 1, min(x + width, img_w))
    bottom = max(top + 1, min(y + height, img_h))
    region = image.crop((left, top, right, bottom))
    downscaled = region.resize((max(1, region.width // 12), max(1, region.height // 12)))
    pixelated  = downscaled.resize(region.size, Image.Resampling.NEAREST)
    image.paste(pixelated, (left, top, right, bottom))


def draw_annotations(image: Image.Image, faces: list[dict]) -> Image.Image:
    """Dibuja bounding boxes y etiquetas (edad + confianza) sobre una copia de la imagen."""
    annotated = image.copy()
    draw      = ImageDraw.Draw(annotated)
    try:
        font = ImageFont.load_default(size=16)
    except TypeError:
        font = ImageFont.load_default()

    img_w, img_h = annotated.size
    for face in faces:
        x = int(face.get("x", 0))
        y = int(face.get("y", 0))
        w = int(face.get("width", 0))
        h = int(face.get("height", 0))
        is_minor = face.get("is_minor", False)
        age      = face.get("estimated_age")
        conf     = face.get("confidence")

        color  = COLOR_MINOR if is_minor else COLOR_ADULT
        label  = "Menor" if is_minor else "Adulto"
        if age is not None:
            label += f"  {age}a"
        if conf is not None and conf >= 0:
            label += f"  ({conf:.0%})"

        # Clampar coordenadas al tamaño de la imagen
        x1 = max(0, x)
        y1 = max(0, y)
        x2 = min(img_w, x + w)
        y2 = min(img_h, y + h)

        # Borde doble para mejor visibilidad
        draw.rectangle([x1, y1, x2, y2], outline=(0, 0, 0), width=4)
        draw.rectangle([x1, y1, x2, y2], outline=color, width=2)

        # Fondo semitransparente para el texto
        text_y = max(0, y1 - 20)
        draw.rectangle([x1, text_y, x1 + len(label) * 8 + 4, text_y + 18], fill=(0, 0, 0))
        draw.text((x1 + 2, text_y + 1), label, fill=color, font=font)

    return annotated


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
        }).get(timeout=5)
    except Exception as dlq_exc:
        print(f"[{APP_NAME}] No se pudo enviar a DLQ: {dlq_exc}")


def process_image(cmd_event: dict, minio_client: Minio) -> dict:
    """
    Genera dos imágenes y las sube a MinIO:
    - Imagen procesada: con los rostros de menores pixelados.
    - Imagen con marcos: original con bounding boxes y etiquetas en todas las caras.
    Devuelve un dict con las claves de salida.
    """
    payload      = cmd_event["payload"]
    input_bucket = payload["bucket"]
    input_key    = payload["object_key"]
    output_bucket = os.getenv("MINIO_PROCESSED_BUCKET", "imagenes-procesadas")
    base_name    = os.path.basename(input_key)
    run_id       = str(uuid.uuid4())

    # Descargar imagen original
    response = minio_client.get_object(input_bucket, input_key)
    try:
        image_bytes = response.read()
    finally:
        response.close()
        response.release_conn()

    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    faces = payload.get("faces", [])

    # ── Imagen pixelada ───────────────────────────────────────────────────────
    processed = image.copy()
    pixelated_count = 0
    for face in faces:
        if face.get("is_minor"):
            pixelate_region(
                processed,
                int(face.get("x", 0)),
                int(face.get("y", 0)),
                int(face.get("width", 0)),
                int(face.get("height", 0)),
            )
            pixelated_count += 1

    processed_buf = io.BytesIO()
    processed.save(processed_buf, format="JPEG", quality=90)
    processed_bytes = processed_buf.getvalue()
    processed_key = f"processed/{run_id}-{base_name}"
    minio_client.put_object(
        output_bucket, processed_key,
        io.BytesIO(processed_bytes), len(processed_bytes),
        content_type="image/jpeg",
    )

    # ── Imagen con marcos (bounding boxes + etiquetas) ────────────────────────
    annotated       = draw_annotations(image, faces)
    annotated_buf   = io.BytesIO()
    annotated.save(annotated_buf, format="JPEG", quality=90)
    annotated_bytes = annotated_buf.getvalue()
    marcos_key = f"marcos/{run_id}-{base_name}"
    minio_client.put_object(
        output_bucket, marcos_key,
        io.BytesIO(annotated_bytes), len(annotated_bytes),
        content_type="image/jpeg",
    )

    return {
        "output_bucket":      output_bucket,
        "output_object_key":  processed_key,
        "marcos_bucket":      output_bucket,
        "marcos_object_key":  marcos_key,
        "pixelated_count":    pixelated_count,
    }


def build_output_event(cmd_event: dict, result: dict) -> dict:
    trace   = cmd_event["event"]["trace"]
    payload = cmd_event["payload"]
    faces   = payload.get("faces", [])
    return {
        "event": {
            "event_id":    str(uuid.uuid4()),
            "event_type":  OUTPUT_TOPIC,
            "event_version": "v1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "trace": {
                "request_id": trace["request_id"],
                "image_id":   trace["image_id"],
            },
            "source": APP_NAME,
        },
        "payload": {
            "bucket":              payload["bucket"],
            "object_key":          payload["object_key"],
            "output_bucket":       result["output_bucket"],
            "output_object_key":   result["output_object_key"],
            "marcos_bucket":       result["marcos_bucket"],
            "marcos_object_key":   result["marcos_object_key"],
            "pixelated_faces_count": result["pixelated_count"],
            "total_faces":         payload.get("total_faces", len(faces)),
            "minors_count":        payload.get("minors_count", result["pixelated_count"]),
        },
    }


def run():
    bootstrap    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
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
            print(f"[pixelation] Kafka no disponible (intento {attempt+1}/12): {exc}. Reintentando en 5s...")
            time.sleep(5)
    else:
        raise RuntimeError("[pixelation] No se pudo conectar a Kafka.")

    print("[pixelation] Escuchando cmd.pixelation...")
    for msg in consumer:
        t_start = time.time()
        try:
            result       = process_image(msg.value, minio_client)
            output_event = build_output_event(msg.value, result)
            producer.send(OUTPUT_TOPIC, output_event).get(timeout=10)

            elapsed_ms = (time.time() - t_start) * 1000
            rid = output_event["event"]["trace"]["request_id"]
            print(
                f"[pixelation] {rid}: {result['pixelated_count']} pixelados, "
                f"marcos generados → evt.pixelation.completed ({elapsed_ms:.0f}ms)"
            )
        except Exception as exc:
            print(f"[pixelation] Error procesando mensaje: {exc}")
            send_to_dlq(producer, msg.value, exc)
            time.sleep(1)


if __name__ == "__main__":
    run()
