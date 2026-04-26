import json
import os
import queue
import threading
import time
from contextlib import contextmanager
from datetime import timedelta

import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from kafka import KafkaConsumer
from minio import Minio


APP_NAME = "api-consulta"
REALTIME_EVENT_TOPIC = "evt.realtime.classification.completed"

# Dispatcher de eventos de tiempo real: {session_id: queue.Queue[dict]}
_rt_sessions: dict[str, "queue.Queue[dict]"] = {}
_rt_lock = threading.Lock()
_RT_MAX_SESSIONS = 64
_RT_QUEUE_MAX = 256


def _rt_get_queue(session_id: str) -> "queue.Queue[dict]":
    with _rt_lock:
        q = _rt_sessions.get(session_id)
        if q is None:
            # Evictar sesiones viejas si superamos el limite
            if len(_rt_sessions) >= _RT_MAX_SESSIONS:
                oldest = next(iter(_rt_sessions))
                _rt_sessions.pop(oldest, None)
            q = queue.Queue(maxsize=_RT_QUEUE_MAX)
            _rt_sessions[session_id] = q
        return q


def _rt_drop_queue(session_id: str) -> None:
    with _rt_lock:
        _rt_sessions.pop(session_id, None)


def _rt_dispatch(payload: dict) -> None:
    session_id = payload.get("session_id")
    if not session_id:
        return
    with _rt_lock:
        q = _rt_sessions.get(session_id)
    if q is None:
        return
    try:
        q.put_nowait(payload)
    except queue.Full:
        # Descartar el mas antiguo para mantener latencia baja
        try:
            q.get_nowait()
            q.put_nowait(payload)
        except queue.Empty:
            pass


def _rt_consumer_loop() -> None:
    """Hilo en background: consume Kafka y despacha por session_id."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    consumer = None
    for attempt in range(60):
        try:
            consumer = KafkaConsumer(
                REALTIME_EVENT_TOPIC,
                bootstrap_servers=bootstrap,
                group_id=f"api2-realtime-{os.getpid()}",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            break
        except Exception as exc:
            print(f"[api2-rt] Kafka no disponible ({attempt+1}/60): {exc}")
            time.sleep(5)
    if consumer is None:
        print("[api2-rt] No se pudo conectar a Kafka, SSE no funcionara.")
        return

    print(f"[api2-rt] Escuchando {REALTIME_EVENT_TOPIC}...")
    for msg in consumer:
        try:
            _rt_dispatch(msg.value.get("payload", {}))
        except Exception as exc:
            print(f"[api2-rt] Error despachando evento: {exc}")


_rt_thread: threading.Thread | None = None


def _ensure_rt_thread() -> None:
    global _rt_thread
    if _rt_thread is None or not _rt_thread.is_alive():
        _rt_thread = threading.Thread(target=_rt_consumer_loop, daemon=True)
        _rt_thread.start()

_pool: pg_pool.ThreadedConnectionPool | None = None


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pg_pool.ThreadedConnectionPool(
            1, 10,
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


app = FastAPI(title="API Consulta")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

minio_client = create_minio_client()

# Arrancar consumer de tiempo real
_ensure_rt_thread()


@app.get("/health")
def health():
    return {"status": "ok", "service": APP_NAME}


@app.get("/realtime/stream/{session_id}")
async def realtime_stream(session_id: str, request: Request):
    """SSE: reenvia al navegador los eventos de clasificacion de tiempo real
    (evt.realtime.classification.completed) filtrados por session_id.
    """
    _ensure_rt_thread()
    q = _rt_get_queue(session_id)

    async def event_generator():
        import asyncio
        loop = asyncio.get_event_loop()
        heartbeat_every = 15.0
        last_beat = time.monotonic()
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    # Esperamos hasta 1s por un evento; cada 15s mandamos heartbeat
                    payload = await loop.run_in_executor(None, lambda: q.get(timeout=1.0))
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_beat = time.monotonic()
                except queue.Empty:
                    if time.monotonic() - last_beat >= heartbeat_every:
                        yield ": heartbeat\n\n"
                        last_beat = time.monotonic()
        finally:
            # Si nadie mas escucha, soltamos la cola para no acumular
            _rt_drop_queue(session_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/download/{guid}")
def download_image(guid: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT url_imagen_terminada FROM Solicitud WHERE guid_solicitud = %s AND estado = 'COMPLETED'",
                (guid,),
            )
            row = cur.fetchone()
    if not row or not row[0]:
        raise HTTPException(status_code=404, detail="Imagen procesada no disponible.")

    bucket, object_key = row[0].split("/", 1)
    filename = object_key.split("/")[-1]

    response = minio_client.get_object(bucket, object_key)
    ext = filename.rsplit(".", 1)[-1].lower()
    media_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp"}
    media_type = media_map.get(ext, "application/octet-stream")
    return StreamingResponse(response, media_type=media_type)


@app.get("/solicitudes/{guid}")
def consultar_solicitud(guid: str):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute(
                """
                SELECT
                    GUID_Solicitud, URL_Imagen_Original, URL_Imagen_Terminada,
                    Estado,
                    Inicio_Solicitud, Fin_Solicitud,
                    Inicio_Deteccion_Caras, Fin_Deteccion_Caras,
                    Inicio_Edad, Fin_edad,
                    Inicio_Pixelado, Fin_Pixelado,
                    Inicio_Almacenamiento_Solicitud, Fin_Almacenamiento_Solicitud
                FROM Solicitud
                WHERE GUID_Solicitud = %s
                """,
                (guid,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Solicitud '{guid}' no encontrada.")
            solicitud = dict(row)

            cur.execute(
                """
                SELECT Id_Imagen, Mayor_18, score,
                       Imagen_X AS x, Imagen_Y AS y,
                       Imagen_Ancho AS width, Imagen_Alto AS height
                FROM Imagenes
                WHERE GUID_Solicitud = %s
                ORDER BY Id_Imagen
                """,
                (guid,),
            )
            caras = [dict(r) for r in cur.fetchall()]

    # Serializar timestamps
    for k, v in solicitud.items():
        if hasattr(v, "isoformat"):
            solicitud[k] = v.isoformat()

    # Convertir Decimal a float para JSON
    for cara in caras:
        if cara.get("score") is not None:
            cara["score"] = float(cara["score"])

    solicitud["caras"] = caras

    # score == -1.0 indica que el análisis de edad falló para esa cara (formato no soportado)
    if any(c.get("score") is not None and c["score"] < 0 for c in caras):
        solicitud["age_detection_warning"] = True

    if solicitud["estado"] == "COMPLETED" and solicitud.get("url_imagen_terminada"):
        base = os.getenv("PUBLIC_BASE_URL", "http://localhost:8002")
        solicitud["download_url"] = f"{base.rstrip('/')}/download/{solicitud['guid_solicitud']}"
    else:
        solicitud.pop("url_imagen_terminada", None)

    return solicitud
