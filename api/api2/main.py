import os
from datetime import timedelta

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from minio import Minio


APP_NAME = "api-consulta"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bda_imagenes"),
        user=os.getenv("POSTGRES_USER", "bda_user"),
        password=os.getenv("POSTGRES_PASSWORD", "bda_pass"),
    )


def create_minio_client() -> Minio:
    endpoint   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure     = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


app = FastAPI(title="API Consulta")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

minio_client = create_minio_client()


@app.get("/health")
def health():
    return {"status": "ok", "service": APP_NAME}


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

    if solicitud["estado"] == "COMPLETED" and solicitud.get("url_imagen_terminada"):
        solicitud["download_url"] = f"http://localhost:8002/download/{solicitud['guid_solicitud']}"
    else:
        solicitud.pop("url_imagen_terminada", None)

    return solicitud
