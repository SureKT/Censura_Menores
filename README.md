# Proyecto: Pixelado de Rostros de Menores

Sistema distribuido event-driven para procesar imagenes y pixelar automaticamente los rostros de personas menores de 18 anos.

## Objetivo del proyecto

Implementar un pipeline basado en eventos con Kafka que permita:
- recibir imagenes,
- detectar rostros,
- estimar edad por rostro,
- pixelar rostros de menores,
- almacenar y consultar resultados.

## Como ejecutar el sistema (docker-compose)

Arranque:
```bash
docker compose up -d
```

Validacion de infraestructura:
```bash
powershell -ExecutionPolicy Bypass -File .\scripts\smoke-test.ps1
```

Parada:
```bash
docker compose down
```

## Estructura del proyecto

```text
Censura_Menores/
  docker-compose.yml
  README.md
  AGENTS.md
  contracts/
    _defs.schema.json
    images.raw.v1.schema.json
    evt.storage.completed.v1.schema.json
    events.dead_letter.v1.schema.json
  scripts/
    smoke-test.ps1
  db/
    init.sql
  dataset/
    face_age/          <- dataset Kaggle frabbisw/facial-age (carpetas = edad)
  api/
    api1/              <- API de ingesta (FastAPI, puerto 8001)
    api2/              <- API de consulta (FastAPI, puerto 8002)
  orquestadores/
    o2/                <- Orquestador de analisis
    o3/                <- Orquestador de decision
    o4/                <- Orquestador de finalizacion
  services/
    face_detection/    <- Deteccion de caras con YOLOv8-face
    age_detection/     <- Estimacion de edad con MobileNetV2 (PyTorch)
    pixelation/        <- Pixelado de rostros de menores
  frontend/
    index.html         <- Interfaz web (puerto 3000)
```

## Descripcion de servicios

### Infraestructura

- `kafka`: broker de eventos en modo KRaft (sin ZooKeeper). Puerto 9092.
- `kafka-init`: crea los 10 topics al arrancar.
- `postgres`: base de datos relacional para metadatos de solicitudes y caras. Puerto 5432.
- `minio`: almacenamiento compatible S3 para imagenes originales y procesadas. Puertos 9000 y 9001.
- `minio-init`: crea los buckets `imagenes-raw` e `imagenes-procesadas` al arrancar.

### APIs REST

- `api1`: API de ingesta (FastAPI). Acepta `POST /images`, sube la imagen a MinIO, inserta la solicitud en PostgreSQL con estado `RECIBIDO` y publica `cmd.face_detection` en Kafka. Puerto 8001.
- `api2`: API de consulta (FastAPI). Expone `GET /solicitudes/{guid}` con el estado del pipeline, coordenadas de caras detectadas y URL prefirmada de MinIO para descargar la imagen procesada. Puerto 8002.

### Orquestadores

- `o2`: orquestador de analisis. Consume `evt.face_detection.completed`, inserta las filas en la tabla `Imagenes` con coordenadas de cada cara y publica `cmd.age_detection`.
- `o3`: orquestador de decision. Consume `evt.age_detection.completed`, actualiza `Mayor_18` y `score` en la tabla `Imagenes` y decide si publicar `cmd.pixelation` (si hay menores) o `cmd.storage` (si no hay menores).
- `o4`: orquestador de finalizacion. Consume `evt.pixelation.completed` o `cmd.storage`, actualiza todos los timestamps finales, guarda la URL de la imagen terminada y marca la solicitud como `COMPLETED`.

### Servicios de procesamiento

- `face-detection`: consume `cmd.face_detection`, descarga la imagen de MinIO, ejecuta el modelo YOLOv8-face y publica `evt.face_detection.completed` con las coordenadas `(x, y, width, height)` de cada rostro detectado.
- `age-detection`: consume `cmd.age_detection`, descarga la imagen de MinIO, recorta cada cara usando las coordenadas del paso anterior y estima la edad con MobileNetV2 (PyTorch). Publica `evt.age_detection.completed` con `estimated_age`, `is_minor` y `confidence` por cara.
- `pixelation`: consume `cmd.pixelation`, descarga la imagen original de MinIO, aplica un filtro de pixelado sobre los rostros marcados como menores y sube la imagen procesada al bucket `imagenes-procesadas`.

### Frontend

Interfaz web servida con `python -m http.server 3000`. Permite subir una imagen, ver el progreso del pipeline en tiempo real (polling a API2 cada 1 segundo), visualizar los bounding boxes de cada cara con etiqueta Menor/Adulto y descargar la imagen procesada.

## Red neuronal de estimacion de edad

### Arquitectura

- Base: **MobileNetV2** preentrenado en ImageNet.
- Cabeza: `Dropout(0.2)` + `Linear(1280 → 1)` (regresion de edad).
- Entrada: recorte de cara 224x224 con 10% de padding alrededor del bounding box.
- Salida: edad estimada (entero 0-120) y confianza (`sigmoid(|edad - 18| / 4)`).
- Umbral: si `edad < 18` → menor.

### Dataset

Dataset: [Kaggle frabbisw/facial-age](https://www.kaggle.com/datasets/frabbisw/facial-age).
Estructura: carpetas numericas (`001/`, `002/`, ..., `110/`) donde el nombre es la edad y el contenido son imagenes de caras.

### Entrenamiento

Instalar dependencias de entrenamiento:
```bash
pip install -r services/age_detection/requirements-train.txt
```

Entrenar:
```bash
cd services/age_detection
python train.py --dataset ../../dataset/face_age --epochs 15
```

Opciones disponibles:
```
--dataset   Ruta al dataset (default: ../../dataset/face_age)
--output    Ruta de salida del modelo (default: age_model.pth)
--epochs    Numero de epocas (default: 15)
--batch     Tamano de batch (default: 32)
--lr        Learning rate (default: 0.001)
```

El modelo entrenado se guarda como `services/age_detection/age_model.pth`. Tras el entrenamiento hay que reconstruir el contenedor:
```bash
docker compose up -d --build age-detection
```

### Prueba manual del modelo

```bash
cd services/age_detection

# Una imagen
python test.py foto.jpg

# Varias imagenes o una carpeta
python test.py foto1.jpg foto2.png carpeta/

# Modelo alternativo
python test.py foto.jpg --model otro_modelo.pth
```

## Topics y flujo de eventos

Topics definidos:
- `images.raw`
- `cmd.face_detection`
- `evt.face_detection.completed`
- `cmd.age_detection`
- `evt.age_detection.completed`
- `cmd.pixelation`
- `evt.pixelation.completed`
- `cmd.storage`
- `evt.storage.completed`
- `events.dead_letter`

Flujo de referencia:
1. Cliente sube imagen via `POST /images` (API1).
2. API1 guarda en MinIO, registra en PostgreSQL y publica `cmd.face_detection`.
3. `face-detection` detecta caras y publica `evt.face_detection.completed`.
4. O2 registra caras en BD y publica `cmd.age_detection`.
5. `age-detection` estima edades y publica `evt.age_detection.completed`.
6. O3 actualiza BD y publica `cmd.pixelation` (menores) o `cmd.storage` (sin menores).
7. `pixelation` pixela caras y publica `evt.pixelation.completed`.
8. O4 marca la solicitud como `COMPLETED` y guarda la URL final.
9. Cliente consulta resultado via `GET /solicitudes/{guid}` (API2).
10. Errores no recuperables se envian a `events.dead_letter`.

## Esquema de la base de datos

### Tabla Solicitud
| Columna | Tipo | Descripcion |
|---|---|---|
| GUID_Solicitud | VARCHAR PK | Identificador unico de la solicitud |
| URL_Imagen_Original | VARCHAR | Ruta en MinIO de la imagen original |
| URL_Imagen_Terminada | VARCHAR | Ruta en MinIO de la imagen procesada |
| Estado | VARCHAR | RECIBIDO / EN_ANALISIS_EDAD / PENDIENTE_PIXELADO / PENDIENTE_STORAGE / COMPLETED |
| Inicio_Solicitud | TIMESTAMP | Inicio del pipeline |
| Fin_Solicitud | TIMESTAMP | Fin del pipeline |
| Inicio/Fin_Deteccion_Caras | TIMESTAMP | Tiempos de la fase de deteccion |
| Inicio/Fin_Edad | TIMESTAMP | Tiempos de la fase de edad |
| Inicio/Fin_Pixelado | TIMESTAMP | Tiempos de la fase de pixelado |
| Inicio/Fin_Almacenamiento_Solicitud | TIMESTAMP | Tiempos de la fase de almacenamiento |

### Tabla Imagenes
| Columna | Tipo | Descripcion |
|---|---|---|
| GUID_Solicitud | VARCHAR FK | Referencia a la solicitud |
| Id_Imagen | INT | Indice de la cara dentro de la solicitud |
| Mayor_18 | BOOLEAN | True si adulto, False si menor |
| score | DECIMAL | Confianza de la prediccion (0.0 - 1.0) |
| Imagen_X | INT | Coordenada X del bounding box |
| Imagen_Y | INT | Coordenada Y del bounding box |
| Imagen_Ancho | INT | Ancho del bounding box en pixeles |
| Imagen_Alto | INT | Alto del bounding box en pixeles |
