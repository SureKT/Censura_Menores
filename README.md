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
Proyecto-Imagenes/
  docker-compose.yml
  README.md
  AGENTS.md
  contracts/
    _defs.schema.json
    evt.storage.completed.v1.schema.json
    events.dead_letter.v1.schema.json
  scripts/
    smoke-test.ps1          <- valida infraestructura y topics Kafka
    manage_models.ps1       <- gestiona versiones del modelo de edad
    latency-report.ps1      <- informe de latencias end-to-end desde PostgreSQL
  db/
    init.sql
  dataset/
    face_age/               <- dataset Kaggle frabbisw/facial-age (carpetas = edad)
  api/
    api1/                   <- API de ingesta (FastAPI, puerto 8001)
    api2/                   <- API de consulta (FastAPI, puerto 8002)
  orquestadores/
    o2/                     <- Orquestador de analisis
    o3/                     <- Orquestador de decision
    o4/                     <- Orquestador de finalizacion
  services/
    face_detection/         <- Deteccion de caras con YOLOv8-face
    age_detection/          <- Estimacion de edad con MobileNetV2 (PyTorch)
      models/               <- historial de versiones del modelo (.pth + registry.json)
    age_realtime/           <- variante ligera para camara en vivo (sin MinIO/Postgres)
    pixelation/             <- Pixelado de rostros de menores + imagen con marcos
  frontend/
    index.html              <- Interfaz web foto estatica (puerto 3000)
    realtime.html           <- Interfaz camara en vivo
    realtime.js             <- logica de deteccion y pixelado en tiempo real
```

## Descripcion de servicios

### Infraestructura

- `kafka`: broker de eventos en modo KRaft (sin ZooKeeper). Puerto 9092.
- `kafka-init`: crea los 10 topics al arrancar (incluye `cmd.realtime.classification` y `evt.realtime.classification.completed` para el flujo de cámara en vivo).
- `postgres`: base de datos relacional para metadatos de solicitudes y caras. Puerto 5432.
- `minio`: almacenamiento compatible S3 para imagenes originales y procesadas. Puertos 9000 y 9001.
- `minio-init`: crea los buckets `imagenes-raw` e `imagenes-procesadas` al arrancar.

### APIs REST

- `api1`: API de ingesta (FastAPI). Acepta `POST /images`, sube la imagen a MinIO, inserta la solicitud en PostgreSQL con estado `CREADA` y publica `cmd.face_detection` en Kafka. Puerto 8001.
- `api2`: API de consulta (FastAPI). Expone `GET /solicitudes/{guid}` con el estado del pipeline, coordenadas de caras y URLs de descarga. También expone `GET /solicitudes/{guid}/marcos` (imagen con bounding boxes) y `GET /solicitudes/{guid}/caras/{id}` (recorte de cara individual). Puerto 8002.

### Orquestadores

- `o2`: orquestador de analisis. Consume `evt.face_detection.completed`, inserta las filas en la tabla `Imagenes` con coordenadas y recorte de cada cara (MinIO), actualiza estado a `CARAS_DETECTADAS` y publica `cmd.age_detection`.
- `o3`: orquestador de decision. Consume `evt.age_detection.completed`, actualiza `Mayor_18` y `score` en la tabla `Imagenes`, actualiza estado a `EDAD_CALCULADA` y publica `cmd.pixelation` (siempre, incluso si no hay menores).
- `o4`: orquestador de finalizacion. Consume `evt.pixelation.completed`, actualiza todos los timestamps finales, guarda la URL de la imagen terminada y la imagen con marcos, y marca la solicitud como `COMPLETADA`.

### Servicios de procesamiento

- `face-detection`: consume `cmd.face_detection`, descarga la imagen de MinIO, ejecuta el modelo YOLOv8-face y publica `evt.face_detection.completed` con las coordenadas `(x, y, width, height)` de cada rostro detectado.
- `age-detection`: consume `cmd.age_detection`, descarga la imagen de MinIO, recorta cada cara (padding 10 %) y estima la edad con MobileNetV2 + TTA (5 augmentaciones). Clasifica como menor si `edad < MINOR_THRESHOLD` (default 22). Publica `evt.age_detection.completed` con `estimated_age`, `is_minor` y `confidence` por cara.
- `pixelation`: consume `cmd.pixelation`, descarga la imagen original de MinIO y genera dos versiones: (1) imagen pixelada con los rostros de menores censurados, (2) imagen con bounding boxes y etiquetas de edad/confianza sobre todas las caras (fuente y grosor de borde escalados dinámicamente al ancho de la imagen). Sube ambas al bucket `imagenes-procesadas` y publica `evt.pixelation.completed`.
- `age-realtime`: variante ligera de `age-detection` para el flujo de cámara en vivo. Consume `cmd.realtime.classification` (crops ya recortados en base64, sin MinIO ni PostgreSQL), ejecuta MobileNetV2 sin TTA (latencia baja) con el mismo umbral `MINOR_THRESHOLD=22`. Publica `evt.realtime.classification.completed`.

## Modo cámara en vivo

Además del flujo clásico de subida de una foto, el sistema incluye una vista de detección y pixelado en tiempo real con la webcam (`http://localhost:3000/realtime.html`).

### Arquitectura del flujo en vivo

```
Navegador                          Backend
-----------------------            -----------------------------------
getUserMedia → <video>
BlazeFace (TF.js) @ 60fps
Tracker IoU → cara1, cara2…
   │ cara NUEVA                    ┌─────────────────────────────┐
   └─► POST /realtime/faces ─────► │ API1 ─► cmd.realtime.classif│
                                   │         │                   │
                                   │         ▼                   │
                                   │  age-realtime (MobileNetV2) │
                                   │         │                   │
                                   │         ▼                   │
                                   │  evt.realtime.classif.compl │
                                   │         │                   │
                                   │         ▼                   │
                                   │  API2 (consumer + SSE)      │
                                   └─────────────────────────────┘
   ◄── SSE /realtime/stream/{session_id}
cache {face_token → is_minor}
pixelado canvas @ 60fps (menores)
```

### Topics Kafka nuevos

- `cmd.realtime.classification`: publicado por API1 con `session_id`, `face_token` y el recorte JPEG en base64.
- `evt.realtime.classification.completed`: publicado por `age-realtime` con `session_id`, `face_token`, `estimated_age`, `is_minor`, `confidence`.

El pipeline clásico (`cmd.face_detection` → … → `evt.storage.completed`) sigue funcionando sin cambios.

### Uso

1. `docker compose up -d` (arranca `age-realtime` entre otros).
2. Abrir `http://localhost:3000` → pulsar **"Cámara en vivo →"** en la cabecera.
3. Pulsar **Iniciar cámara** y aceptar permiso del navegador.
4. Cada cara recibe un ID estable (`cara1`, `cara2`, …) y se clasifica una sola vez; si es menor, se pixela en vivo en cada frame mientras siga visible.

### Decisiones de diseño

- La detección de caras y el tracking corren **en el navegador** (TF.js + BlazeFace). Enviar 60 frames/s al backend sería inviable.
- El backend solo recibe **un crop por cara nueva** (no reprocesa caras ya clasificadas).
- La respuesta se entrega por **Server-Sent Events** (`/realtime/stream/{session_id}`) desde API2.
- No se persiste en PostgreSQL (serían miles de eventos por sesión). Los eventos viven en Kafka (24h de retención) para auditoría.
- Diagrama: `docs/flujo-tiempo-real.drawio`.

### Frontend

Servido por Nginx en el puerto 3000. Dos vistas:

- **`index.html`** — sube una foto, muestra progreso del pipeline en tiempo real (polling a API2 cada segundo), y al terminar presenta un toggle **Pixelada / Con marcos** para alternar entre la imagen con menores pixelados y la imagen anotada con bounding boxes y etiquetas de edad. No dibuja nada en canvas sobre el resultado final: usa directamente las imágenes generadas por el backend.
- **`realtime.html`** — cámara en vivo. Usa BlazeFace (TF.js) para detectar caras en el navegador a ~15fps, envía un crop por cara nueva al backend via SSE y pixela en canvas las caras clasificadas como menores.

## Red neuronal de estimacion de edad

### Arquitectura

- Base: **MobileNetV2** preentrenado en ImageNet.
- Cabeza: `Dropout(0.3)` → `Linear(1280 → 256)` → `ReLU` → `Dropout(0.2)` → `Linear(256 → 1)` (regresion de edad).
- Entrada: recorte de cara 224×224 con 10 % de padding alrededor del bounding box.
- Salida: edad estimada (entero 0-120) y confianza (`sigmoid(|edad - umbral| / 4)`).
- **Umbral**: `edad < MINOR_THRESHOLD` → menor (por defecto 22; configurable por variable de entorno).

### Inferencia robusta

El servicio `age-detection` aplica **Test-Time Augmentation (TTA)**: promedia 5 predicciones con distintas transformaciones (original, flip horizontal, crop central, jitter de brillo, escala de grises). Reduce la varianza en imágenes difíciles (ángulo, iluminación, baja resolución). Configurable con `TTA_PASSES` (env var; por defecto 5). El servicio `age-realtime` no usa TTA para mantener latencia baja.

### Dataset

Dataset: [Kaggle frabbisw/facial-age](https://www.kaggle.com/datasets/frabbisw/facial-age).
Estructura: carpetas numericas (`001/`, `002/`, ..., `110/`) donde el nombre es la edad y el contenido son imagenes de caras.

### Entrenamiento

Instalar dependencias:
```bash
# Con GPU NVIDIA (recomendado — ~10x mas rapido que CPU)
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124 --upgrade
pip install -r services/age_detection/requirements-train.txt

# Solo CPU
pip install -r services/age_detection/requirements-train.txt
```

Entrenar:
```bash
cd services/age_detection
python train.py --dataset ../../dataset/face_age --epochs 30 --desc "descripcion_version"
```

Opciones disponibles:
```
--dataset          Ruta al dataset (default: ../../dataset/face_age)
--output           Ruta del modelo activo (default: age_model.pth)
--epochs           Epocas totales fase1+fase2 (default: 30)
--batch            Tamano de batch (default: 32)
--lr               Learning rate del clasificador; backbone usa lr*0.1 (default: 0.001)
--freeze-epochs    Epocas con backbone congelado (default: 8)
--desc             Descripcion corta para el registro de versiones
--minor-threshold  Umbral que se registra en el historial (default: 22)
```

**Pérdida:** `BoundaryAwareLoss` = `HuberLoss(delta=3) + 0.7 · BCE(18)`. El término BCE penaliza especialmente los errores que invierten la clasificación menor/adulto.

**Sampler:** `WeightedRandomSampler` con boost para las franjas de mayor riesgo:
| Franja | Boost |
|---|---|
| 0-4 años | ×1.5 |
| 5-9 años | ×1.5 |
| 10-12 años | ×1.5 |
| 13-17 años | ×2.0 |
| 18-20 años | ×1.5 |

Al terminar, el script guarda la versión en `models/` y actualiza `registry.json`. Reconstruir el contenedor para aplicar el nuevo modelo:
```bash
docker compose up -d --build age-detection
```

### Versionado de modelos

```powershell
# Ver todas las versiones
.\scripts\manage_models.ps1 list

# Detalle de una version
.\scripts\manage_models.ps1 info v1

# Activar una version (copia el .pth y reinicia contenedores)
.\scripts\manage_models.ps1 use v1
```

El historial se almacena en `services/age_detection/models/registry.json`. Cada entrada incluye id, MAE de validación, alpha, umbral y descripción.

### Métricas y gráficas

Genera una figura PNG con las 3 gráficas clave:
- **Scatter predicha vs real** — precisión global y distribución de errores
- **MAE por franja de edad** — donde falla el modelo (critico para niños pequeños)
- **Matriz de confusión binaria** — recall de menores y falsos negativos (el error más grave)

```bash
cd services/age_detection

# Modelo activo, val set (15 % del dataset)
python metrics.py

# Modelo especifico
python metrics.py --model models/v1_20260505_baseline.pth

# Todo el dataset
python metrics.py --full-dataset

# Comparar dos versiones (scatter lado a lado)
python metrics.py --compare v1 v2
```

La imagen se guarda junto al `.pth` evaluado: `models/<nombre>_metrics.png`.

### Prueba manual del modelo (imagen individual)

```bash
cd services/age_detection

# Una imagen
python test.py foto.jpg

# Varias imagenes o una carpeta
python test.py foto1.jpg foto2.png carpeta/

# Modelo alternativo
python test.py foto.jpg --model models/v1_20260505_baseline.pth
```

## Topics y flujo de eventos

Topics definidos:
- `cmd.face_detection`
- `evt.face_detection.completed`
- `cmd.age_detection`
- `evt.age_detection.completed`
- `cmd.pixelation`
- `evt.pixelation.completed`
- `evt.storage.completed`
- `cmd.realtime.classification`
- `evt.realtime.classification.completed`
- `events.dead_letter`

Flujo de referencia:
1. Cliente sube imagen via `POST /images` (API1). Estado: `CREADA`.
2. API1 guarda en MinIO, registra en PostgreSQL y publica `cmd.face_detection`.
3. `face-detection` detecta caras y publica `evt.face_detection.completed`.
4. O2 registra caras en BD (con recortes en MinIO), estado `CARAS_DETECTADAS`, publica `cmd.age_detection`.
5. `age-detection` estima edades y publica `evt.age_detection.completed`.
6. O3 actualiza `Mayor_18`/`score` en BD, estado `EDAD_CALCULADA`, publica `cmd.pixelation` (siempre).
7. `pixelation` pixela menores, genera imagen con marcos, publica `evt.pixelation.completed`.
8. O4 marca la solicitud como `COMPLETADA`, guarda URL imagen terminada y URL imagen con marcos.
9. Cliente consulta resultado via `GET /solicitudes/{guid}` (API2).
10. Errores no recuperables se envian a `events.dead_letter`.

## Decisiones de diseño relevantes

### API1 absorbe la función de O1

El flujo descrito en la especificación contempla un **Orquestador 1 (O1)** que consumiría el topic `images.raw` y publicaría `cmd.face_detection`. Esta funcionalidad fue consolidada en **API1** por las siguientes razones:

- Elimina una hop de red y un topic intermedio innecesario.
- API1 ya tiene acceso a MinIO y PostgreSQL en el momento de la ingesta; delegar a O1 requeriría duplicar ese acceso.
- La especificación permite explícitamente el flujo desacoplado sin orquestador central.

El topic `images.raw` **no se crea ni se usa** en el pipeline real. El único punto de entrada al bus de eventos es `cmd.face_detection`, publicado directamente por API1 tras subir la imagen a MinIO e insertar la fila en PostgreSQL.

### O3 siempre pasa por Pixelation

Aunque la especificación contempla un atajo directo a `cmd.storage` cuando no hay menores, O3 enruta **siempre** a `cmd.pixelation`. Motivo: el servicio Pixelation genera la imagen con marcos (bounding boxes + etiquetas de edad/confianza) sobre **todas** las caras, independientemente de si hay menores. Sin esa imagen no habría vista "Con marcos" en el frontend. El overhead es despreciable (< 50 ms para imágenes normales).

---

## Gestión de errores

### Estrategia general

Cada servicio del pipeline sigue el mismo patrón de tolerancia a fallos:

| Nivel | Mecanismo |
|---|---|
| Conexión a Kafka | Reintento con backoff (12 intentos × 5 s) al arrancar |
| Procesamiento de mensaje | `try/except` por mensaje; el error no mata el consumer |
| Error no recuperable | El mensaje se reenvía a `events.dead_letter` con causa y mensaje original |
| Caída del contenedor | `restart: unless-stopped` en docker-compose — Docker reinicia automáticamente |

### Qué ocurre si falla cada servicio

| Servicio | Consecuencia | Recuperación |
|---|---|---|
| **kafka** | Todo el pipeline se detiene | Docker reinicia; los consumers retoman desde el último offset confirmado |
| **api1** | No se aceptan nuevas imágenes (HTTP 503) | Reinicio automático; solicitudes en vuelo no se pierden (ya están en Kafka) |
| **face-detection** | El mensaje queda en `cmd.face_detection` sin consumir | Al reiniciar reanuda desde ese offset; no se pierde ningún mensaje |
| **age-detection** | Ídem en `cmd.age_detection`; fallback: todas las caras se marcan como adulto con `confidence=-1` | Reinicio automático |
| **pixelation** | Ídem en `cmd.pixelation` | Reinicio automático |
| **o2 / o3 / o4** | El orquestador no actualiza la BD ni publica el siguiente comando | Reinicio automático; el consumer retoma el mensaje pendiente |
| **postgres** | Los orquestadores no pueden escribir estado | Reinicio automático con volumen persistente; no se pierden datos ya escritos |
| **minio** | Los servicios no pueden leer/escribir imágenes | Reinicio automático con volumen persistente |
| **api2** | El cliente no puede consultar resultados | Reinicio automático; el procesamiento en Kafka continúa sin API2 |

### Dead Letter Queue

El topic `events.dead_letter` recibe mensajes cuando un servicio no puede procesar un evento tras agotar los reintentos. Cada mensaje de DLQ incluye:
- `error.type` y `error.message` — causa del fallo
- `original_message` — el evento completo que falló

Para inspeccionar mensajes en DLQ:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic events.dead_letter \
  --from-beginning \
  --max-messages 20
```

---

## Métricas de rendimiento

### Por servicio (tiempo de procesamiento)

Cada servicio loguea el tiempo de procesamiento de cada evento en milisegundos:
```
[age_detection] req-abc → 3 caras, 1 menores (312ms)
[pixelation]    req-abc: 1 pixelados, marcos generados → evt.pixelation.completed (87ms)
```

### Latencia end-to-end

La tabla `Solicitud` almacena timestamps de inicio y fin de cada fase. Para consultar las latencias reales:

```powershell
# Últimas 20 solicitudes completadas con desglose por fase
.\scripts\latency-report.ps1

# Solo resumen agregado (media / min / max)
.\scripts\latency-report.ps1 -Summary

# Ampliar a las últimas 100
.\scripts\latency-report.ps1 -Last 100
```

Tiempos de referencia observados en la máquina de desarrollo (CPU, sin GPU):

| Fase | Tiempo típico |
|---|---|
| Detección de caras (YOLOv8) | 300–800 ms |
| Estimación de edad × cara (MobileNetV2 + TTA×5) | 200–600 ms |
| Pixelado + marcos | 50–150 ms |
| **Total end-to-end** | **< 2 s** (1–3 caras) |

### Capacidad de procesamiento múltiple

Cada servicio es un **consumer group Kafka independiente**. Al lanzar múltiples instancias de un servicio se reparten las particiones automáticamente, escalando el throughput horizontalmente sin cambios de código.

---

## Variables de entorno relevantes

| Variable | Servicio | Default | Descripcion |
|---|---|---|---|
| `MINOR_THRESHOLD` | age-detection, age-realtime | `22` | Edad maxima para clasificar como menor. Valor conservador (margen de 4 años sobre el límite legal de 18). |
| `TTA_PASSES` | age-detection | `5` | Número de augmentaciones TTA a promediar en inferencia (1 = sin TTA). |
| `MODEL_PATH` | age-detection, age-realtime | `age_model.pth` | Ruta al fichero `.pth` del modelo activo. |

## Esquema de la base de datos

### Tabla Solicitud
| Columna | Tipo | Descripcion |
|---|---|---|
| GUID_Solicitud | VARCHAR PK | Identificador unico de la solicitud |
| URL_Imagen_Original | VARCHAR | Ruta en MinIO de la imagen original |
| URL_Imagen_Terminada | VARCHAR | Ruta en MinIO de la imagen pixelada |
| URL_Imagen_Marcos | VARCHAR | Ruta en MinIO de la imagen con bounding boxes |
| Estado | VARCHAR | CREADA / CARAS_DETECTADAS / EDAD_CALCULADA / COMPLETADA |
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
| URL_Imagen | VARCHAR | Ruta en MinIO del recorte de la cara |
| Mayor_18 | BOOLEAN | True si adulto, False si menor |
| score | DECIMAL | Confianza de la prediccion (0.0 - 1.0) |
| Imagen_X | INT | Coordenada X del bounding box |
| Imagen_Y | INT | Coordenada Y del bounding box |
| Imagen_Ancho | INT | Ancho del bounding box en pixeles |
| Imagen_Alto | INT | Alto del bounding box en pixeles |
