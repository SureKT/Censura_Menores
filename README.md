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
Proyecto_Imagenes_IA/
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
```

## Descripcion de servicios

Servicios actualmente implementados en compose:
- `kafka`: broker de eventos (KRaft).
- `kafka-init`: crea topics al arrancar.
- `postgres`: base de datos relacional para metadatos.
- `minio`: almacenamiento compatible S3 para imagenes.
- `minio-init`: crea buckets iniciales.

Servicios planificados para el pipeline completo:
- `api-gateway`
- `orchestrator`
- `face-detection`
- `age-detection`
- `pixelation`
- `storage-service`

## Topics y flujo de eventos

Topics definidos:
- `images.raw`
- `evt.face_detection.completed`
- `evt.age_detection.completed`
- `evt.pixelation.completed`
- `evt.storage.completed`
- `cmd.face_detection`
- `cmd.age_detection`
- `cmd.pixelation`
- `cmd.storage`
- `events.dead_letter`

Flujo de referencia:
1. Cliente publica imagen via API Gateway hacia `images.raw`.
2. Orchestrator publica comandos `cmd.*` para cada etapa.
3. Servicios de proceso publican eventos `evt.*.completed`.
4. Storage persiste resultado y estado final.
5. Errores no recuperables se envian a `events.dead_letter`.

