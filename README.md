# Proyecto: Identificacion y Pixelado de Rostros de Menores

Sistema distribuido **event-driven** para procesar imagenes y pixelar automaticamente los rostros de personas menores de 18 anos.

## Objetivo

Implementar un pipeline de microservicios basado en Kafka que:
- reciba imagenes desde una API,
- detecte rostros,
- estime edad por rostro,
- pixelice solo los menores,
- y almacene el resultado final con trazabilidad del proceso.

## Arquitectura del proyecto (fase simplificada)

Para priorizar funcionalidad y sencillez, la fase actual usa solo infraestructura base:

- `kafka`: bus de eventos (KRaft, sin ZooKeeper).
- `kafka-init`: creacion automatica de topics al arrancar.
- `postgres`: persistencia relacional.
- `minio`: almacenamiento compatible S3.
- `minio-init`: creacion automatica de buckets.

Los microservicios de negocio (`api-gateway`, `orchestrator`, `face-detection`, etc.) se incorporaran despues, de forma incremental.

## Flujo de eventos (referencia funcional)

1. Cliente envia imagen a `api-gateway`.
2. `api-gateway` publica evento inicial en `images.raw`.
3. `orchestrator` consume y publica `cmd.face_detection`.
4. `face-detection` procesa y publica `images.faces_detected`.
5. `orchestrator` publica `cmd.age_detection`.
6. `age-detection` publica `images.age_estimated`.
7. `orchestrator` decide:
   - si hay menores: `cmd.pixelation`,
   - si no hay menores: `cmd.storage`.
8. `pixelation` publica `images.processed`.
9. `storage-service` persiste estado final y salida.

## Topics definidos

- `images.raw`
- `images.faces_detected`
- `images.age_estimated`
- `images.processed`
- `cmd.face_detection`
- `cmd.age_detection`
- `cmd.pixelation`
- `cmd.storage`
- `events.dead_letter`

## Como ejecutar (Docker Compose minimo)

### Requisitos
- Docker Desktop
- Docker Compose v2

### Arranque
```bash
docker compose up -d
```

### Verificacion rapida
- Kafka Broker (host): `localhost:9092`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- PostgreSQL: `localhost:5432`

### Operar Kafka por consola/scripts
Listar topics:
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
```

Consumir mensajes:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic images.raw --from-beginning
```

Publicar mensaje:
```bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:29092 --topic images.raw
```

### Smoke test automatizado
Ejecutar validacion completa de infraestructura:
```bash
powershell -ExecutionPolicy Bypass -File .\scripts\smoke-test.ps1
```

Si ya tienes los contenedores levantados:
```bash
powershell -ExecutionPolicy Bypass -File .\scripts\smoke-test.ps1 -SkipUp
```

### Parada
```bash
docker compose down
```

Para borrar volumenes:
```bash
docker compose down -v
```

## Estructura esperada del repositorio

```text
Proyecto_Imagenes_IA/
  docker-compose.yml
  README.md
  db/
    init.sql
  contracts/
    *.schema.json
```

## Guia de desarrollo (equipo)

Basada en la especificacion del proyecto:

1. **Planificacion**
   - division por servicios y responsables;
   - definicion de contratos de eventos (JSON Schema por topic).
2. **Implementacion**
   - empezar por scripts/consumidores simples sobre Kafka;
   - incorporar microservicios despues, solo cuando la fase de infraestructura este estable.
3. **Pruebas**
   - unitarias por microservicio;
   - pruebas de integracion con Kafka;
   - validacion end-to-end del flujo completo.

## Gestion de errores (base minima)

- Reintentos controlados para errores transitorios.
- Dead-letter topic (`events.dead_letter`) para mensajes no procesables.
- Registro estructurado con `request_id`/`image_id` para trazabilidad.
- Estados de solicitud e imagen persistidos en BD.

## Estandares para trabajar con Cursor y Claude Code

Para minimizar diferencias entre agentes y entre miembros del equipo:

1. **Fuente de verdad**: `README.md` + contratos de eventos versionados.
2. **Contratos primero**: no se programa un consumidor/productor sin schema definido.
3. **Definition of Done por servicio**:
   - endpoint o consumer operativo,
   - tests basicos,
   - manejo de error + DLQ,
   - logs minimos,
   - documentacion corta de uso.
4. **Commits pequenos y trazables**: 1 objetivo por commit.
5. **Nombres consistentes**:
   - topics: `dominio.accion` o `cmd.*` / `images.*`,
   - consumer groups con sufijo `-group`.
6. **Checklist de PR**:
   - no romper contratos de eventos,
   - actualizar README si cambia arquitectura/flujo,
   - incluir evidencia de prueba.

Recomendacion: crear una carpeta `contracts/` con JSON Schemas por topic para que ambos agentes trabajen con las mismas reglas.
