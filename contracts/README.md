# Contratos de Eventos (JSON Schema)

Este directorio define los contratos **v1** de todos los topics Kafka del proyecto.

## Estructura

- `_defs.schema.json`: definiciones compartidas (`event metadata`, `bounding_box`, etc.).
- `images.raw.v1.schema.json`
- `cmd.face_detection.v1.schema.json`
- `images.faces_detected.v1.schema.json`
- `cmd.age_detection.v1.schema.json`
- `images.age_estimated.v1.schema.json`
- `cmd.pixelation.v1.schema.json`
- `images.processed.v1.schema.json`
- `cmd.storage.v1.schema.json`
- `events.dead_letter.v1.schema.json`

## Convenciones

- Todos los eventos incluyen:
  - `event_id` (uuid)
  - `event_type`
  - `event_version` (constante `v1`)
  - `occurred_at` (date-time UTC)
  - `trace` (`request_id`, `image_id`)
- Los productores deben validar el payload antes de publicar.
- Los consumidores deben validar el mensaje al consumir.
- En caso de error no recuperable, publicar en `events.dead_letter`.

## Compatibilidad

- Cambios **breaking** requieren un nuevo schema versionado (`v2`, etc.).
- No modificar restricciones de `v1` de forma incompatible.
