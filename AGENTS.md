# AGENTS.md

Guia comun para mantener consistencia entre desarrollo asistido por IA (Cursor, Claude Code) y trabajo manual.

## 1) Norma de simplicidad (obligatoria)

- Mantener **una sola documentacion principal**: `README.md`.
- No crear nuevos `README*`, docs largas o guias extra salvo peticion explicita.
- Respuestas y cambios de agentes: breves, accionables y sin texto redundante.
- Preferir scripts y comandos reproducibles antes que explicaciones extensas.
- Si una explicacion supera 8-10 lineas, convertirla en checklist corto.

## 2) Principios de colaboracion

- Un solo objetivo por tarea/PR.
- No cambiar contratos de eventos sin coordinar al equipo.
- Documentar cualquier cambio de arquitectura en `README.md`.
- Priorizar cambios pequenos, revisables y con pruebas.

## 3) Convenciones de eventos

Topics de ingesta/resultado:
- `images.raw` — imagen recibida por API Ingesta.
- `images.faces_detected` — coordenadas de rostros detectados.
- `images.age_estimated` — edades estimadas por rostro.
- `images.processed` — imagen final pixelada lista para almacenar.

Topics de eventos de servicio (publicados por servicios IA):
- `evt.face_detection.completed`
- `evt.age_detection.completed`
- `evt.pixelation.completed`
- `evt.storage.completed`

Topics de comando (publicados por orquestadores):
- `cmd.face_detection`, `cmd.age_detection`, `cmd.pixelation`, `cmd.storage`.

Topic de errores no recuperables: `events.dead_letter`.

Todo mensaje debe incluir identificadores trazables (`GUID_Solicitud`, `image_id` cuando aplique).

## 4) Contratos y versionado

- Los contratos JSON Schema se guardan en `contracts/`.
- Regla: "contract-first" minima (solo contratos necesarios para la fase actual).
- Cambios breaking requieren version de schema y plan de migracion.
- Respetar el flujo de fases definido en `Workflow.md`: Ingesta → Analisis → Decision → Finalizacion.

## 5) Definition of Done (por microservicio)

Servicios del pipeline (ver `Workflow.md` para detalle de cada uno):
- **APIs**: API Ingesta, API Consulta.
- **Logica IA**: Face Detection, Age Detection, Pixelation.
- **Orquestadores**: Orq. Analisis, Orq. Decision, Orq. Finalizacion.
- **Infraestructura**: Kafka, MinIO, PostgreSQL.

Para considerar una tarea completada:
- Producer/consumer implementado y operativo.
- Interaccion con BD correcta (tablas `Solicitud` e `Imagenes` segun fase).
- Manejo de errores basico + envio a DLQ cuando no se pueda procesar.
- Logging minimo de entrada, salida y error.
- Tests minimos (unitarios y/o integracion segun impacto).
- Documentacion breve de ejecucion/configuracion.

## 6) Estilo de trabajo recomendado

- Commits pequenos y descriptivos.
- No mezclar refactor grande con cambio funcional.
- Mantener nombres y variables de entorno coherentes con `docker-compose.yml`.
- Si una decision afecta a varios servicios, abrir issue o nota tecnica antes de implementar.

## 7) Checklist previo a merge

- [ ] No se rompieron topics ni formatos de eventos existentes.
- [ ] Se actualizo documentacion necesaria (`README.md`, contratos).
- [ ] Se validaron pruebas locales relevantes.
- [ ] Se incluyeron notas de riesgos o limitaciones conocidas.
