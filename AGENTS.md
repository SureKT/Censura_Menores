# AGENTS.md

Guia comun para mantener consistencia entre desarrollo asistido por IA (Cursor, Claude Code) y trabajo manual.

## 1) Principios de colaboracion

- Un solo objetivo por tarea/PR.
- No cambiar contratos de eventos sin coordinar al equipo.
- Documentar cualquier cambio de arquitectura en `README.md`.
- Priorizar cambios pequenos, revisables y con pruebas.

## 2) Convenciones de eventos

- Topics de dominio: `images.*`.
- Topics de comando: `cmd.*`.
- Topic de errores no recuperables: `events.dead_letter`.
- Todo mensaje debe incluir identificadores trazables (`request_id`, `image_id` cuando aplique).

## 3) Contratos y versionado

- Los contratos JSON Schema se guardan en `contracts/`.
- Regla: "contract-first" (definir schema antes de implementar producer/consumer).
- Cambios breaking requieren version de schema y plan de migracion.

## 4) Definition of Done (por microservicio)

Para considerar una tarea completada:
- Producer/consumer implementado y operativo.
- Manejo de errores basico + envio a DLQ cuando no se pueda procesar.
- Logging minimo de entrada, salida y error.
- Tests minimos (unitarios y/o integracion segun impacto).
- Documentacion breve de ejecucion/configuracion.

## 5) Estilo de trabajo recomendado

- Commits pequenos y descriptivos.
- No mezclar refactor grande con cambio funcional.
- Mantener nombres y variables de entorno coherentes con `docker-compose.yml`.
- Si una decision afecta a varios servicios, abrir issue o nota tecnica antes de implementar.

## 6) Checklist previo a merge

- [ ] No se rompieron topics ni formatos de eventos existentes.
- [ ] Se actualizo documentacion necesaria (`README.md`, contratos).
- [ ] Se validaron pruebas locales relevantes.
- [ ] Se incluyeron notas de riesgos o limitaciones conocidas.
