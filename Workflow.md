# Workflow del Proyecto: Identificación y Pixelado de Rostros

Este documento detalla el flujo de trabajo y las responsabilidades de los 12 contenedores que componen el sistema distribuido basado en eventos. La arquitectura separa la infraestructura, las APIs de interfaz, la lógica de procesamiento (IA) y la orquestación.

## 1. Resumen de Contenedores (12)

| Categoría | Contenedores |
| :--- | :--- |
| **Infraestructura** | Kafka, MinIO, PostgreSQL |
| **APIs** | API Ingesta, API Consulta |
| **Lógica (IA)** | Face Detection, Age Detection, Pixelation |
| **Orquestación** | Orq. Entrada, Orq. Análisis, Orq. Decisión, Orq. Finalización |

---

## 2. Descripción Detallada del Flujo

### Fase I: Ingesta e Inicialización
1.  **API Ingesta**:
    * Recibe la imagen mediante un endpoint REST.
    * Sube el archivo original a **MinIO**.
    * Publica el evento inicial en el topic `images.raw`.
2.  **Orquestador de Entrada**:
    * **Trigger**: Escucha el topic `images.raw`.
    * **Interacción BD**: Inserta el registro en la tabla `Solicitud`. Genera el `GUID_Solicitud`, establece el `Estado` como 'RECIBIDO' y marca el `Inicio_Solicitud`.
    * **Kafka**: Publica el comando `cmd.face_detection`.

### Fase II: Análisis de Rostros
3.  **Face Detection Service (Lógica)**:
    * Realiza la detección de coordenadas de rostros en la imagen.
    * Publica el evento `evt.face_detection.completed`.
4.  **Orquestador de Análisis**:
    * **Trigger**: Escucha `evt.face_detection.completed`.
    * **Interacción BD**: 
        * Registra `Inicio_Deteccion_Caras` y `Fin_Deteccion_Caras` en la tabla `Solicitud`.
        * Actualiza `Num_Imagenes_Total` con la cantidad de rostros detectados.
        * Inserta una fila en la tabla `Imagenes` por cada rostro detectado para seguimiento individual.
        * Marca `Inicio_Edad` en la tabla `Solicitud`.
    * **Kafka**: Publica el comando `cmd.age_detection`.

### Fase III: Clasificación y Decisión
5.  **Age Detection Service (Lógica)**:
    * Estima la edad de los rostros detectados.
    * Publica el evento `evt.age_detection.completed`.
6.  **Orquestador de Decisión**:
    * **Trigger**: Escucha `evt.age_detection.completed`.
    * **Interacción BD**:
        * Registra `Fin_Edad` en la tabla `Solicitud`.
        * Actualiza el estado de cada registro en la tabla `Imagenes` (Menor/Adulto).
        * Calcula y actualiza `Num_Imagenes_Pixeladas`.
        * Si hay menores: Marca `Inicio_Pixelado` en `Solicitud`.
    * **Kafka**: 
        * Si hay menores: Publica `cmd.pixelation`.
        * Si NO hay menores: Publica directamente `cmd.storage`.

### Fase IV: Procesamiento Final y Cierre
7.  **Pixelation Service (Lógica)**:
    * Aplica el filtro de pixelado en las coordenadas indicadas.
    * Publica el evento `evt.pixelation.completed`.
8.  **Orquestador de Finalización**:
    * **Trigger**: Escucha `evt.pixelation.completed` o el salto desde el Orquestador de Decisión.
    * **Interacción BD**:
        * Marca `Fin_Pixelado` (si aplica).
        * Registra `Inicio_Almacenamiento_Solicitud`.
        * Tras confirmar el guardado del resultado en **MinIO**: Registra `Fin_Almacenamiento_Solicitud`, `Fin_Solicitud` y cambia el estado global a 'COMPLETED'.
    * **Kafka**: Publica `evt.storage.completed`.

---

## 3. Interfaz de Usuario (Consulta)
* **API Consulta**:
    * Permite al usuario enviar su `GUID_Solicitud`.
    * Consulta en **PostgreSQL** los tiempos y estados de las tablas `Solicitud` e `Imagenes`.
    * Si el estado es 'COMPLETED', devuelve la URL de descarga del objeto en **MinIO**.

---

## 4. Esquema de Base de Datos Relacionado

Los orquestadores garantizan la integridad de estas tablas durante todo el ciclo de vida:
* **Solicitud**: Controla los tiempos de cada bloque lógico y el estado global del pipeline.
* **Imagenes**: Desglose detallado de cada entidad detectada dentro de una solicitud.