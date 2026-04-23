FROM python:3.11-slim

ENV YOLO_CONFIG_DIR=/app/.ultralytics
ENV YOLO_TELEMETRY=False
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar PyTorch CPU primero (capa separada para aprovechar cache de Docker)
RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-descargar el modelo YOLOv8-face durante el build.
# Si falla (sin internet), el servicio lo descargara al arrancar.
RUN wget -q -O /app/yolov8n-face.pt \
    "https://github.com/akanametov/yolo-face/releases/download/1.0.0/yolov8n-face.pt" \
    && echo "Modelo YOLOv8-face descargado." \
    || echo "Advertencia: se descargara al arrancar."

COPY . .

CMD ["python", "-u", "main.py"]
