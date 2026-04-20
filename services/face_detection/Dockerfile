FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-descargar el modelo YOLOv8-face durante el build.
# Si falla (sin internet), el servicio lo descargara al arrancar.
RUN wget -q -O /app/yolov8n-face.pt \
    "https://github.com/akanametov/yolo-face/releases/download/v0.0.0/yolov8n-face.pt" \
    && echo "Modelo YOLOv8-face descargado." \
    || echo "Advertencia: se descargara al arrancar."

COPY . .

CMD ["python", "main.py"]
