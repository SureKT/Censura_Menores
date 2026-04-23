FROM python:3.11-slim

WORKDIR /app

# PyTorch CPU (no necesita CUDA para inferencia)
RUN pip install --no-cache-dir \
    torch==2.2.0+cpu \
    torchvision==0.17.0+cpu \
    --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-descargar pesos base de MobileNetV2 para evitar descarga en cada arranque
RUN python -c "from torchvision import models; models.mobilenet_v2(weights=models.MobileNet_V2_Weights.IMAGENET1K_V1)"

CMD ["python", "main.py"]
