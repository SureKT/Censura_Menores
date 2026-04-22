"""
Prueba el modelo con imágenes propias.

Uso:
    # Una imagen
    python test.py foto.jpg

    # Varias imágenes o carpeta
    python test.py foto1.jpg foto2.png carpeta/

    # Modelo distinto
    python test.py foto.jpg --model mi_modelo.pth
"""
import argparse
import sys
from pathlib import Path

import torch
from PIL import Image
from torchvision import transforms

from model import build_model, predict_age

EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}

TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])


def collect_images(paths: list[str]) -> list[Path]:
    images = []
    for p in paths:
        path = Path(p)
        if path.is_dir():
            for ext in EXTENSIONS:
                images.extend(path.glob(f"*{ext}"))
                images.extend(path.glob(f"*{ext.upper()}"))
        elif path.suffix.lower() in EXTENSIONS:
            images.append(path)
        else:
            print(f"[aviso] Ignorando {p} (extensión no soportada)")
    return sorted(set(images))


def run(model, images: list[Path]):
    print(f"\n{'Imagen':<45} {'Edad':>5}  {'Menor':>6}  {'Confianza':>10}")
    print("-" * 72)
    for img_path in images:
        try:
            img = Image.open(img_path).convert("RGB")
            tensor = TRANSFORM(img).unsqueeze(0)
            age, confidence = predict_age(model, tensor)
            menor = "SI" if age < 18 else "no"
            print(f"{str(img_path):<45} {age:>5}  {menor:>6}  {confidence:>10.4f}")
        except Exception as e:
            print(f"{str(img_path):<45}  ERROR: {e}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="Imágenes o carpetas a evaluar")
    parser.add_argument("--model", default="age_model.pth", help="Ruta al modelo entrenado")
    args = parser.parse_args()

    model_path = Path(args.model)
    if not model_path.exists():
        print(f"Error: modelo no encontrado en '{model_path}'. Entrena primero con train.py")
        sys.exit(1)

    print(f"Cargando modelo desde {model_path}...")
    model = build_model()
    model.load_state_dict(torch.load(model_path, map_location="cpu", weights_only=True))
    model.eval()

    images = collect_images(args.inputs)
    if not images:
        print("No se encontraron imágenes.")
        sys.exit(1)

    print(f"Evaluando {len(images)} imagen(es)...")
    run(model, images)
