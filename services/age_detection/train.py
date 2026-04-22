"""
Entrena MobileNetV2 para regresion de edad.

Uso:
    python train.py --dataset ../../dataset/face_age --epochs 15

El modelo se guarda en age_model.pth (o --output).
"""
import argparse
import random
from pathlib import Path

import torch
import torch.nn as nn
from PIL import Image
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms

from model import build_model


TRANSFORM_TRAIN = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.RandomHorizontalFlip(),
    transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.1),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])

TRANSFORM_VAL = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])


class AgeDataset(Dataset):
    def __init__(self, samples: list[tuple[Path, int]], transform=None):
        self.samples = samples
        self.transform = transform

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        path, age = self.samples[idx]
        img = Image.open(path).convert("RGB")
        if self.transform:
            img = self.transform(img)
        return img, torch.tensor(float(age))


def load_samples(root: Path) -> list[tuple[Path, int]]:
    samples = []
    for folder in root.iterdir():
        if not folder.is_dir():
            continue
        try:
            age = int(folder.name)
        except ValueError:
            continue
        for ext in ("*.png", "*.jpg", "*.jpeg"):
            for img_path in folder.glob(ext):
                samples.append((img_path, age))
    return samples


def split(samples, val_ratio=0.15, seed=42):
    rng = random.Random(seed)
    shuffled = samples[:]
    rng.shuffle(shuffled)
    n_val = max(1, int(len(shuffled) * val_ratio))
    return shuffled[n_val:], shuffled[:n_val]


def train(dataset_root: str, output_path: str, epochs: int, batch_size: int, lr: float):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Dispositivo: {device}")

    all_samples = load_samples(Path(dataset_root))
    if not all_samples:
        raise RuntimeError(f"No se encontraron imágenes en {dataset_root}")
    print(f"Imágenes totales: {len(all_samples)}")

    train_raw, val_raw = split(all_samples)
    print(f"Train: {len(train_raw)}  Val: {len(val_raw)}")

    train_loader = DataLoader(
        AgeDataset(train_raw, TRANSFORM_TRAIN),
        batch_size=batch_size, shuffle=True, num_workers=2, pin_memory=device.type == "cuda",
    )
    val_loader = DataLoader(
        AgeDataset(val_raw, TRANSFORM_VAL),
        batch_size=batch_size, shuffle=False, num_workers=2,
    )

    model = build_model().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=5, gamma=0.5)
    criterion = nn.HuberLoss()

    best_mae = float("inf")
    for epoch in range(1, epochs + 1):
        # --- Train ---
        model.train()
        train_loss = 0.0
        for imgs, ages in train_loader:
            imgs, ages = imgs.to(device), ages.to(device)
            optimizer.zero_grad()
            loss = criterion(model(imgs).squeeze(1), ages)
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * len(imgs)
        train_loss /= len(train_raw)

        # --- Validation ---
        model.eval()
        val_loss = val_mae = 0.0
        with torch.no_grad():
            for imgs, ages in val_loader:
                imgs, ages = imgs.to(device), ages.to(device)
                preds = model(imgs).squeeze(1)
                val_loss += criterion(preds, ages).item() * len(imgs)
                val_mae += (preds - ages).abs().sum().item()
        val_loss /= len(val_raw)
        val_mae /= len(val_raw)

        scheduler.step()
        marker = " *" if val_mae < best_mae else ""
        print(f"Epoch {epoch:02d}/{epochs}  train={train_loss:.3f}  val_loss={val_loss:.3f}  MAE={val_mae:.1f} años{marker}")

        if val_mae < best_mae:
            best_mae = val_mae
            torch.save(model.state_dict(), output_path)

    print(f"\nEntrenamiento finalizado. Mejor MAE={best_mae:.1f} años → {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrena el modelo de estimación de edad")
    parser.add_argument("--dataset", default="../../dataset/face_age", help="Carpeta raíz del dataset")
    parser.add_argument("--output",  default="age_model.pth",          help="Ruta de salida del modelo")
    parser.add_argument("--epochs",  type=int,   default=15)
    parser.add_argument("--batch",   type=int,   default=32)
    parser.add_argument("--lr",      type=float, default=1e-3)
    args = parser.parse_args()

    train(args.dataset, args.output, args.epochs, args.batch, args.lr)
