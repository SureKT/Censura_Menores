"""
Entrena MobileNetV2 para regresion de edad.

Mejoras respecto a la version anterior:
  - Aumento de datos moderado (rotacion, jitter, erasing) sin distorsiones agresivas
  - Split estratificado por franja de edad
  - WeightedRandomSampler para compensar el desbalance adultos/menores
  - BoundaryAwareLoss: HuberLoss + penalizacion en el umbral 18 anos
  - Entrenamiento en 2 fases: backbone congelado (8 epocas) -> fine-tuning completo
  - Scheduler CosineAnnealingLR + AdamW con LR diferenciado
  - AMP (FP16) en GPU con gradient clipping (max_norm=5)
  - Barra de progreso por batch, tiempos por epoca, normas de gradiente

Uso:
    python train.py --dataset ../../dataset/face_age --epochs 25
"""
import argparse
import random
import time
from collections import Counter
from pathlib import Path

import torch
import torch.nn as nn
import torch.nn.functional as F
from PIL import Image
from torch.utils.data import DataLoader, Dataset, WeightedRandomSampler
from torchvision import transforms
try:
    from tqdm import tqdm
except ImportError:
    class _TqdmFallback:
        """
        Fallback ligero cuando tqdm no esta instalado.
        Mantiene la misma interfaz minima usada en este script.
        """
        def __init__(self, iterable, *args, **kwargs):
            self._iterable = iterable

        def __iter__(self):
            return iter(self._iterable)

        def set_postfix(self, **kwargs):
            pass

        def close(self):
            pass

    def tqdm(iterable, *args, **kwargs):
        return _TqdmFallback(iterable, *args, **kwargs)

from model import build_model


# ── Pipelines de transformacion ───────────────────────────────────────────────

TRANSFORM_TRAIN = transforms.Compose([
    transforms.Resize((256, 256)),
    transforms.RandomCrop(224),
    transforms.RandomHorizontalFlip(),
    transforms.RandomRotation(10),
    transforms.ColorJitter(brightness=0.3, contrast=0.3, saturation=0.2, hue=0.05),
    transforms.RandomGrayscale(p=0.05),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
    transforms.RandomErasing(p=0.1, scale=(0.02, 0.10)),
])

TRANSFORM_VAL = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])


# ── Dataset ───────────────────────────────────────────────────────────────────

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


# ── Estratificacion y muestreo ────────────────────────────────────────────────

def age_bin(age: int) -> int:
    """Agrupa edades en franjas para estratificacion y muestreo."""
    if age <= 4:   return 0
    if age <= 9:   return 1
    if age <= 12:  return 2
    if age <= 17:  return 3  # menores proximos al umbral
    if age <= 20:  return 4  # adultos jovenes proximos al umbral
    if age <= 30:  return 5
    if age <= 45:  return 6
    return 7


def stratified_split(
    samples: list[tuple[Path, int]],
    val_ratio: float = 0.15,
    seed: int = 42,
) -> tuple[list, list]:
    """Split manteniendo la proporcion de cada franja de edad en val."""
    rng = random.Random(seed)
    by_bin: dict[int, list] = {}
    for s in samples:
        by_bin.setdefault(age_bin(s[1]), []).append(s)

    train, val = [], []
    for items in by_bin.values():
        shuffled = items[:]
        rng.shuffle(shuffled)
        n_val = max(1, int(len(shuffled) * val_ratio))
        val.extend(shuffled[:n_val])
        train.extend(shuffled[n_val:])
    return train, val


def make_weighted_sampler(samples: list[tuple[Path, int]]) -> WeightedRandomSampler:
    """
    Sobremuestra las franjas de edad infrarrepresentadas.
    Las franjas 10-17 y 18-20 reciben peso extra por estar cerca del umbral de decision.
    """
    bins = [age_bin(age) for _, age in samples]
    bin_counts = Counter(bins)
    boundary_boost = {2: 1.5, 3: 2.0, 4: 1.5}  # bins 10-12, 13-17, 18-20
    weights = [
        boundary_boost.get(b, 1.0) / bin_counts[b]
        for b in bins
    ]
    return WeightedRandomSampler(weights, num_samples=len(weights), replacement=True)


# ── Loss con consciencia del umbral 18 ───────────────────────────────────────

class BoundaryAwareLoss(nn.Module):
    """
    HuberLoss para la regresion de edad + BCE en el umbral de 18 anos.

    El termino BCE penaliza especialmente los errores que invierten la
    clasificacion menor/adulto (p.ej. predecir 19 cuando la edad real es 16).
    Devuelve (total, huber, boundary) para poder mostrar cada componente.
    """
    def __init__(self, boundary: float = 18.0, alpha: float = 0.3):
        super().__init__()
        self.boundary = boundary
        self.alpha = alpha

    def forward(
        self, preds: torch.Tensor, targets: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        huber = F.huber_loss(preds, targets, delta=3.0)
        true_minor = (targets < self.boundary).float()
        pred_logits_minor = (self.boundary - preds) * 0.5
        boundary = F.binary_cross_entropy_with_logits(pred_logits_minor, true_minor)
        return huber + self.alpha * boundary, huber.detach(), boundary.detach()


# ── Helpers de congelado ──────────────────────────────────────────────────────

def freeze_backbone(model: nn.Module):
    for name, param in model.named_parameters():
        if "classifier" not in name:
            param.requires_grad = False


def unfreeze_all(model: nn.Module):
    for param in model.parameters():
        param.requires_grad = True


def grad_norm(module: nn.Module) -> float:
    """Norma L2 de los gradientes de un modulo (0.0 si aun no hay backward)."""
    total = 0.0
    for p in module.parameters():
        if p.grad is not None:
            total += p.grad.detach().norm(2).item() ** 2
    return total ** 0.5


# ── Bucle de entrenamiento ────────────────────────────────────────────────────

def run_epoch(
    model: nn.Module,
    loader: DataLoader,
    criterion: BoundaryAwareLoss,
    optimizer,
    device: torch.device,
    is_train: bool,
    scaler=None,
    desc: str = "",
) -> dict:
    """
    Ejecuta una epoca completa y devuelve metricas detalladas.
    Muestra una barra tqdm con loss y MAE actualizados batch a batch.
    """
    model.train(is_train)
    use_amp = scaler is not None
    total_loss = total_huber = total_boundary = total_mae = 0.0
    n_seen = 0

    pbar = tqdm(loader, desc=desc, unit="batch", ncols=100, leave=False)

    if is_train:
        for imgs, ages in pbar:
            imgs, ages = imgs.to(device), ages.to(device)
            optimizer.zero_grad()
            with torch.amp.autocast(device_type=device.type, enabled=use_amp):
                preds = model(imgs).squeeze(1)
                loss, huber, boundary = criterion(preds, ages)
            if use_amp:
                scaler.scale(loss).backward()
                scaler.unscale_(optimizer)
                nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
                scaler.step(optimizer)
                scaler.update()
            else:
                loss.backward()
                nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
                optimizer.step()

            b = len(imgs)
            n_seen        += b
            total_loss    += loss.item()    * b
            total_huber   += huber.item()   * b
            total_boundary += boundary.item() * b
            total_mae     += (preds.detach() - ages).abs().sum().item()

            pbar.set_postfix(
                loss=f"{total_loss/n_seen:.3f}",
                mae=f"{total_mae/n_seen:.1f}a",
                bnd=f"{total_boundary/n_seen:.3f}",
            )
    else:
        with torch.no_grad():
            for imgs, ages in pbar:
                imgs, ages = imgs.to(device), ages.to(device)
                with torch.amp.autocast(device_type=device.type, enabled=use_amp):
                    preds = model(imgs).squeeze(1)
                    loss, huber, boundary = criterion(preds, ages)

                b = len(imgs)
                n_seen        += b
                total_loss    += loss.item()    * b
                total_huber   += huber.item()   * b
                total_boundary += boundary.item() * b
                total_mae     += (preds - ages).abs().sum().item()

                pbar.set_postfix(
                    loss=f"{total_loss/n_seen:.3f}",
                    mae=f"{total_mae/n_seen:.1f}a",
                )

    pbar.close()
    n = len(loader.dataset)
    return {
        "loss":     total_loss     / n,
        "huber":    total_huber    / n,
        "boundary": total_boundary / n,
        "mae":      total_mae      / n,
    }


# ── Helpers de display ────────────────────────────────────────────────────────

def fmt_time(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def print_epoch(tag: str, epoch: int, total: int, train: dict, val: dict,
                epoch_secs: float, elapsed_secs: float,
                lr_backbone: float | None, lr_head: float, marker: str):
    lr_str = (f"lr backbone={lr_backbone:.1e} head={lr_head:.1e}"
              if lr_backbone is not None else f"lr={lr_head:.1e}")
    print(
        f"  {tag} {epoch:02d}/{total} │ "
        f"train loss={train['loss']:.3f} (huber={train['huber']:.3f} bnd={train['boundary']:.3f}) │ "
        f"val loss={val['loss']:.3f}  MAE={val['mae']:.1f}a │ "
        f"{lr_str} │ "
        f"epoca {fmt_time(epoch_secs)}  total {fmt_time(elapsed_secs)}"
        f"{marker}"
    )


def print_grad_norms(model: nn.Module):
    bb_norm = grad_norm(model.features)
    cl_norm = grad_norm(model.classifier)
    print(f"         grad_norm  backbone={bb_norm:.4f}  classifier={cl_norm:.4f}")


# ── Entrenamiento principal ───────────────────────────────────────────────────

def train(
    dataset_root: str,
    output_path: str,
    epochs: int,
    batch_size: int,
    lr: float,
    freeze_epochs: int,
):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Dispositivo: {device}", end="")
    if device.type == "cuda":
        print(f"  ({torch.cuda.get_device_name(0)}, AMP activado)")
    else:
        print("  (CPU — instala torch+cu124 para usar la GPU)")

    all_samples = load_samples(Path(dataset_root))
    if not all_samples:
        raise RuntimeError(f"No se encontraron imagenes en {dataset_root}")
    print(f"Imagenes totales: {len(all_samples)}")

    train_raw, val_raw = stratified_split(all_samples)
    n_minors = sum(1 for _, age in train_raw if age < 18)
    print(f"Train: {len(train_raw)}  Val: {len(val_raw)}  "
          f"(menores en train: {n_minors} = {100*n_minors/len(train_raw):.1f}%)")

    n_workers = 4 if device.type == "cuda" else 2
    sampler = make_weighted_sampler(train_raw)
    train_loader = DataLoader(
        AgeDataset(train_raw, TRANSFORM_TRAIN),
        batch_size=batch_size, sampler=sampler,
        num_workers=n_workers, pin_memory=(device.type == "cuda"),
        persistent_workers=(n_workers > 0),
    )
    val_loader = DataLoader(
        AgeDataset(val_raw, TRANSFORM_VAL),
        batch_size=batch_size, shuffle=False,
        num_workers=n_workers, persistent_workers=(n_workers > 0),
    )

    model     = build_model().to(device)
    criterion = BoundaryAwareLoss()
    scaler    = torch.amp.GradScaler("cuda") if device.type == "cuda" else None
    t_start   = time.time()

    # ── Fase 1: backbone congelado, solo clasificador ─────────────────────────
    if freeze_epochs > 0:
        freeze_backbone(model)
        opt1 = torch.optim.Adam(
            filter(lambda p: p.requires_grad, model.parameters()), lr=lr
        )
        print(f"\n── Fase 1: backbone congelado ({freeze_epochs} epocas) ──")
        for epoch in range(1, freeze_epochs + 1):
            t0 = time.time()
            tr = run_epoch(model, train_loader, criterion, opt1, device,
                           is_train=True,  scaler=scaler,
                           desc=f"[F1 train {epoch:02d}/{freeze_epochs}]")
            vl = run_epoch(model, val_loader,   criterion, None, device,
                           is_train=False, scaler=scaler,
                           desc=f"[F1 val   {epoch:02d}/{freeze_epochs}]")
            print_epoch("F1", epoch, freeze_epochs, tr, vl,
                        time.time() - t0, time.time() - t_start,
                        None, lr, "")
            print_grad_norms(model)

    # ── Fase 2: fine-tuning completo con LR diferenciado ─────────────────────
    unfreeze_all(model)
    optimizer = torch.optim.AdamW(
        [
            {"params": model.features.parameters(),   "lr": lr * 0.1},
            {"params": model.classifier.parameters(), "lr": lr},
        ],
        weight_decay=1e-4,
    )
    main_epochs = epochs - freeze_epochs
    scheduler   = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=main_epochs)

    best_mae = float("inf")
    print(f"\n── Fase 2: fine-tuning completo ({main_epochs} epocas) ──")
    for epoch in range(1, main_epochs + 1):
        t0 = time.time()
        tr = run_epoch(model, train_loader, criterion, optimizer, device,
                       is_train=True,  scaler=scaler,
                       desc=f"[F2 train {epoch:02d}/{main_epochs}]")
        vl = run_epoch(model, val_loader,   criterion, None,      device,
                       is_train=False, scaler=scaler,
                       desc=f"[F2 val   {epoch:02d}/{main_epochs}]")
        scheduler.step()

        marker = "  ★ mejor" if vl["mae"] < best_mae else ""
        lr_bb  = optimizer.param_groups[0]["lr"]
        lr_cl  = optimizer.param_groups[1]["lr"]
        print_epoch("F2", epoch, main_epochs, tr, vl,
                    time.time() - t0, time.time() - t_start,
                    lr_bb, lr_cl, marker)
        print_grad_norms(model)

        if vl["mae"] < best_mae:
            best_mae = vl["mae"]
            torch.save(model.state_dict(), output_path)

    total = time.time() - t_start
    print(f"\n{'─'*80}")
    print(f"Entrenamiento finalizado en {fmt_time(total)}")
    print(f"Mejor MAE en validacion: {best_mae:.1f} anos  →  {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrena el modelo de estimacion de edad")
    parser.add_argument("--dataset",       default="../../dataset/face_age")
    parser.add_argument("--output",        default="age_model.pth")
    parser.add_argument("--epochs",        type=int,   default=25)
    parser.add_argument("--batch",         type=int,   default=32)
    parser.add_argument("--lr",            type=float, default=1e-3)
    parser.add_argument("--freeze-epochs", type=int,   default=8)
    args = parser.parse_args()

    train(args.dataset, args.output, args.epochs, args.batch, args.lr, args.freeze_epochs)
