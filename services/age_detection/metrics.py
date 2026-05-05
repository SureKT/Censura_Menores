"""
Evalua el modelo y genera las 3 graficas clave para revisar su funcionamiento.

Las tres graficas:
  1. Predicha vs Real    — scatter con linea de prediccion perfecta y umbral
  2. MAE por franja      — donde se concentran los errores (critico para ninos)
  3. Confusion matrix    — cuantos menores se escapan (falsos negativos)

Uso:
    python metrics.py                                      # modelo activo, val set
    python metrics.py --model models/v1_20260505_baseline.pth
    python metrics.py --threshold 22 --full-dataset        # todo el dataset
    python metrics.py --compare v1 v2                      # superpone dos modelos en scatter
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")          # sin display — funciona en contenedor y en local
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import torch
from PIL import Image
from torchvision import transforms

from model import build_model
from train import load_samples, stratified_split

# ── Configuracion visual ──────────────────────────────────────────────────────

plt.rcParams.update({
    "font.family":  "sans-serif",
    "font.size":    9,
    "axes.spines.top":   False,
    "axes.spines.right": False,
})

COLOR_MINOR  = "#B43C28"
COLOR_ADULT  = "#3E6B4E"
COLOR_MUTED  = "#7A7773"
COLOR_DANGER = "#B43C28"   # rojo para los FN (menores no detectados)

TRANSFORM_EVAL = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])

# Franjas de edad para el grafico de MAE
AGE_BINS = [
    (0,   4,  "0-4"),
    (5,   9,  "5-9"),
    (10,  12, "10-12"),
    (13,  17, "13-17"),
    (18,  20, "18-20"),
    (21,  30, "21-30"),
    (31,  45, "31-45"),
    (46,  60, "46-60"),
    (61, 120, "61+"),
]

MINOR_BINS = {"0-4", "5-9", "10-12", "13-17"}


def bin_label(age: int) -> str:
    for lo, hi, label in AGE_BINS:
        if lo <= age <= hi:
            return label
    return "?"


# ── Inferencia ────────────────────────────────────────────────────────────────

def load_model(path: str) -> torch.nn.Module:
    model = build_model()
    model.load_state_dict(torch.load(path, map_location="cpu", weights_only=True))
    model.eval()
    return model


def evaluate(model: torch.nn.Module, samples: list) -> tuple[np.ndarray, np.ndarray]:
    """Devuelve (actuals, predictions) en arrays numpy."""
    actuals, predictions = [], []
    n = len(samples)
    for i, (path, actual_age) in enumerate(samples):
        if i % 500 == 0:
            print(f"  {i:>5}/{n}", flush=True)
        try:
            img    = Image.open(path).convert("RGB")
            tensor = TRANSFORM_EVAL(img).unsqueeze(0)
            with torch.no_grad():
                raw = model(tensor).squeeze().item()
            actuals.append(actual_age)
            predictions.append(int(max(0, min(120, round(raw)))))
        except Exception:
            pass   # imagen corrupta — ignorar
    return np.array(actuals), np.array(predictions)


# ── Graficas ──────────────────────────────────────────────────────────────────

def plot_scatter(ax, actuals: np.ndarray, predictions: np.ndarray,
                 threshold: int, label: str = ""):
    """Scatter edad real vs predicha, coloreado por clase real."""
    is_minor = actuals < 18    # coloreamos por edad legal, no por umbral

    ax.scatter(actuals[is_minor],  predictions[is_minor],
               alpha=0.35, s=7, color=COLOR_MINOR,
               label="Menor real (<18)", rasterized=True)
    ax.scatter(actuals[~is_minor], predictions[~is_minor],
               alpha=0.2, s=7, color=COLOR_ADULT,
               label="Adulto real (≥18)", rasterized=True)

    # Linea de prediccion perfecta
    lim = max(int(actuals.max()), int(predictions.max()), 80)
    ax.plot([0, lim], [0, lim], "k--", linewidth=0.9, label="Perfecta")

    # Umbral de clasificacion
    ax.axhline(threshold, color=COLOR_DANGER, linewidth=1.2, linestyle=":",
               alpha=0.9, label=f"Umbral ({threshold}a)")

    mae = np.abs(actuals - predictions).mean()
    titulo = f"Predicha vs Real  —  MAE = {mae:.1f}a"
    if label:
        titulo += f"  [{label}]"
    ax.set_title(titulo)
    ax.set_xlabel("Edad real")
    ax.set_ylabel("Edad predicha")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.legend(fontsize=7.5, markerscale=2)


def plot_mae_by_bin(ax, actuals: np.ndarray, predictions: np.ndarray,
                    threshold: int, label: str = ""):
    """MAE agrupado por franja de edad real."""
    errors_by_bin = defaultdict(list)
    for a, p in zip(actuals, predictions):
        errors_by_bin[bin_label(a)].append(abs(a - p))

    ordered_labels = [lbl for _, _, lbl in AGE_BINS if lbl in errors_by_bin]
    maes   = [np.mean(errors_by_bin[l])  for l in ordered_labels]
    counts = [len(errors_by_bin[l])      for l in ordered_labels]
    colors = [COLOR_MINOR if l in MINOR_BINS else COLOR_MUTED for l in ordered_labels]

    bars = ax.bar(ordered_labels, maes, color=colors, alpha=0.85, edgecolor="white",
                  width=0.6)

    for bar, count, mae_val in zip(bars, counts, maes):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.25,
                f"n={count}\n{mae_val:.1f}a",
                ha="center", va="bottom", fontsize=7)

    titulo = "MAE por franja de edad  (rojo = menores legales)"
    if label:
        titulo += f"  [{label}]"
    ax.set_title(titulo)
    ax.set_xlabel("Franja de edad real")
    ax.set_ylabel("MAE (años)")
    ax.set_ylim(0, max(maes) * 1.35 if maes else 10)
    ax.tick_params(axis="x", rotation=20)

    # Leyenda manual para los colores
    ax.legend(handles=[
        mpatches.Patch(color=COLOR_MINOR, alpha=0.85, label="Menor (<18)"),
        mpatches.Patch(color=COLOR_MUTED, alpha=0.85, label="Adulto (≥18)"),
    ], fontsize=7.5, loc="upper right")


def plot_confusion(ax, actuals: np.ndarray, predictions: np.ndarray,
                   threshold: int):
    """
    Matriz de confusion binaria menor/adulto.
    Resalta en rojo los FN: menores clasificados como adultos (el error mas grave).
    """
    pred_minor = predictions < threshold
    real_minor = actuals < 18

    tp = int(( real_minor &  pred_minor).sum())
    fp = int((~real_minor &  pred_minor).sum())
    fn = int(( real_minor & ~pred_minor).sum())
    tn = int((~real_minor & ~pred_minor).sum())

    # Orden filas/cols: [Adulto=0, Menor=1]
    cm = np.array([[tn, fp],
                   [fn, tp]], dtype=float)

    ax.imshow(cm, cmap="Blues", vmin=0, vmax=max(cm.max(), 1))

    labels = ["Adulto", "Menor"]
    ax.set_xticks([0, 1]); ax.set_xticklabels(labels)
    ax.set_yticks([0, 1]); ax.set_yticklabels(labels)
    ax.set_xlabel("Predicho")
    ax.set_ylabel("Real")
    ax.set_title(f"Confusion matrix  (umbral = {threshold})")

    total = cm.sum()
    for i in range(2):
        for j in range(2):
            val = int(cm[i, j])
            pct = 100 * val / cm[i].sum() if cm[i].sum() > 0 else 0
            color = "white" if val > total / 5 else "black"
            ax.text(j, i, f"{val}\n({pct:.0f}%)",
                    ha="center", va="center",
                    fontsize=9, fontweight="bold", color=color)

    # Marco rojo en FN (fila 1=Menor, col 0=Adulto predicho)
    ax.add_patch(plt.Rectangle(
        (-0.5, 0.5), 1, 1,
        fill=False, edgecolor=COLOR_DANGER, linewidth=2.5,
        zorder=5, label="FN: peligroso"
    ))

    # Metricas bajo el titulo del eje x
    n_real_minor = real_minor.sum()
    recall  = tp / n_real_minor if n_real_minor > 0 else 0
    n_real_adult = (~real_minor).sum()
    fpr     = fp / n_real_adult if n_real_adult > 0 else 0

    ax.set_xlabel(
        f"Predicho\n\n"
        f"Recall menores: {recall:.1%}   |   "
        f"FN (perdidos): {fn} ({1-recall:.1%})   |   "
        f"Falsas alarmas: {fpr:.1%}"
    )


# ── Reporte principal ─────────────────────────────────────────────────────────

def generate_report(model_path: str, dataset_root: str,
                    threshold: int, full_dataset: bool,
                    out_dir: str | None = None) -> str:
    print(f"\nModelo : {model_path}")
    model = load_model(model_path)

    print(f"Dataset: {dataset_root}")
    all_samples = load_samples(Path(dataset_root))
    if not all_samples:
        raise RuntimeError(f"Sin imágenes en {dataset_root}")

    if full_dataset:
        samples     = all_samples
        split_label = f"dataset completo ({len(samples)} imgs)"
    else:
        _, val_samples = stratified_split(all_samples)
        samples        = val_samples
        split_label    = f"val set ({len(samples)} imgs)"

    print(f"Split  : {split_label}")
    actuals, predictions = evaluate(model, samples)

    # ── Layout 2×2 (scatter | confusion matrix / MAE a lo ancho) ─────────────
    fig = plt.figure(figsize=(15, 10))
    fig.suptitle(
        f"Métricas: {Path(model_path).name}   ·   umbral menor: edad < {threshold}   ·   {split_label}",
        fontsize=10, fontweight="bold", y=0.99,
    )

    gs  = fig.add_gridspec(2, 2, hspace=0.55, wspace=0.35,
                           top=0.93, bottom=0.08, left=0.07, right=0.97)
    ax_scatter = fig.add_subplot(gs[0, 0])
    ax_cm      = fig.add_subplot(gs[0, 1])
    ax_mae     = fig.add_subplot(gs[1, :])   # ocupa las dos columnas

    plot_scatter(ax_scatter, actuals, predictions, threshold)
    plot_confusion(ax_cm,    actuals, predictions, threshold)
    plot_mae_by_bin(ax_mae,  actuals, predictions, threshold)

    # ── Guardar ───────────────────────────────────────────────────────────────
    dest_dir  = Path(out_dir) if out_dir else Path(model_path).parent
    dest_dir.mkdir(parents=True, exist_ok=True)
    stem      = Path(model_path).stem
    out_path  = dest_dir / f"{stem}_metrics.png"

    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"\nGráfica guardada: {out_path}\n")
    return str(out_path)


# ── Modo comparacion (dos modelos en el mismo scatter) ────────────────────────

def generate_comparison(model_ids: list[str], dataset_root: str,
                        threshold: int, full_dataset: bool) -> str:
    """Compara dos versiones del modelo en una figura de scatter + MAE lado a lado."""
    models_dir = Path("models")

    # Resolver rutas por id (v1, v2, …) o ruta directa
    def resolve(mid: str) -> Path:
        p = Path(mid)
        if p.exists():
            return p
        import json
        reg = json.loads((models_dir / "registry.json").read_text(encoding="utf-8"))
        for v in reg["versions"]:
            if v["id"] == mid:
                return models_dir / v["filename"]
        raise FileNotFoundError(f"Version '{mid}' no encontrada.")

    paths = [resolve(m) for m in model_ids[:2]]

    all_samples = load_samples(Path(dataset_root))
    if full_dataset:
        samples = all_samples
    else:
        _, samples = stratified_split(all_samples)

    results = []
    for path in paths:
        print(f"\nEvaluando {path.name}...")
        m = load_model(str(path))
        a, p = evaluate(m, samples)
        results.append((path.stem, a, p))

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        f"Comparativa: {paths[0].name}  vs  {paths[1].name}\n"
        f"umbral={threshold}  ·  {'dataset completo' if full_dataset else 'val set'}",
        fontsize=10, fontweight="bold",
    )
    for ax, (lbl, actuals, predictions) in zip(axes, results):
        plot_scatter(ax, actuals, predictions, threshold, label=lbl)

    out_path = Path("models") / f"compare_{'_vs_'.join(model_ids)}_metrics.png"
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"Comparativa guardada: {out_path}\n")
    return str(out_path)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Genera métricas y gráficas del modelo de edad",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python metrics.py
  python metrics.py --model models/v1_20260505_baseline.pth
  python metrics.py --threshold 22 --full-dataset
  python metrics.py --compare v1 v2
        """,
    )
    parser.add_argument("--model",        default="age_model.pth",
                        help="Ruta al .pth o 'age_model.pth' para el activo (default)")
    parser.add_argument("--dataset",      default="../../dataset/face_age")
    parser.add_argument("--threshold",    type=int, default=22,
                        help="Umbral de edad para clasificar como menor (default: 22)")
    parser.add_argument("--full-dataset", action="store_true",
                        help="Evaluar sobre todo el dataset (default: solo val set)")
    parser.add_argument("--out-dir",      default=None,
                        help="Carpeta de salida para la imagen (default: misma carpeta que el modelo)")
    parser.add_argument("--compare",      nargs=2, metavar=("V1", "V2"),
                        help="Compara dos versiones: --compare v1 v2")
    args = parser.parse_args()

    if args.compare:
        generate_comparison(args.compare, args.dataset, args.threshold, args.full_dataset)
    else:
        if not Path(args.model).exists():
            print(f"Error: modelo no encontrado en '{args.model}'")
            sys.exit(1)
        generate_report(args.model, args.dataset, args.threshold,
                        args.full_dataset, args.out_dir)
