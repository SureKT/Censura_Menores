"""Copia sincronizada de services/age_detection/model.py.

Mantener coherente con el entrenador. Si cambia la arquitectura del modelo alla,
hay que reflejarlo aqui para que load_state_dict funcione.
"""

import math

import torch
import torch.nn as nn
from torchvision import models


def build_model() -> nn.Module:
    model = models.mobilenet_v2(weights=models.MobileNet_V2_Weights.IMAGENET1K_V1)
    # Cabeza con una capa oculta para mayor capacidad en la regresion de edad
    model.classifier = nn.Sequential(
        nn.Dropout(0.3),
        nn.Linear(model.last_channel, 256),
        nn.ReLU(),
        nn.Dropout(0.2),
        nn.Linear(256, 1),
    )
    return model


def predict_age(model: nn.Module, tensor: torch.Tensor) -> tuple[int, float]:
    """Returns (estimated_age, confidence). Confidence is higher the further from 18."""
    model.eval()
    with torch.no_grad():
        raw = model(tensor).squeeze().item()
    age = int(max(0, min(120, round(raw))))
    confidence = round(1.0 / (1.0 + math.exp(-abs(age - 18) / 4.0)), 4)
    return age, confidence
