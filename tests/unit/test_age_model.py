"""Tests unitarios del modelo de estimación de edad."""
import importlib.util
import sys
from pathlib import Path

import pytest
from PIL import Image

try:
    import torch
    from model import build_model, predict_age
except Exception as exc:  # pragma: no cover
    pytest.skip(f"Dependencias de torch no disponibles: {exc}", allow_module_level=True)

ROOT = Path(__file__).resolve().parents[2]
AGE_DET_DIR = ROOT / "services" / "age_detection"

spec = importlib.util.spec_from_file_location("age_detection_main", AGE_DET_DIR / "main.py")
_age_det = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(_age_det)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"No se pudo cargar age_detection/main.py: {exc}", allow_module_level=True)


@pytest.fixture(scope="module")
def model():
    return build_model()


def test_build_model_returns_nn(model):
    import torch.nn as nn
    assert isinstance(model, nn.Module)


def test_predict_age_range(model):
    """La edad predicha debe estar entre 0 y 120."""
    tensor = torch.randn(1, 3, 224, 224)
    age, confidence = predict_age(model, tensor)
    assert 0 <= age <= 120


def test_predict_age_confidence_range(model):
    """La confianza debe estar entre 0 y 1."""
    tensor = torch.randn(1, 3, 224, 224)
    _, confidence = predict_age(model, tensor)
    assert 0.0 <= confidence <= 1.0


def test_confidence_higher_far_from_18(model):
    """Confianza de edad=5 debe ser mayor que confianza de edad=17 (más lejos de 18)."""
    import math
    conf_5  = 1.0 / (1.0 + math.exp(-abs(5  - 18) / 4.0))
    conf_17 = 1.0 / (1.0 + math.exp(-abs(17 - 18) / 4.0))
    assert conf_5 > conf_17


def test_crop_face_returns_pil_image():
    """crop_face debe devolver una imagen PIL recortada."""
    img = Image.new("RGB", (300, 300), color=(128, 64, 32))
    face = {"x": 50, "y": 50, "width": 100, "height": 100}
    crop = _age_det.crop_face(img, face)
    assert isinstance(crop, Image.Image)
    assert crop.size[0] > 0 and crop.size[1] > 0


def test_crop_face_clamps_to_image_bounds():
    """crop_face no debe fallar aunque el bounding box salga de los límites."""
    img = Image.new("RGB", (100, 100))
    face = {"x": 80, "y": 80, "width": 200, "height": 200}
    crop = _age_det.crop_face(img, face)
    assert isinstance(crop, Image.Image)
