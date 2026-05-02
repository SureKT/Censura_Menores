"""Tests unitarios del servicio de pixelado."""
import importlib.util
import sys
from pathlib import Path

import pytest
from PIL import Image

ROOT = Path(__file__).resolve().parents[2]
PIX_DIR = ROOT / "services" / "pixelation"

if str(PIX_DIR) not in sys.path:
    sys.path.insert(0, str(PIX_DIR))

spec = importlib.util.spec_from_file_location("pixelation_main", PIX_DIR / "main.py")
pix = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(pix)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"No se pudo cargar pixelation/main.py: {exc}", allow_module_level=True)

pixelate_region = pix.pixelate_region
build_output_event = pix.build_output_event


def make_image(w=200, h=200, color=(100, 150, 200)):
    return Image.new("RGB", (w, h), color=color)


def make_gradient_image(w=200, h=200):
    """Imagen con gradiente de colores — no uniforme, segura para probar pixelación."""
    img = Image.new("RGB", (w, h))
    pixels = img.load()
    for x in range(w):
        for y in range(h):
            pixels[x, y] = (x % 256, y % 256, (x + y) % 256)
    return img


def test_pixelate_region_modifica_imagen():
    """La región pixelada debe quedar visualmente bloqueada (píxeles uniformes por bloque)."""
    img = make_gradient_image()
    pixelate_region(img, 50, 50, 80, 80)

    # Todos los píxeles del primer bloque (12x12) deben ser idénticos
    # porque el algoritmo downscale→upscale NEAREST uniformiza el bloque.
    ref = img.getpixel((50, 50))
    block_uniform = all(
        img.getpixel((50 + dx, 50 + dy)) == ref
        for dx in range(12)
        for dy in range(12)
    )
    assert block_uniform, "El primer bloque de pixelación no es uniforme"

    # La región debe ser diferente a la imagen sin pixelar en al menos un píxel
    original = make_gradient_image()
    changed = any(
        img.getpixel((x, y)) != original.getpixel((x, y))
        for x in range(50, 130)
        for y in range(50, 130)
    )
    assert changed, "La pixelación no modificó ningún píxel de la región"


def test_pixelate_region_fuera_de_limites_no_falla():
    """No debe lanzar excepción si el bounding box sale de los límites."""
    img = make_image(100, 100)
    pixelate_region(img, -20, -20, 300, 300)


def test_pixelate_region_dimension_cero_no_falla():
    """Dimensiones cero o negativas no deben lanzar excepción."""
    img = make_image()
    pixelate_region(img, 10, 10, 0, 0)
    pixelate_region(img, 10, 10, -5, -5)


def test_build_output_event_estructura():
    """El evento de salida debe tener la estructura correcta."""
    cmd = {
        "event": {
            "event_type": "cmd.pixelation",
            "event_version": "v1",
            "occurred_at": "2026-01-01T00:00:00Z",
            "trace": {"request_id": "req-123", "image_id": "img-456"},
            "source": "test",
        },
        "payload": {
            "bucket": "imagenes-raw",
            "object_key": "raw/foto.jpg",
            "faces": [
                {"x": 10, "y": 10, "width": 50, "height": 50, "is_minor": True},
                {"x": 100, "y": 100, "width": 50, "height": 50, "is_minor": False},
            ],
            "total_faces": 2,
            "minors_count": 1,
        },
    }
    event = build_output_event(cmd)
    assert event["event"]["event_type"] == "evt.pixelation.completed"
    assert event["event"]["trace"]["request_id"] == "req-123"
    assert "payload" in event
