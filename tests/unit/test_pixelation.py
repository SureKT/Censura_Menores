"""Tests unitarios del servicio de pixelado."""
import pytest
from PIL import Image

from main import pixelate_region, build_output_event


def make_image(w=200, h=200, color=(100, 150, 200)):
    return Image.new("RGB", (w, h), color=color)


def test_pixelate_region_modifica_imagen():
    """La región pixelada debe ser distinta a la original."""
    img = make_image()
    original = img.copy()
    pixelate_region(img, 50, 50, 80, 80)
    # Al menos algún píxel de la región debe haber cambiado
    changed = any(
        img.getpixel((x, y)) != original.getpixel((x, y))
        for x in range(50, 130)
        for y in range(50, 130)
    )
    assert changed


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
