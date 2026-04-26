"""Tests de la logica pura del servicio age_realtime.

Se carga el fichero por ruta para evitar colision con otros `main.py` del proyecto.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AR_DIR = ROOT / "services" / "age_realtime"

# Hacemos visible model.py para el import que hace age_realtime/main.py
if str(AR_DIR) not in sys.path:
    sys.path.insert(0, str(AR_DIR))

spec = importlib.util.spec_from_file_location("age_realtime_main", AR_DIR / "main.py")
rt = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(rt)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"No se pudo cargar age_realtime/main.py (posible falta de deps): {exc}", allow_module_level=True)


def test_build_output_event_estructura():
    cmd = {
        "event": {"event_id": "e1"},
        "payload": {"session_id": "s1", "face_token": "t1", "image_b64": "xx"},
    }
    out = rt.build_output_event(cmd, age=12, is_minor=True, confidence=0.87)
    assert out["event"]["event_type"] == "evt.realtime.classification.completed"
    assert out["event"]["source"] == "age-realtime"
    assert out["payload"]["session_id"] == "s1"
    assert out["payload"]["face_token"] == "t1"
    assert out["payload"]["estimated_age"] == 12
    assert out["payload"]["is_minor"] is True
    assert out["payload"]["confidence"] == 0.87


def test_build_output_event_event_id_unico():
    cmd = {"event": {}, "payload": {"session_id": "s", "face_token": "t"}}
    a = rt.build_output_event(cmd, 20, False, 0.9)
    b = rt.build_output_event(cmd, 20, False, 0.9)
    assert a["event"]["event_id"] != b["event"]["event_id"]


def test_classify_sin_modelo_usa_fallback():
    """Sin modelo, la clasificacion debe devolver (30, False, 0.0)."""
    import base64
    import io as _io
    from PIL import Image

    img = Image.new("RGB", (64, 64), color=(128, 128, 128))
    buf = _io.BytesIO()
    img.save(buf, format="JPEG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")

    age, is_minor, conf = rt.classify(b64, model=None)
    assert age == 30
    assert is_minor is False
    assert conf == 0.0
