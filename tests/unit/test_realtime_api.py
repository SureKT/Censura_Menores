"""Tests de la logica pura de la variante de tiempo real en API1.

Se carga el fichero por ruta para evitar colision con otros main.py del proyecto.
No toca Kafka ni la red; solo valida el builder del comando.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
API1_DIR = ROOT / "api" / "api1"

if str(API1_DIR) not in sys.path:
    sys.path.insert(0, str(API1_DIR))

spec = importlib.util.spec_from_file_location("api1_main", API1_DIR / "main.py")
api1 = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(api1)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"No se pudo cargar api1/main.py (posible falta de deps): {exc}", allow_module_level=True)

REALTIME_TOPIC = api1.REALTIME_TOPIC
build_realtime_command = api1.build_realtime_command


def test_build_realtime_command_estructura():
    cmd = build_realtime_command(
        session_id="sess-abc",
        face_token="tok-001",
        image_b64="aGVsbG8=",
    )
    assert cmd["event"]["event_type"] == REALTIME_TOPIC
    assert cmd["event"]["source"] == "api-ingesta"
    assert cmd["payload"]["session_id"] == "sess-abc"
    assert cmd["payload"]["face_token"] == "tok-001"
    assert cmd["payload"]["image_b64"] == "aGVsbG8="


def test_build_realtime_command_event_id_unico():
    c1 = build_realtime_command("s", "t", "x")
    c2 = build_realtime_command("s", "t", "x")
    assert c1["event"]["event_id"] != c2["event"]["event_id"]


def test_build_realtime_command_sin_trace():
    """El flujo realtime NO lleva request_id/image_id; el route por session_id basta."""
    cmd = build_realtime_command("s", "t", "x")
    assert "trace" not in cmd["event"]
