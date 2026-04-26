"""Tests del dispatcher interno de SSE en API2.

Se carga el fichero por ruta para no colisionar con api1/main.py. Al importar,
API2 arranca un hilo daemon que intenta conectar a Kafka; es daemon y se reintenta
en background, no afecta a la ejecucion de los tests.
"""
import importlib.util
import queue
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
API2_DIR = ROOT / "api" / "api2"

if str(API2_DIR) not in sys.path:
    sys.path.insert(0, str(API2_DIR))

spec = importlib.util.spec_from_file_location("api2_main", API2_DIR / "main.py")
main = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(main)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"No se pudo cargar api2/main.py: {exc}", allow_module_level=True)


def _reset_sessions():
    with main._rt_lock:
        main._rt_sessions.clear()


def test_dispatch_enruta_por_session_id():
    _reset_sessions()
    q_a = main._rt_get_queue("sess-A")
    q_b = main._rt_get_queue("sess-B")

    main._rt_dispatch({"session_id": "sess-A", "face_token": "t1", "is_minor": True})
    main._rt_dispatch({"session_id": "sess-B", "face_token": "t2", "is_minor": False})

    ev_a = q_a.get(timeout=1)
    ev_b = q_b.get(timeout=1)
    assert ev_a["face_token"] == "t1"
    assert ev_b["face_token"] == "t2"

    with pytest.raises(queue.Empty):
        q_a.get_nowait()
    with pytest.raises(queue.Empty):
        q_b.get_nowait()


def test_dispatch_sin_session_id_no_explota():
    _reset_sessions()
    main._rt_dispatch({"face_token": "x"})


def test_dispatch_a_session_inexistente_no_explota():
    _reset_sessions()
    main._rt_dispatch({"session_id": "fantasma", "face_token": "x"})


def test_drop_queue_elimina_sesion():
    _reset_sessions()
    main._rt_get_queue("sess-X")
    assert "sess-X" in main._rt_sessions
    main._rt_drop_queue("sess-X")
    assert "sess-X" not in main._rt_sessions
