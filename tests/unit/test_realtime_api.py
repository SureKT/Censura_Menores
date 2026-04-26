"""Tests de la logica pura de la variante de tiempo real en API1.

No toca Kafka ni la red; solo valida el builder del comando.
"""
from main import REALTIME_TOPIC, build_realtime_command


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
