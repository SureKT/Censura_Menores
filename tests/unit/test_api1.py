"""Tests unitarios de la lógica pura de API1."""
from main import build_face_detection_command


def test_build_face_detection_command_estructura():
    """El comando debe tener event + payload con los campos correctos."""
    cmd = build_face_detection_command(
        request_id="req-001",
        image_id="img-001",
        bucket="imagenes-raw",
        object_key="raw/foto.jpg",
        content_type="image/jpeg",
        size_bytes=204800,
    )
    assert cmd["event"]["event_type"] == "cmd.face_detection"
    assert cmd["event"]["trace"]["request_id"] == "req-001"
    assert cmd["event"]["trace"]["image_id"] == "img-001"
    assert cmd["payload"]["bucket"] == "imagenes-raw"
    assert cmd["payload"]["object_key"] == "raw/foto.jpg"
    assert cmd["payload"]["content_type"] == "image/jpeg"
    assert cmd["payload"]["size_bytes"] == 204800


def test_build_face_detection_command_event_id_unico():
    """Cada llamada debe generar un event_id distinto."""
    cmd1 = build_face_detection_command("r1", "i1", "b", "k", "image/png", 100)
    cmd2 = build_face_detection_command("r1", "i1", "b", "k", "image/png", 100)
    assert cmd1["event"]["event_id"] != cmd2["event"]["event_id"]


def test_build_face_detection_command_source():
    """El source debe identificar el servicio."""
    cmd = build_face_detection_command("r", "i", "b", "k", "image/jpeg", 1)
    assert cmd["event"]["source"] == "api-ingesta"
