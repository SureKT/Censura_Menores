"""
Test de integracion: face detection con imagen real y modelo YOLO real.

Coloca tu propia foto en tests/assets/test_face.jpg para usarla.
Si no existe, se descarga una imagen de muestra automaticamente.

Ejecutar:
    pytest tests/test_integration.py -v -s
"""
import os
import urllib.request

import cv2
import pytest


ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
LOCAL_IMAGE = os.path.join(ASSETS_DIR, "test_face.jpg")
FALLBACK_IMAGE_URL = (
    "https://raw.githubusercontent.com/opencv/opencv/master/samples/data/group.jpg"
)


@pytest.fixture(scope="session")
def test_image():
    if os.path.exists(LOCAL_IMAGE):
        img = cv2.imread(LOCAL_IMAGE)
        if img is not None:
            return img

    print(f"\n[integration] Descargando imagen de prueba...")
    try:
        urllib.request.urlretrieve(FALLBACK_IMAGE_URL, LOCAL_IMAGE)
    except Exception as exc:
        pytest.skip(f"No se pudo obtener imagen de prueba: {exc}")

    img = cv2.imread(LOCAL_IMAGE)
    if img is None:
        pytest.skip("La imagen descargada no se pudo decodificar.")
    return img


@pytest.fixture(scope="session")
def yolo_model():
    import main
    try:
        return main.load_model()
    except RuntimeError as exc:
        pytest.skip(str(exc))


def test_deteccion_con_foto_real(test_image, yolo_model):
    import main
    h, w = test_image.shape[:2]
    faces = main.detect_faces(yolo_model, test_image)

    print(f"\n{'='*50}")
    print(f"Imagen: {w}x{h} px")
    print(f"Rostros detectados: {len(faces)}")
    for i, face in enumerate(faces, start=1):
        print(f"  Cara {i}: x={face['x']} y={face['y']} w={face['width']} h={face['height']}")
    print("="*50)

    assert len(faces) >= 1, f"Se esperaba al menos 1 rostro, se detectaron {len(faces)}"
