import sys
import os
from unittest.mock import MagicMock

for _mod in ["kafka", "minio"]:
    sys.modules.setdefault(_mod, MagicMock())

SERVICE_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, SERVICE_DIR)

if "YOLO_MODEL_PATH" not in os.environ:
    os.environ["YOLO_MODEL_PATH"] = os.path.join(SERVICE_DIR, "yolov8n-face.pt")
