import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Exponer los módulos de cada servicio sin necesidad de instalarlos como paquete
sys.path.insert(0, str(ROOT / "services" / "age_detection"))
sys.path.insert(0, str(ROOT / "services" / "pixelation"))
sys.path.insert(0, str(ROOT / "services" / "face_detection"))
sys.path.insert(0, str(ROOT / "api" / "api1"))
# Nota: los modulos `main` de api2 y age_realtime NO se exponen aqui para evitar
# colision con api1/main. Los tests de esos servicios los cargan via importlib.
