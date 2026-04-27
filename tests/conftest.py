import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Solo se añade age_detection para que model.py (compartido con age_realtime)
# sea importable via `from model import ...` en test_age_model.py.
# El resto de servicios cargan su main.py via importlib para evitar colisiones.
sys.path.insert(0, str(ROOT / "services" / "age_detection"))
