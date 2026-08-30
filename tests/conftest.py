import sys
from pathlib import Path

# The generator modules use flat imports (from trade_scenarios import ...),
# so data_generator/ has to be importable directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_generator"))
