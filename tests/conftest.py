import sys
from pathlib import Path

file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
print(f"Parent directory: {parent}, Root directory: {root}")
sys.path.append(str(root / "bus_server"))
