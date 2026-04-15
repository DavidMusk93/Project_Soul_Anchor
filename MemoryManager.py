from pathlib import Path
import runpy
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from soul_anchor.manager import MemoryManager  # noqa: E402

__all__ = ["MemoryManager"]


if __name__ == "__main__":
    runpy.run_module("soul_anchor.manager", run_name="__main__")
