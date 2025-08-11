from __future__ import annotations
import os, json, glob
from pathlib import Path
from typing import Dict, Any, List

def ensure_dirs(*paths: str | Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def load_json(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def expand_globs(root: str | Path, pattern: str) -> List[Path]:
    root = Path(root)
    return sorted([Path(p) for p in root.glob(pattern)])
