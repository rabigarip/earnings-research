"""Loads TOML config and resolves project paths."""

from __future__ import annotations
import os
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_cfg: dict | None = None

def cfg() -> dict:
    global _cfg
    if _cfg is None:
        with open(ROOT / "config" / "settings.toml", "rb") as f:
            _cfg = tomllib.load(f)
    return _cfg

def root() -> Path:
    return ROOT
