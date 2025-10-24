"""Kiro Insbridge packages module.

This module provides utilities for managing package versions and paths.
"""

from pathlib import Path

THIS_DIR = Path(__file__).parent
PROJECT_DIR = (THIS_DIR / "../..").resolve()