"""
Configuración de pytest para el proyecto ETL.
"""
import pytest
import sys
from pathlib import Path

# Asegurar que src está en el path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
