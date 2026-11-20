"""Configuración del módulo de visualización (Fase 8b)."""
from __future__ import annotations

import os
from pathlib import Path

BASE = Path(__file__).resolve().parent
PROYECTO = BASE.parent
PLANTILLAS = BASE / "plantillas"
VENDOR = BASE / "vendor"

SALIDA = Path(os.getenv("SALIDA_VISUALIZACION", str(PROYECTO / "salida")))
DOCS = PROYECTO / "docs"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "matricula_es")
# El dashboard se conecta con el usuario de SOLO LECTURA (`lector`)
POSTGRES_USER = os.getenv("POSTGRES_READ_USER", "lector")
POSTGRES_PASSWORD = os.getenv("POSTGRES_READ_PASSWORD", "lector")

AUTORES = "Camilo Zuleta Delgado · Santiago Restrepo Silva"
INSTITUCION = "Institución Universitaria Pascual Bravo · Gestión de Datos Masivos G004"
