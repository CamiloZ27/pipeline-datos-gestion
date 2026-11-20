"""
Configuración de la Fase 2 (Ingesta a la zona raw) y marca de agua incremental.

Fuente oficial:
  Ministerio de Educación Nacional (SNIES) — MEN_MATRICULA_ESTADISTICA_ES
  Portal:  https://www.datos.gov.co/d/5wck-szir
  API:     Socrata SODA2 (abierta, sin autenticación obligatoria)
"""
from __future__ import annotations

import os
from pathlib import Path

# Zona raw (aterrizaje local): los registros llegan intactos sin limpiar.
RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))

# Archivo de marca de agua (watermark de carga incremental - Fase 6)
WATERMARK_FILE = Path(os.getenv("WATERMARK_FILE", "data/raw/_marca_agua.json"))

# Fuente Socrata SODA2 — MEN Matrícula Educación Superior Colombia
FUENTE = "datos_gov_co"
DATASET = "men_matricula_es"
SOCRATA_DATASET_ID = "5wck-szir"
SOCRATA_URL = f"https://www.datos.gov.co/resource/{SOCRATA_DATASET_ID}.json"

# Token opcional (gratis en datos.gov.co -> Configuración de desarrollador).
# Sin token la API SODA2 funciona normalmente con límites estándar.
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

# SODA 2.1 permite páginas de hasta 50.000 filas; 390.903 filas = solo 8 peticiones.
TAMANO_PAGINA = int(os.getenv("SOCRATA_PAGE_SIZE", "50000"))

# Parámetros HTTP y reintentos exponenciales ante 429 / 5xx
HTTP_TIMEOUT = 60
HTTP_REINTENTOS = 5
USER_AGENT = "pipeline-matricula-es-pascual-bravo/1.0"

