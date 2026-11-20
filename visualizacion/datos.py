"""Lectura de la zona de servicio desde PostgreSQL (usuario `lector`) o respaldo CSV."""
from __future__ import annotations

import csv
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List

from visualizacion import config

TABLAS_OBLIGATORIAS = [
    "indicador_evolucion_semestral",
    "indicador_departamentos",
    "indicador_sector_metodologia",
    "indicador_areas_genero",
    "indicador_top_ies",
    "metadatos_pipeline",
]
TABLAS_OPCIONALES = ["stream_ventanas"]


def _normalizar_valor(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def cargar_postgres() -> Dict[str, List[dict]]:
    import pg8000.native

    con = pg8000.native.Connection(
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        database=config.POSTGRES_DB,
    )
    resultado: Dict[str, List[dict]] = {}
    try:
        for tabla in TABLAS_OBLIGATORIAS + TABLAS_OPCIONALES:
            try:
                filas = con.run(f"SELECT * FROM servicio.{tabla}")
                cols = [c["name"] for c in con.columns]
                resultado[tabla] = [
                    {col: _normalizar_valor(val) for col, val in zip(cols, fila)}
                    for fila in filas
                ]
            except Exception:
                if tabla in TABLAS_OBLIGATORIAS:
                    raise
                resultado[tabla] = []
    finally:
        con.close()
    return resultado


def _convertir_csv(val: str):
    if val == "":
        return None
    if val in ("True", "true"):
        return True
    if val in ("False", "false"):
        return False
    try:
        if "." in val:
            return float(val)
        return int(val)
    except ValueError:
        return val


def cargar_csv(carpeta: str) -> Dict[str, List[dict]]:
    base = Path(carpeta)
    resultado: Dict[str, List[dict]] = {}
    for tabla in TABLAS_OBLIGATORIAS + TABLAS_OPCIONALES:
        ruta = base / f"{tabla}.csv"
        if not ruta.exists():
            if tabla in TABLAS_OBLIGATORIAS:
                raise FileNotFoundError(f"Falta tabla de servicio CSV: {ruta}")
            resultado[tabla] = []
            continue
        with ruta.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            resultado[tabla] = [
                {k: _convertir_csv(v) for k, v in fila.items()} for fila in reader
            ]
    return resultado


def cargar(origen: str = "postgres") -> Dict[str, List[dict]]:
    if origen == "postgres":
        return cargar_postgres()
    return cargar_csv(origen)


def cargar_reporte_calidad() -> dict:
    ruta = config.PROYECTO / "evidencias" / "reporte_calidad.json"
    if ruta.exists():
        return json.loads(ruta.read_text(encoding="utf-8"))
    return {}

