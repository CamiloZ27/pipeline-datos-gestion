"""
Utilidades compartidas de ingesta (Fase 2 + Marca de agua de Fase 6):
  - Sesión HTTP con reintentos exponenciales ante 429 / 5xx / errores de red.
  - Construcción del «sobre» JSON estándar (payload intacto).
  - Escritura particionada en JSON Lines (fecha_ingesta=YYYY-MM-DD) + manifiesto.
  - Lectura y actualización de la marca de agua para cargas incrementales.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingesta import config

log = logging.getLogger("ingesta")


def crear_sesion() -> requests.Session:
    """Sesión HTTP con espera exponencial (1s, 2s, 4s, 8s...) ante fallos 429/5xx."""
    reintentos = Retry(
        total=config.HTTP_REINTENTOS,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    sesion = requests.Session()
    adaptador = HTTPAdapter(max_retries=reintentos)
    sesion.mount("https://", adaptador)
    sesion.mount("http://", adaptador)
    sesion.headers.update({"User-Agent": config.USER_AGENT})
    return sesion


def ahora_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def construir_sobre(
    fuente: str,
    dataset: str,
    id_registro: str,
    payload: dict,
    fecha_ingesta: str,
) -> dict:
    """Envuelve el registro crudo SIN modificarlo (principio de zona raw inmutable).

    - id_registro: llave natural del dato, usada como key en Kafka y deduplicación.
    - payload: diccionario exacto entregado por la API Socrata.
    """
    return {
        "fuente": fuente,
        "dataset": dataset,
        "id_registro": id_registro,
        "fecha_ingesta": fecha_ingesta,
        "payload": payload,
    }


def ruta_particion(
    fuente: str,
    dataset: str,
    fecha_ingesta: str,
    raw_dir: Optional[Path] = None,
) -> Path:
    """data/raw/<fuente>/<dataset>/fecha_ingesta=YYYY-MM-DD/"""
    base = raw_dir or config.RAW_DIR
    dia = fecha_ingesta[:10]
    return base / fuente / dataset / f"fecha_ingesta={dia}"


def guardar_jsonl(
    registros: List[dict],
    fuente: str,
    dataset: str,
    fecha_ingesta: str,
    url: str,
    parametros: dict,
    raw_dir: Optional[Path] = None,
) -> Path:
    """Guarda los sobres en formato JSON Lines y genera su manifiesto de auditoría."""
    carpeta = ruta_particion(fuente, dataset, fecha_ingesta, raw_dir=raw_dir)
    carpeta.mkdir(parents=True, exist_ok=True)

    marca = fecha_ingesta.replace(":", "").replace("-", "")
    archivo = carpeta / f"{dataset}_{marca}.jsonl"

    with archivo.open("w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    manifiesto = {
        "fuente": fuente,
        "dataset": dataset,
        "fecha_ingesta": fecha_ingesta,
        "url": url,
        "parametros": parametros,
        "registros": len(registros),
        "archivo": archivo.name,
        "bytes": archivo.stat().st_size,
    }
    ruta_manifiesto = carpeta / f"_manifiesto_{marca}.json"
    ruta_manifiesto.write_text(
        json.dumps(manifiesto, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("%s/%s: %d registros -> %s", fuente, dataset, len(registros), archivo)
    return archivo


# ------------------------------------------------------------------ Marca de agua
def leer_marca_agua(ruta: Optional[Path] = None) -> Optional[Dict]:
    """Lee la última fecha/periodo cargado exitosamente (carga incremental)."""
    archivo = ruta or config.WATERMARK_FILE
    if not archivo.exists():
        return None
    return json.loads(archivo.read_text(encoding="utf-8"))


def guardar_marca_agua(
    ultimo_anio: int,
    ultimo_semestre: int,
    fecha_periodo: str,
    fecha_ingesta: str,
    registros_ejecucion: int,
    ruta: Optional[Path] = None,
) -> Path:
    """Persiste la marca de agua tras una ingesta exitosa."""
    archivo = ruta or config.WATERMARK_FILE
    archivo.parent.mkdir(parents=True, exist_ok=True)
    estado = {
        "fuente": config.FUENTE,
        "dataset": config.DATASET,
        "ultimo_anio": ultimo_anio,
        "ultimo_semestre": ultimo_semestre,
        "ultima_fecha_periodo": fecha_periodo,
        "ultima_ingesta_utc": fecha_ingesta,
        "registros_ultima_ejecucion": registros_ejecucion,
    }
    archivo.write_text(json.dumps(estado, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(
        "Marca de agua actualizada -> periodo %d-%d (%s)",
        ultimo_anio,
        ultimo_semestre,
        fecha_periodo,
    )
    return archivo

