#!/usr/bin/env python3
"""
Fase 4b — Mapper en Python (estilo Hadoop Streaming).

Lee líneas JSONL de la zona raw por stdin y emite, por cada registro:
    <anio>|<id_sector>\t<matriculados>\t1

Uso directo (pipeline Unix / Hadoop Streaming):
    cat data/raw/datos_gov_co/men_matricula_es/fecha_ingesta=*/*.jsonl \\
        | python3 mapreduce/mapper.py \\
        | sort \\
        | python3 mapreduce/reducer.py
"""
from __future__ import annotations

import json
import sys
from typing import Iterable, Iterator


def map_lineas(lineas: Iterable[str]) -> Iterator[str]:
    """Emite pares clave-valor `anio|id_sector\tmatriculados\t1` por cada sobre raw."""
    for linea in lineas:
        linea = linea.strip()
        if not linea:
            continue
        try:
            sobre = json.loads(linea)
        except json.JSONDecodeError:
            continue
        payload = sobre.get("payload") or {}
        anio = str(payload.get("a_o") or "DESCONOCIDO").strip()
        sector = str(payload.get("id_sector") or "DESCONOCIDO").strip()
        raw_mat = str(payload.get("matriculados_2015") or "0").strip()
        try:
            matriculados = int(float(raw_mat))
        except ValueError:
            matriculados = 0
        yield f"{anio}|{sector}\t{matriculados}\t1\n"


def main() -> int:
    for salida in map_lineas(sys.stdin):
        sys.stdout.write(salida)
    return 0


if __name__ == "__main__":
    sys.exit(main())

