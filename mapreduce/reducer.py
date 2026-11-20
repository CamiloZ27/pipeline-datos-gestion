#!/usr/bin/env python3
"""
Fase 4b — Reducer en Python (estilo Hadoop Streaming).

Recibe por stdin las líneas ya ordenadas por clave (`sort`) emitidas por mapper.py:
    <anio>|<id_sector>\t<matriculados>\t1

Agrega por clave sin cargar el dataset entero en memoria (streaming O(1) en RAM)
y emite:
    <anio>|<id_sector>\t<total_registros_raw>\t<total_matriculados_raw>
"""
from __future__ import annotations

import sys
from typing import Dict, Iterable, Iterator, Tuple


def reduce_lineas(lineas: Iterable[str]) -> Iterator[Tuple[str, int, int]]:
    """Agrega registros y matriculados por clave consecutiva (entrada ordenada)."""
    clave_actual = None
    suma_mat = 0
    suma_reg = 0

    for linea in lineas:
        linea = linea.strip()
        if not linea:
            continue
        partes = linea.split("\t")
        if len(partes) != 3:
            continue
        clave, mat_s, reg_s = partes
        mat = int(mat_s)
        reg = int(reg_s)

        if clave == clave_actual:
            suma_mat += mat
            suma_reg += reg
        else:
            if clave_actual is not None:
                yield clave_actual, suma_reg, suma_mat
            clave_actual = clave
            suma_mat = mat
            suma_reg = reg

    if clave_actual is not None:
        yield clave_actual, suma_reg, suma_mat


def ejecutar_en_memoria(lineas_raw: Iterable[str]) -> Dict[str, Tuple[int, int]]:
    """Ejecuta Map -> Shuffle/Sort -> Reduce usando mapper.py y reducer.py."""
    from mapreduce.mapper import map_lineas

    intermedias = sorted(map_lineas(lineas_raw))
    return {clave: (reg, mat) for clave, reg, mat in reduce_lineas(intermedias)}


def main() -> int:
    for clave, reg, mat in reduce_lineas(sys.stdin):
        sys.stdout.write(f"{clave}\t{reg}\t{mat}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

