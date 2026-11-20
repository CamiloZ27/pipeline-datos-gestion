"""
CLI local para ejecutar MapReduce en Python (sin necesidad de Spark).

Uso:
    python3 -m mapreduce ejecutar --origen data/raw
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from mapreduce.reducer import ejecutar_en_memoria


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="mapreduce", description="Fase 4b: MapReduce Python")
    sub = parser.add_subparsers(dest="comando", required=True)
    p_ej = sub.add_parser("ejecutar", help="Ejecuta mapper + sort + reducer sobre archivos raw")
    p_ej.add_argument("--origen", default="data/raw")
    args = parser.parse_args(argv)

    archivos = sorted(Path(args.origen).glob("*/*/fecha_ingesta=*/*.jsonl"))
    if not archivos:
        print(f"No se encontraron archivos .jsonl en {args.origen}")
        return 1

    def leer_todo():
        for a in archivos:
            with a.open(encoding="utf-8") as f:
                yield from f

    res = ejecutar_en_memoria(leer_todo())
    tot_r = sum(v[0] for v in res.values())
    tot_m = sum(v[1] for v in res.values())
    print(f"{'Clave (anio|sector)':<20} {'Registros':>12} {'Matriculados':>14}")
    print("-" * 48)
    for k, (r, m) in sorted(res.items()):
        print(f"{k:<20} {r:>12,} {m:>14,}")
    print("-" * 48)
    print(f"{'TOTAL':<20} {tot_r:>12,} {tot_m:>14,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

