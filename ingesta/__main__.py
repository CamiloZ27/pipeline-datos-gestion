"""
CLI de la Fase 2 (Ingesta a zona raw).

Ejemplos:
    python3 -m ingesta descargar                           # historia completa (2015-2021)
    python3 -m ingesta descargar --hasta-anio 2020         # carga inicial hasta 2020
    python3 -m ingesta descargar --incremental             # trae solo lo posterior a la marca de agua
    python3 -m ingesta marca-agua                          # muestra la marca de agua actual
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from ingesta import comun, matricula


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="ingesta",
        description="Fase 2: Ingesta API datos.gov.co (5wck-szir) -> zona raw JSONL",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="comando", required=True)

    p_desc = sub.add_parser("descargar", help="Descarga registros desde Socrata SODA2")
    p_desc.add_argument("--desde-anio", type=int, default=None)
    p_desc.add_argument("--desde-semestre", type=int, choices=[1, 2], default=None)
    p_desc.add_argument("--hasta-anio", type=int, default=None)
    p_desc.add_argument("--hasta-semestre", type=int, choices=[1, 2], default=None)
    p_desc.add_argument(
        "--incremental",
        action="store_true",
        help="Usa la marca de agua guardada para traer solo periodos nuevos",
    )
    p_desc.add_argument("--tamano-pagina", type=int, default=None)
    p_desc.add_argument("--max-paginas", type=int, default=None)

    sub.add_parser("marca-agua", help="Muestra la marca de agua actual")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.comando == "marca-agua":
        marca = comun.leer_marca_agua()
        print(json.dumps(marca, ensure_ascii=False, indent=2) if marca else "Sin marca de agua.")
        return 0

    archivo = matricula.ejecutar(
        desde_anio=args.desde_anio,
        desde_semestre=args.desde_semestre,
        hasta_anio=args.hasta_anio,
        hasta_semestre=args.hasta_semestre,
        incremental=args.incremental,
        tamano_pagina=args.tamano_pagina,
        max_paginas=args.max_paginas,
    )
    if archivo:
        print(f"Archivo generado: {archivo}")
    else:
        print("0 registros nuevos (marca de agua actualizada).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

