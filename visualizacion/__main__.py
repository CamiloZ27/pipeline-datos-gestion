"""
CLI de la Fase 8b (Tablero HTML interactivo con Plotly.js).

Uso:
    python3 -m visualizacion generar                    # lee PostgreSQL (usuario lector)
    python3 -m visualizacion generar --publicar         # también copia a docs/index.html
    python3 -m visualizacion generar --origen datos_muestra/zona_servicio --publicar
"""
from __future__ import annotations

import argparse
import sys

from visualizacion.generar import generar


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="visualizacion", description="Fase 8b: Dashboard Plotly HTML")
    sub = parser.add_subparsers(dest="comando", required=True)

    p_gen = sub.add_parser("generar", help="Genera salida/dashboard.html desde la zona de servicio")
    p_gen.add_argument("--origen", default="postgres", help="'postgres' o ruta a carpeta de CSV")
    p_gen.add_argument("--publicar", action="store_true", help="Copia el HTML a docs/index.html")

    args = parser.parse_args(argv)
    if args.comando == "generar":
        ruta = generar(origen=args.origen, publicar=args.publicar)
        print(f"Dashboard generado correctamente: {ruta}")
        if args.publicar:
            print("Publicado para GitHub Pages en: docs/index.html")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

