"""
CLI de la Fase 4a (Data Lake HDFS).

Comandos:
    python3 -m datalake consumir
    python3 -m datalake ls /datalake/raw
    python3 -m datalake resumen [/datalake/raw]
    python3 -m datalake cat <ruta_hdfs> --lineas 3
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from datalake import config, consumidor_hdfs
from datalake.webhdfs import WebHDFS


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="datalake", description="Fase 4a: Data Lake HDFS")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="comando", required=True)

    p_cons = sub.add_parser("consumir", help="Consume Kafka hacia HDFS en lotes")
    p_cons.add_argument("--continuo", action="store_true")

    p_ls = sub.add_parser("ls", help="Lista una ruta en HDFS")
    p_ls.add_argument("ruta", nargs="?", default=config.RAIZ_RAW)

    p_res = sub.add_parser("resumen", help="Resumen de archivos y bytes en HDFS")
    p_res.add_argument("ruta", nargs="?", default=config.RAIZ_RAW)

    p_cat = sub.add_parser("cat", help="Muestra primeras líneas de un archivo HDFS")
    p_cat.add_argument("ruta")
    p_cat.add_argument("--lineas", type=int, default=3)

    p_sub = sub.add_parser("subir", help="Sube un archivo local a HDFS")
    p_sub.add_argument("local")
    p_sub.add_argument("destino")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.comando == "consumir":
        res = consumidor_hdfs.ejecutar(continuo=args.continuo)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0

    hdfs = WebHDFS()
    if args.comando == "ls":
        for item in hdfs.listar(args.ruta):
            tipo = "DIR " if item["type"] == "DIRECTORY" else "FILE"
            print(f"{tipo}  {item['length']:>12,} B  {args.ruta.rstrip('/')}/{item['pathSuffix']}")
        return 0
    if args.comando == "resumen":
        info = hdfs.resumen(args.ruta)
        mb = info["length"] / (1024 * 1024)
        print(
            f"Ruta: {args.ruta}\n"
            f"  Carpetas: {info['directoryCount']}\n"
            f"  Archivos: {info['fileCount']}\n"
            f"  Tamaño:   {info['length']:,} bytes ({mb:.2f} MB)"
        )
        return 0
    if args.comando == "cat":
        contenido = hdfs.leer(args.ruta).decode("utf-8", errors="replace").splitlines()
        for linea in contenido[: args.lineas]:
            print(linea)
        return 0
    if args.comando == "subir":
        n = hdfs.subir_archivo(args.local, args.destino)
        print(f"Subido {args.local} -> {args.destino} ({n:,} bytes)")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

