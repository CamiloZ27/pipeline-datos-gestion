"""
CLI de la Fase 3 (Apache Kafka).

Comandos:
    python3 -m streaming crear-topicos
    python3 -m streaming producir [--reenviar]
    python3 -m streaming consumir [--topico raw.datos_gov_co] [--grupo demo-grupo] [--max 5]
    python3 -m streaming info-topico
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from streaming import consumidor, productor, topicos


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="streaming", description="Fase 3: Apache Kafka")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="comando", required=True)

    sub.add_parser("crear-topicos", help="Crea el tópico con 3 particiones")

    p_prod = sub.add_parser("producir", help="Publica los .jsonl pendientes en Kafka")
    p_prod.add_argument("--reenviar", action="store_true", help="Ignora checkpoint .kafka_ok")

    p_cons = sub.add_parser("consumir", help="Consume mensajes con un consumer group")
    p_cons.add_argument("--topico", default="raw.datos_gov_co")
    p_cons.add_argument("--grupo", default="verificacion-cli")
    p_cons.add_argument("--max", type=int, default=5)
    p_cons.add_argument("--sin-grupo", action="store_true")

    sub.add_parser("info-topico", help="Muestra particiones y offsets de cada tópico")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.comando == "crear-topicos":
        res = topicos.crear_topicos()
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0
    if args.comando == "producir":
        res = productor.ejecutar(reenviar=args.reenviar)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0
    if args.comando == "consumir":
        grupo = None if args.sin_grupo else args.grupo
        consumidor.ejecutar(topico=args.topico, maximo=args.max, grupo=grupo)
        return 0
    if args.comando == "info-topico":
        print(json.dumps(topicos.describir_topicos(), ensure_ascii=False, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

