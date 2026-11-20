"""
Productor Kafka (Fase 3): lee los JSONL de la zona raw y publica cada sobre
como un mensaje en Kafka con key = id_registro.

Control de duplicados si el productor se ejecuta dos veces:
  1. Nivel productor: tras confirmar con acks="all" + flush(), se escribe un
     archivo checkpoint `<archivo>.jsonl.kafka_ok`. Si se ejecuta de nuevo,
     omite los archivos ya enviados (0 mensajes duplicados) salvo `--reenviar`.
  2. Nivel partición: al usar key = id_registro, todos los mensajes del mismo
     registro caen siempre en la misma partición (hash(key) % 3) y en orden.
  3. Nivel consumidor / Spark: aun si se fuerza `--reenviar`, el consumidor HDFS
     usa offsets deterministas y PySpark deduplica por `id_registro` con Window.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator, List, Optional

from streaming import config

log = logging.getLogger("streaming.productor")


def crear_productor():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        client_id="matricula-productor",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        batch_size=128 * 1024,
        compression_type="gzip",
    )


def archivos_pendientes(
    fuente: Optional[str] = None,
    reenviar: bool = False,
    raw_dir: Optional[Path] = None,
) -> List[Path]:
    """Retorna los archivos .jsonl de data/raw que todavía no tienen .kafka_ok."""
    base = raw_dir or config.RAW_DIR
    fuentes = [fuente] if fuente else list(config.TOPICOS)
    archivos = []
    for f in fuentes:
        for archivo in sorted((base / f).glob("*/fecha_ingesta=*/*.jsonl")):
            marca = archivo.with_name(archivo.name + config.SUFIJO_ENVIADO)
            if reenviar or not marca.exists():
                archivos.append(archivo)
    return archivos


def leer_sobres(archivo: Path) -> Iterator[dict]:
    with archivo.open(encoding="utf-8") as f:
        for n, linea in enumerate(f, 1):
            linea = linea.strip()
            if not linea:
                continue
            try:
                yield json.loads(linea)
            except json.JSONDecodeError as e:
                log.warning("%s línea %d inválida, se omite: %s", archivo.name, n, e)


def enviar_archivo(productor, archivo: Path) -> int:
    """Publica cada línea del JSONL con key = id_registro y marca .kafka_ok."""
    errores = []

    def al_fallar(exc):
        errores.append(exc)

    enviados = 0
    for sobre in leer_sobres(archivo):
        topico = config.TOPICOS[sobre["fuente"]]
        productor.send(
            topico,
            key=sobre["id_registro"],
            value=sobre,
            headers=[
                ("dataset", sobre["dataset"].encode("utf-8")),
                ("fecha_ingesta", sobre["fecha_ingesta"].encode("utf-8")),
            ],
        ).add_errback(al_fallar)
        enviados += 1

    productor.flush()
    if errores:
        raise RuntimeError(
            f"{archivo.name}: {len(errores)} errores al producir (primero: {errores[0]})"
        )

    archivo.with_name(archivo.name + config.SUFIJO_ENVIADO).write_text(
        str(enviados),
        encoding="utf-8",
    )
    return enviados


def ejecutar(
    fuente: Optional[str] = None,
    reenviar: bool = False,
    productor=None,
    raw_dir: Optional[Path] = None,
) -> dict:
    archivos = archivos_pendientes(fuente=fuente, reenviar=reenviar, raw_dir=raw_dir)
    if not archivos:
        log.info("No hay archivos pendientes por enviar a Kafka (checkpoint .kafka_ok al día).")
        return {}

    propio = productor is None
    productor = productor or crear_productor()
    resumen = {}
    try:
        for archivo in archivos:
            n = enviar_archivo(productor, archivo)
            log.info("Enviado %s -> %d mensajes", archivo.name, n)
            resumen[str(archivo)] = n
    finally:
        if propio:
            productor.close()
    return resumen

