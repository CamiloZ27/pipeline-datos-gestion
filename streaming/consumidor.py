"""Consumidor de verificación con Consumer Group para inspección en terminal (Fase 3)."""
from __future__ import annotations

import json
import logging
from collections import Counter
from typing import Optional

from streaming import config

log = logging.getLogger("streaming.consumidor")


def crear_consumidor(topico: str, grupo: Optional[str], desde_inicio: bool):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        topico,
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        group_id=grupo,
        auto_offset_reset="earliest" if desde_inicio else "latest",
        enable_auto_commit=grupo is not None,
        consumer_timeout_ms=5000,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def ejecutar(
    topico: str = "raw.datos_gov_co",
    maximo: int = 5,
    desde_inicio: bool = True,
    grupo: Optional[str] = "verificacion-cli",
    consumidor=None,
) -> dict:
    """Lee mensajes con un consumer group, imprime una muestra y resume por partición."""
    propio = consumidor is None
    consumidor = consumidor or crear_consumidor(topico, grupo, desde_inicio)
    por_particion = Counter()
    por_anio = Counter()
    muestras = []

    try:
        for i, msg in enumerate(consumidor):
            por_particion[msg.partition] += 1
            anio = msg.value.get("payload", {}).get("a_o", "?")
            por_anio[anio] += 1
            if i < maximo:
                muestras.append(
                    {
                        "particion": msg.partition,
                        "offset": msg.offset,
                        "key": msg.key,
                        "ies": msg.value.get("payload", {}).get(
                            "instituci_n_de_educaci_n_superior_ies"
                        ),
                        "anio": anio,
                        "matriculados": msg.value.get("payload", {}).get("matriculados_2015"),
                    }
                )
                print(
                    f"[partición {msg.partition} | offset {msg.offset}] "
                    f"key={msg.key} | anio={anio} | mat={muestras[-1]['matriculados']}"
                )
    finally:
        if propio:
            consumidor.close()

    total = sum(por_particion.values())
    print(f"\nGrupo '{grupo}' leyó {total:,} mensajes de '{topico}':")
    print("  Distribución por partición:", dict(sorted(por_particion.items())))
    print("  Distribución por año:      ", dict(sorted(por_anio.items())))
    return {
        "topico": topico,
        "grupo": grupo,
        "total": total,
        "por_particion": dict(sorted(por_particion.items())),
        "por_anio": dict(sorted(por_anio.items())),
        "muestras": muestras,
    }

