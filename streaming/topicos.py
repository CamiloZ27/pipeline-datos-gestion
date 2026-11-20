"""Creación e inspección de tópicos en Kafka con KafkaAdminClient (Fase 3)."""
from __future__ import annotations

import logging

from streaming import config

log = logging.getLogger("streaming.topicos")


def crear_topicos(admin=None) -> dict:
    """Crea explícitamente los tópicos configurados (3 particiones)."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    propio = admin is None
    admin = admin or KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        client_id="matricula-admin",
    )
    try:
        existentes = set(admin.list_topics())
        resultado = {}
        nuevos = []
        for topico in config.TOPICOS.values():
            if topico in existentes:
                resultado[topico] = "ya existía"
            else:
                nuevos.append(
                    NewTopic(
                        name=topico,
                        num_partitions=config.PARTICIONES,
                        replication_factor=config.REPLICACION,
                        topic_configs=config.CONFIG_TOPICO,
                    )
                )
        if nuevos:
            try:
                admin.create_topics(nuevos)
            except TopicAlreadyExistsError:
                pass
            for t in nuevos:
                resultado[t.name] = "creado"

        for topico, estado in resultado.items():
            log.info("%-24s %s (particiones=%d)", topico, estado, config.PARTICIONES)
        return resultado
    finally:
        if propio:
            admin.close()


def describir_topicos() -> dict:
    """Devuelve particiones y conteo de mensajes por partición de cada tópico."""
    from kafka import KafkaConsumer, TopicPartition

    consumidor = KafkaConsumer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        client_id="matricula-inspector",
    )
    resumen = {}
    try:
        for topico in config.TOPICOS.values():
            parts = sorted(consumidor.partitions_for_topic(topico) or [])
            tps = [TopicPartition(topico, p) for p in parts]
            inicios = consumidor.beginning_offsets(tps) if tps else {}
            finales = consumidor.end_offsets(tps) if tps else {}
            detalle = {
                p: {
                    "offset_inicio": inicios[TopicPartition(topico, p)],
                    "offset_fin": finales[TopicPartition(topico, p)],
                    "mensajes": finales[TopicPartition(topico, p)] - inicios[TopicPartition(topico, p)],
                }
                for p in parts
            }
            resumen[topico] = {
                "num_particiones": len(parts),
                "total_mensajes": sum(d["mensajes"] for d in detalle.values()),
                "particiones": detalle,
            }
        return resumen
    finally:
        consumidor.close()

