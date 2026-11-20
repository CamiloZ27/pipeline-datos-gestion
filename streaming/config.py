"""Configuración de Apache Kafka (Fase 3)."""
from __future__ import annotations

import os

from ingesta import config as config_ingesta

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_DIR = config_ingesta.RAW_DIR

# Un tópico por fuente con 3 particiones (permite paralelismo por consumer group)
TOPICOS = {
    "datos_gov_co": "raw.datos_gov_co",
}

PARTICIONES = 3
REPLICACION = 1
CONFIG_TOPICO = {
    "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 días de retención
    "cleanup.policy": "delete",
}

# Archivo checkpoint que marca qué .jsonl ya se publicaron en Kafka
SUFIJO_ENVIADO = ".kafka_ok"

