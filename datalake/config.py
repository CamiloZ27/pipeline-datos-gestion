"""Configuración de HDFS / WebHDFS y consumidor Kafka -> HDFS (Fase 4a)."""
from __future__ import annotations

import os

from streaming import config as config_streaming

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
# Traduce 'datanode:9864' (red interna Docker) a 'localhost:9864' en el host Mac/Windows.
# Dentro de contenedores (Airflow), DATANODE_PUBLICO="" deja la dirección interna intacta.
DATANODE_PUBLICO = os.getenv("DATANODE_PUBLICO", "localhost:9864")
HDFS_USUARIO = os.getenv("HDFS_USUARIO", "root")
HDFS_TIMEOUT = 60

RAIZ_RAW = "/datalake/raw"
RAIZ_CURATED = "/datalake/curated"

KAFKA_BOOTSTRAP = config_streaming.KAFKA_BOOTSTRAP
TOPICOS = list(config_streaming.TOPICOS.values())
GRUPO_CONSUMIDOR = os.getenv("HDFS_KAFKA_GROUP", "hdfs-raw-writer")

# Un archivo por lote grande (evita el problema de millones de archivos pequeños en el NameNode)
MAX_REGISTROS_POR_LOTE = int(os.getenv("HDFS_BATCH_RECORDS", "50000"))
ESPERA_POLL_MS = 2500
POLLS_VACIOS_PARA_TERMINAR = 3
ESPERA_ASIGNACION_S = 30
REINTENTOS_MODO_SEGURO = 10
ESPERA_MODO_SEGURO_S = 5

