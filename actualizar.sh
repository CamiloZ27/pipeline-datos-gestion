#!/usr/bin/env bash
# =============================================================================
# Ejecución completa del pipeline end-to-end (macOS / Linux)
# Estudiantes: Camilo Zuleta Delgado · Santiago Restrepo Silva
#
# Recorre todas las fases (2 a 8) + Hive opcional, encendiendo solo los
# contenedores que necesita cada etapa para mantener bajo el consumo de RAM.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-.venv/bin/python3}"
if [ ! -x "$PYTHON" ]; then
  PYTHON="python3"
fi

echo "=== [1/8] Ingesta desde datos.gov.co (SODA2, dataset 5wck-szir) ==="
"$PYTHON" -m ingesta descargar "$@"

echo ""
echo "=== [2/8] Apache Kafka: tópico 3 particiones + productor ==="
docker compose --profile kafka --profile hdfs up -d --wait
"$PYTHON" -m streaming crear-topicos
"$PYTHON" -m streaming producir

echo ""
echo "=== [3/8] HDFS: consumidor por lotes (WebHDFS) ==="
"$PYTHON" -m datalake consumir
"$PYTHON" -m datalake resumen /datalake/raw

echo ""
echo "=== [4/8] Spark Structured Streaming (Capa Rápida Lambda) ==="
docker compose --profile postgres up -d --wait
docker compose run --rm spark-streaming

echo ""
echo "=== [5/8] MapReduce Python vs. PySpark (validación cruzada sobre raw) ==="
docker compose stop kafka
docker compose run --rm spark-mapreduce

echo ""
echo "=== [6/8] PySpark Zona Curada (Parquet particionado + 5 validaciones) ==="
docker compose run --rm spark

echo ""
echo "=== [7/8] Zona de Servicio PostgreSQL (tablas de indicadores) ==="
docker compose run --rm spark-servir

echo ""
echo "=== [8/8] Tablero HTML interactivo (Plotly.js) ==="
"$PYTHON" -m visualizacion generar --origen postgres --publicar

docker compose stop namenode datanode

echo ""
echo "Pipeline completo finalizado sin errores."
echo "  Tablero local: salida/dashboard.html"
echo "  GitHub Pages:  docs/index.html"

