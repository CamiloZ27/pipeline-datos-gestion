"""
### Componente opcional (+3 pts) — DAG completo en Apache Airflow 3.3.2

Orquesta el pipeline end-to-end con reintentos automáticos y reglas de disparo:
  1. `ingesta_incremental`: descarga registros nuevos de datos.gov.co (marca de agua)
  2. `encender_kafka_hdfs`: arranca Kafka y HDFS
  3. `kafka_producir`: publica nuevos JSONL en tópico `raw.datos_gov_co` (3 particiones)
  4. `hdfs_consumir`: escribe lotes desde Kafka hacia HDFS (`/datalake/raw`)
  5. `spark_streaming_lambda`: actualiza la capa de velocidad (watermark + checkpoint)
  6. `apagar_kafka`: libera RAM (trigger_rule="all_done")
  7. `mapreduce_vs_spark`: valida conteos MapReduce Python vs. PySpark
  8. `spark_curar_y_calidad`: cura a Parquet particionado y corre 5 validaciones (fail-fast)
  9. `spark_servir_postgres`: carga indicadores a PostgreSQL `servicio.*`
 10. `apagar_hdfs`: libera RAM (trigger_rule="all_done")
 11. `generar_dashboard`: regenera el tablero HTML Plotly autocontenido
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

ZONA = "America/Bogota"
SPARK = "matricula-spark-tareas"
SPARK_SUBMIT = [
    "/opt/spark/bin/spark-submit",
    "--master",
    "local[2]",
    "--driver-memory",
    "1500m",
]


@dag(
    dag_id="matricula_es_pipeline_completo",
    description="Ingesta incremental -> Kafka -> HDFS -> MapReduce -> PySpark -> Streaming -> PostgreSQL -> Dashboard",
    schedule="0 6 * * 1",  # Lunes 6:00 a. m. hora Colombia (también ejecutable manualmente)
    start_date=pendulum.datetime(2026, 11, 1, tz=ZONA),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "camilo_zuleta_santiago_restrepo",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["matricula-es", "pascual-bravo", "lambda"],
    doc_md=__doc__,
)
def matricula_pipeline():
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def ingesta_incremental():
        from ingesta import matricula

        archivo = matricula.ejecutar(incremental=True)
        return str(archivo) if archivo else "sin_datos_nuevos"

    @task
    def encender_kafka_hdfs():
        from orquestacion import contenedores

        contenedores.encender(["matricula-kafka", "matricula-namenode", "matricula-datanode"])

    @task
    def kafka_producir():
        from streaming import productor, topicos

        topicos.crear_topicos()
        enviados = productor.ejecutar()
        return {"archivos": len(enviados), "mensajes": sum(enviados.values())}

    @task
    def hdfs_consumir():
        from datalake import consumidor_hdfs

        return consumidor_hdfs.ejecutar()

    @task
    def spark_streaming_lambda():
        from orquestacion import contenedores

        jars_stream = ",".join([
            "/proyecto/jars/postgresql-42.7.4.jar",
            "/proyecto/jars/spark-sql-kafka-0-10_2.13-4.0.2.jar",
            "/proyecto/jars/spark-token-provider-kafka-0-10_2.13-4.0.2.jar",
            "/proyecto/jars/kafka-clients-3.9.1.jar",
            "/proyecto/jars/commons-pool2-2.12.0.jar",
            "/proyecto/jars/scala-parallel-collections_2.13-1.2.0.jar",
        ])
        contenedores.ejecutar(
            SPARK,
            SPARK_SUBMIT
            + [
                "--jars",
                jars_stream,
                "streaming_spark/flujo_matricula.py",
            ],
            directorio="/proyecto",
        )

    @task(trigger_rule="all_done")
    def apagar_kafka():
        from orquestacion import contenedores

        contenedores.apagar(["matricula-kafka"])

    @task
    def mapreduce_vs_spark():
        from orquestacion import contenedores

        contenedores.ejecutar(
            SPARK,
            SPARK_SUBMIT + ["mapreduce/comparar_spark.py"],
            directorio="/proyecto",
        )

    @task
    def spark_curar_y_calidad():
        from orquestacion import contenedores

        contenedores.ejecutar(
            SPARK,
            SPARK_SUBMIT
            + [
                "--conf",
                "spark.hadoop.dfs.replication=1",
                "procesamiento/curar.py",
            ],
            directorio="/proyecto",
        )

    @task
    def spark_servir_postgres():
        from orquestacion import contenedores

        contenedores.ejecutar(
            SPARK,
            SPARK_SUBMIT
            + [
                "--jars",
                "/proyecto/jars/postgresql-42.7.4.jar",
                "procesamiento/servir.py",
            ],
            directorio="/proyecto",
        )

    @task(trigger_rule="all_done")
    def apagar_hdfs():
        from orquestacion import contenedores

        contenedores.apagar(["matricula-namenode", "matricula-datanode"])

    @task
    def generar_dashboard():
        from visualizacion.generar import generar

        ruta = generar(origen="postgres", publicar=True)
        return str(ruta)

    # Grafo de dependencias (Arquitectura Lambda completa)
    ing = ingesta_incremental()
    enc = encender_kafka_hdfs()
    prod = kafka_producir()
    hdfs = hdfs_consumir()
    stream = spark_streaming_lambda()
    mr = mapreduce_vs_spark()
    cur = spark_curar_y_calidad()
    srv = spark_servir_postgres()
    dash = generar_dashboard()

    ing >> enc >> prod >> [hdfs, stream]
    [hdfs, stream] >> apagar_kafka()
    hdfs >> mr >> cur >> srv
    srv >> apagar_hdfs()
    [srv, stream] >> dash


matricula_pipeline()
