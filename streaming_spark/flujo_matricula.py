"""
Fase 7 — Spark Structured Streaming (Capa Rápida / Velocidad de Arquitectura Lambda).

Lee en streaming desde el tópico de Apache Kafka `raw.datos_gov_co` (o desde una
carpeta de lotes JSONL con `--origen carpeta`) y calcula una agregación por
ventana de tiempo con:
  - `event_time`: timestamp de ingesta del evento (`fecha_ingesta` UTC)
  - `withWatermark("event_time", "5 minutes")`: maneja eventos tardíos y limita
    el estado en memoria de Spark
  - `dropDuplicatesWithinWatermark(["id_registro"])`: evita duplicados en el flujo
  - `window("event_time", "10 minutes")`: ventana temporal por año, sector y metodología
  - `checkpointLocation`: guarda offsets exactos de Kafka y estado de ventanas

Prueba de reinicio sin duplicación:
  Al ejecutar este script una segunda vez con el mismo `--checkpoint` y sin
  mensajes nuevos en Kafka, Spark lee los offsets confirmados del checkpoint,
  procesa 0 filas nuevas y los conteos de salida permanecen exactamente iguales.

Integración Lambda (Batch + Velocidad -> Servicio):
  - La capa Batch (`procesamiento/curar.py` -> `servir.py`) calcula la verdad
    histórica completa sobre HDFS (`servicio.indicador_sector_metodologia`).
  - La capa de Velocidad (`flujo_matricula.py`) procesa incrementalmente lo que
    entra por Kafka con baja latencia y actualiza `servicio.stream_ventanas` en
    PostgreSQL, permitiendo monitorear cargas recientes antes del próximo batch.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from procesamiento import catalogo
from procesamiento.transformaciones import ESQUEMA_PAYLOAD, ESQUEMA_SOBRE, crear_sesion


def _mapa_spark(diccionario: dict):
    return F.create_map(*[F.lit(x) for par in diccionario.items() for x in par])


def construir_flujo_agregado(lineas_stream: DataFrame, watermark: str = "5 minutes", ventana: str = "10 minutes") -> DataFrame:
    """Transforma el flujo crudo en agregación por ventana temporal con watermark."""
    m_sector = _mapa_spark(catalogo.SECTORES)
    m_met = _mapa_spark(catalogo.METODOLOGIAS)

    sobres = (
        lineas_stream.select(F.from_json("value", ESQUEMA_SOBRE).alias("s"))
        .select("s.*")
        .where(F.col("id_registro").isNotNull() & F.col("payload").isNotNull())
        .withColumn(
            "event_time",
            F.expr("try_to_timestamp(fecha_ingesta, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
        )
        .where(F.col("event_time").isNotNull())
    )

    con_watermark = sobres.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(
        ["id_registro"]
    )

    p = F.from_json("payload", ESQUEMA_PAYLOAD)
    id_sector = F.expr("try_cast(trim(p.id_sector) AS INT)")
    id_met = F.expr("try_cast(trim(p.id_metodologia) AS INT)")
    anio = F.expr("try_cast(trim(p.a_o) AS INT)")
    mat = F.expr("try_cast(try_cast(trim(p.matriculados_2015) AS DOUBLE) AS INT)")

    eventos = (
        con_watermark.withColumn("p", p)
        .select(
            "event_time",
            "id_registro",
            anio.alias("anio"),
            F.coalesce(m_sector[id_sector], F.lit("Sin especificar")).alias("sector_nombre"),
            F.coalesce(m_met[id_met], F.lit("Otra metodología")).alias("metodologia_nombre"),
            mat.alias("total_matriculados"),
        )
        .where(F.col("anio").isNotNull() & F.col("total_matriculados").isNotNull() & (F.col("total_matriculados") > 0))
    )

    return (
        eventos.groupBy(
            F.window("event_time", ventana).alias("ventana"),
            "anio",
            "sector_nombre",
            "metodologia_nombre",
        )
        .agg(
            F.count("*").alias("registros_unicos"),
            F.sum("total_matriculados").alias("total_matriculados"),
        )
        .select(
            F.col("ventana.start").alias("ventana_inicio"),
            F.col("ventana.end").alias("ventana_fin"),
            "anio",
            "sector_nombre",
            "metodologia_nombre",
            "registros_unicos",
            "total_matriculados",
        )
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Fase 7: Spark Structured Streaming (Capa Rápida Lambda)")
    parser.add_argument("--origen", choices=["kafka", "carpeta"], default="kafka")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:19092"))
    parser.add_argument("--topico", default="raw.datos_gov_co")
    parser.add_argument("--carpeta", default="data/raw/datos_gov_co/men_matricula_es")
    parser.add_argument("--checkpoint", default="data/checkpoints/stream_matricula")
    parser.add_argument("--salida-parquet", default="data/curated_local/stream_ventanas")
    parser.add_argument("--watermark", default="5 minutes")
    parser.add_argument("--ventana", default="10 minutes")
    parser.add_argument("--escribir-postgres", action="store_true", default=True)
    parser.add_argument("--sin-postgres", action="store_true")
    parser.add_argument(
        "--jdbc",
        default=os.getenv("POSTGRES_JDBC", "jdbc:postgresql://postgres:5432/matricula_es"),
    )
    parser.add_argument("--usuario", default=os.getenv("POSTGRES_USER", "pipeline"))
    parser.add_argument("--clave", default=os.getenv("POSTGRES_PASSWORD", "pipeline"))
    parser.add_argument("--evidencia", default="evidencias/streaming_reinicio.json")
    args = parser.parse_args(argv)

    t0 = time.time()
    spark: SparkSession = crear_sesion("streaming-matricula-es")
    spark.sparkContext.setLogLevel("ERROR")

    if args.origen == "kafka":
        print(f"\n[Fase 7] Structured Streaming desde Kafka ({args.bootstrap} / tópico {args.topico})")
        crudo = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("subscribe", args.topico)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("value"))
        )
    else:
        print(f"\n[Fase 7] Structured Streaming desde carpeta ({args.carpeta})")
        crudo = spark.readStream.text(f"{args.carpeta}/fecha_ingesta=*")

    agregado = construir_flujo_agregado(crudo, watermark=args.watermark, ventana=args.ventana)

    lotes_ejecutados = []
    destino_parquet = Path(args.salida_parquet)
    destino_parquet.parent.mkdir(parents=True, exist_ok=True)

    def procesar_lote(df_lote: DataFrame, batch_id: int):
        n_filas = df_lote.count()
        if n_filas == 0:
            return
        suma_reg = int(df_lote.agg(F.sum("registros_unicos")).collect()[0][0] or 0)
        suma_mat = int(df_lote.agg(F.sum("total_matriculados")).collect()[0][0] or 0)

        # Escritura idempotente usando directorio temporal (evita leer y sobrescribir la misma ruta)
        import shutil
        tmp_parquet = destino_parquet.with_name(destino_parquet.name + "_tmp")
        shutil.rmtree(tmp_parquet, ignore_errors=True)
        if destino_parquet.exists() and any(destino_parquet.glob("*.parquet")):
            previo = spark.read.parquet(str(destino_parquet))
            claves = ["ventana_inicio", "ventana_fin", "anio", "sector_nombre", "metodologia_nombre"]
            sin_actualizados = previo.join(df_lote.select(*claves), claves, "left_anti")
            combinado = sin_actualizados.unionByName(df_lote)
            combinado.coalesce(1).write.mode("overwrite").parquet(str(tmp_parquet))
            shutil.rmtree(destino_parquet, ignore_errors=True)
            shutil.move(str(tmp_parquet), str(destino_parquet))
        else:
            df_lote.coalesce(1).write.mode("overwrite").parquet(str(destino_parquet))

        if not args.sin_postgres:
            # Refleja el estado consolidado de la capa rápida en PostgreSQL (unión Lambda)
            estado_actual = spark.read.parquet(str(destino_parquet))
            (
                estado_actual.write.mode("overwrite")
                .option("truncate", "true")
                .jdbc(
                    args.jdbc,
                    "servicio.stream_ventanas",
                    properties={
                        "user": args.usuario,
                        "password": args.clave,
                        "driver": "org.postgresql.Driver",
                    },
                )
            )

        lotes_ejecutados.append(
            {
                "batch_id": batch_id,
                "filas_ventana_lote": n_filas,
                "registros_unicos_lote": suma_reg,
                "matriculados_lote": suma_mat,
            }
        )
        print(
            f"  [Batch {batch_id}] {n_filas} filas de ventana | "
            f"registros_unicos={suma_reg:,} | matriculados={suma_mat:,}"
        )

    query = (
        agregado.writeStream.outputMode("update")
        .foreachBatch(procesar_lote)
        .option("checkpointLocation", args.checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    # Conteos acumulados después de esta corrida (para demostrar reinicio sin duplicación)
    filas_totales = 0
    reg_totales = 0
    mat_totales = 0
    if destino_parquet.exists() and any(destino_parquet.glob("*.parquet")):
        final_df = spark.read.parquet(str(destino_parquet))
        filas_totales = final_df.count()
        agg_final = final_df.agg(
            F.sum("registros_unicos").alias("r"),
            F.sum("total_matriculados").alias("m"),
        ).collect()[0]
        reg_totales = int(agg_final["r"] or 0)
        mat_totales = int(agg_final["m"] or 0)
        print("\n=== Estado consolidado de la capa de velocidad (muestra) ===")
        final_df.orderBy("anio", F.col("total_matriculados").desc()).show(12, truncate=False)

    evento_corrida = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "origen": args.origen,
        "watermark": args.watermark,
        "ventana": args.ventana,
        "checkpoint": args.checkpoint,
        "lotes_nuevos_procesados": len(lotes_ejecutados),
        "detalle_lotes": lotes_ejecutados,
        "estado_acumulado": {
            "filas_ventana_total": filas_totales,
            "registros_unicos_total": reg_totales,
            "total_matriculados": mat_totales,
        },
        "duracion_s": round(time.time() - t0, 1),
    }

    ruta_ev = Path(args.evidencia)
    ruta_ev.parent.mkdir(parents=True, exist_ok=True)
    historial = []
    if ruta_ev.exists():
        try:
            prev = json.loads(ruta_ev.read_text(encoding="utf-8"))
            historial = prev.get("ejecuciones", [])
        except json.JSONDecodeError:
            historial = []
    historial.append(evento_corrida)
    ruta_ev.write_text(
        json.dumps({"ejecuciones": historial}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"\nResumen corrida #{len(historial)}: lotes_nuevos={len(lotes_ejecutados)} | "
        f"filas_ventana={filas_totales} | registros_unicos={reg_totales:,} | "
        f"matriculados={mat_totales:,} ({time.time() - t0:.1f} s)"
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
