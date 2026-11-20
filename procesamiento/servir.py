"""
Fase 8a — Trabajo Spark: zona curada (HDFS Parquet) -> zona de servicio (PostgreSQL).

Construye 6 tablas analíticas precalculadas en el esquema `servicio.*` para que
el tablero HTML (Plotly.js) y Power BI consulten en milisegundos sin hacer cálculos:

  1. servicio.indicador_evolucion_semestral   Evolución 2015-2021 por semestre y fecha_periodo
  2. servicio.indicador_departamentos         Ranking y serie de los 33 departamentos DANE + foco Antioquia
  3. servicio.indicador_sector_metodologia    Sector (Oficial/Privada) x Metodología (Presencial/Virtual/Distancia)
  4. servicio.indicador_areas_genero          8 áreas SNIES x Género (brecha de género por área y año)
  5. servicio.indicador_top_ies               Top IES nacionales y de Antioquia (incluye I.U. Pascual Bravo)
  6. servicio.metadatos_pipeline              Fuente, fecha de corte, conteos de trazabilidad y versión
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from procesamiento import catalogo
from procesamiento.transformaciones import crear_sesion

HDFS = "hdfs://namenode:8020"


def construir_tablas_servicio(curado: DataFrame) -> dict:
    """Construye los indicadores agregados listos para consumo BI."""
    curado = curado.cache()

    # 1. Evolución semestral nacional (2015-1 a 2021-2)
    evolucion = (
        curado.groupBy("anio", "semestre", "fecha_periodo")
        .agg(
            F.sum("total_matriculados").alias("total_matriculados"),
            F.sum(F.when(F.col("cod_depto_oferta") == "05", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_antioquia"
            ),
            F.sum(F.when(F.col("sector_nombre") == "Oficial", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_oficial"
            ),
            F.sum(F.when(F.col("sector_nombre") == "Privada", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_privada"
            ),
            F.sum(F.when(F.col("metodologia_nombre") == "Virtual", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_virtual"
            ),
            F.sum(F.when(F.col("genero_nombre") == "Femenino", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_femenino"
            ),
            F.sum(F.when(F.col("genero_nombre") == "Masculino", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_masculino"
            ),
            F.countDistinct("codigo_ies").alias("total_ies"),
            F.countDistinct("codigo_snies").alias("total_programas"),
            F.count("*").alias("registros"),
        )
        .withColumn("periodo_label", F.concat_ws("-", F.col("anio").cast("string"), F.col("semestre").cast("string")))
        .withColumn(
            "pct_antioquia",
            F.round(100.0 * F.col("matriculados_antioquia") / F.col("total_matriculados"), 2),
        )
        .withColumn(
            "pct_virtual",
            F.round(100.0 * F.col("matriculados_virtual") / F.col("total_matriculados"), 2),
        )
        .withColumn(
            "pct_femenino",
            F.round(100.0 * F.col("matriculados_femenino") / F.col("total_matriculados"), 2),
        )
        .orderBy("anio", "semestre")
    )

    # 2. Departamentos por año (semestre 1 como cohorte principal + total anual)
    w_anio = Window.partitionBy("anio")
    departamentos = (
        curado.groupBy("anio", "cod_depto_oferta", "departamento_oferta")
        .agg(
            F.sum("total_matriculados").alias("total_matriculados"),
            F.sum(F.when(F.col("semestre") == 1, F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_sem1"
            ),
            F.sum(F.when(F.col("sector_nombre") == "Oficial", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_oficial"
            ),
            F.sum(F.when(F.col("metodologia_nombre") == "Virtual", F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_virtual"
            ),
            F.countDistinct("codigo_ies").alias("total_ies"),
            F.countDistinct("codigo_snies").alias("total_programas"),
        )
        .withColumn("es_antioquia", F.col("cod_depto_oferta") == "05")
        .withColumn(
            "participacion_nacional_pct",
            F.round(100.0 * F.col("total_matriculados") / F.sum("total_matriculados").over(w_anio), 2),
        )
        .withColumn(
            "pct_oficial",
            F.round(100.0 * F.col("matriculados_oficial") / F.col("total_matriculados"), 2),
        )
        .orderBy("anio", F.col("total_matriculados").desc())
    )

    # 3. Sector x Metodología por año y semestre
    sector_metodologia = (
        curado.groupBy("anio", "semestre", "fecha_periodo", "sector_nombre", "metodologia_nombre")
        .agg(
            F.sum("total_matriculados").alias("total_matriculados"),
            F.countDistinct("codigo_snies").alias("total_programas"),
            F.count("*").alias("registros"),
        )
        .orderBy("anio", "semestre", "sector_nombre", "metodologia_nombre")
    )

    # 4. Áreas de conocimiento x Género por año
    areas_genero = (
        curado.groupBy("anio", "area_nombre", "genero_nombre")
        .agg(
            F.sum("total_matriculados").alias("total_matriculados"),
            F.countDistinct("codigo_snies").alias("total_programas"),
        )
        .orderBy("anio", "area_nombre", "genero_nombre")
    )

    # 5. Top IES (Nacional y Antioquia, último año con datos + histórico)
    top_ies = (
        curado.groupBy(
            "anio",
            "codigo_ies",
            "nombre_ies",
            "sector_nombre",
            "caracter_nombre",
            "departamento_ies",
        )
        .agg(
            F.sum("total_matriculados").alias("total_matriculados"),
            F.sum(F.when(F.col("semestre") == 1, F.col("total_matriculados")).otherwise(0)).alias(
                "matriculados_sem1"
            ),
            F.countDistinct("codigo_snies").alias("total_programas"),
        )
        .withColumn("es_pascual_bravo", F.col("codigo_ies") == 3107)
        .withColumn("es_antioquia", F.col("departamento_ies") == "Antioquia")
        .orderBy("anio", F.col("total_matriculados").desc())
    )

    # 6. Metadatos y fecha de corte para el dashboard
    stats = curado.agg(
        F.count("*").alias("filas_curadas"),
        F.sum("total_matriculados").alias("total_matriculados_acum"),
        F.min("fecha_periodo").alias("fecha_desde"),
        F.max("fecha_periodo").alias("fecha_hasta"),
        F.min("anio").alias("anio_desde"),
        F.max("anio").alias("anio_hasta"),
        F.countDistinct("codigo_ies").alias("ies_unicas"),
        F.countDistinct("codigo_snies").alias("programas_unicos"),
        F.countDistinct("departamento_oferta").alias("departamentos_unicos"),
    ).collect()[0]

    spark = curado.sparkSession
    metadatos = spark.createDataFrame(
        [
            (
                catalogo.FUENTE_OFICIAL_CITA,
                "https://www.datos.gov.co/d/5wck-szir",
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                str(stats["fecha_desde"]),
                str(stats["fecha_hasta"]),
                int(stats["anio_desde"]),
                int(stats["anio_hasta"]),
                int(stats["filas_curadas"]),
                int(stats["total_matriculados_acum"]),
                int(stats["ies_unicas"]),
                int(stats["programas_unicos"]),
                int(stats["departamentos_unicos"]),
            )
        ],
        [
            "fuente_cita",
            "url_fuente",
            "fecha_ejecucion_utc",
            "fecha_desde",
            "fecha_hasta",
            "anio_desde",
            "anio_hasta",
            "filas_curadas",
            "total_matriculados_acum",
            "ies_unicas",
            "programas_unicos",
            "departamentos_unicos",
        ],
    )

    return {
        "indicador_evolucion_semestral": evolucion,
        "indicador_departamentos": departamentos,
        "indicador_sector_metodologia": sector_metodologia,
        "indicador_areas_genero": areas_genero,
        "indicador_top_ies": top_ies,
        "metadatos_pipeline": metadatos,
    }


def escribir_postgres(tablas: dict, url: str, usuario: str, clave: str, esquema: str) -> None:
    for nombre, df in tablas.items():
        t0 = time.time()
        n = df.count()
        (
            df.write.mode("overwrite")
            .option("truncate", "true")
            .option("batchsize", 5000)
            .jdbc(
                url,
                f"{esquema}.{nombre}",
                properties={
                    "user": usuario,
                    "password": clave,
                    "driver": "org.postgresql.Driver",
                },
            )
        )
        print(f"  {esquema}.{nombre:<32} {n:>7,} filas  ({time.time() - t0:.1f} s)")


def exportar_muestras_csv(tablas: dict, carpeta_salida: Path) -> None:
    """Exporta las tablas de la zona de servicio a CSV (entregable Datos + respaldo)."""
    carpeta_salida.mkdir(parents=True, exist_ok=True)
    for nombre, df in tablas.items():
        ruta = carpeta_salida / f"{nombre}.csv"
        filas = df.collect()
        cols = df.columns
        import csv

        with ruta.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            for row in filas:
                writer.writerow([row[c] for c in cols])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Fase 8a: zona curada HDFS -> PostgreSQL servicio.*")
    p.add_argument("--curado", default=f"{HDFS}/datalake/curated/matricula_es")
    p.add_argument(
        "--jdbc",
        default=os.getenv("POSTGRES_JDBC", "jdbc:postgresql://postgres:5432/matricula_es"),
    )
    p.add_argument("--usuario", default=os.getenv("POSTGRES_USER", "pipeline"))
    p.add_argument("--clave", default=os.getenv("POSTGRES_PASSWORD", "pipeline"))
    p.add_argument("--esquema", default="servicio")
    p.add_argument("--exportar-csv", default="datos_muestra/zona_servicio")
    p.add_argument("--master", default=None)
    args = p.parse_args(argv)

    inicio = time.time()
    spark: SparkSession = crear_sesion("servir-matricula-es", master=args.master)
    spark.sparkContext.setLogLevel("ERROR")

    curado = spark.read.parquet(args.curado)
    tablas = construir_tablas_servicio(curado)

    print(f"\n[Fase 8a] Cargando indicadores a PostgreSQL ({args.jdbc}, esquema {args.esquema}):")
    escribir_postgres(tablas, args.jdbc, args.usuario, args.clave, args.esquema)

    if args.exportar_csv:
        exportar_muestras_csv(tablas, Path(args.exportar_csv))
        print(f"Tablas de servicio exportadas también a {args.exportar_csv}/")

    print("\n=== Evolución semestral (resumen) ===")
    tablas["indicador_evolucion_semestral"].select(
        "periodo_label",
        "total_matriculados",
        "matriculados_antioquia",
        "pct_virtual",
        "pct_femenino",
    ).show(20, truncate=False)

    print(f"Terminado en {time.time() - inicio:.1f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

