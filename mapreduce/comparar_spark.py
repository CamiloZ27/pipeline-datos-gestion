"""
Fase 4b — Validación cruzada: MapReduce en Python vs. PySpark sobre los mismos
datos raw en HDFS (o carpeta local).

Calcula sobre la zona raw (SIN deduplicar aún, para comparar exactamente el
cálculo distribuido sobre datos crudos):
  - Conteo de filas raw por (anio, id_sector)
  - Suma de matriculados raw por (anio, id_sector)

Al final imprime la tabla lado a lado y comprueba 0 diferencias entre ambos motores.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

HDFS = "hdfs://namenode:8020"
NOMBRES_SECTOR = {"1": "Oficial", "2": "Privada"}


def _ejecutar_mapreduce_subprocess(archivos_jsonl: list) -> Dict[str, Tuple[int, int]]:
    """Ejecuta literalmente `mapper.py | sort | reducer.py` sobre los archivos JSONL."""
    proyecto = Path(__file__).resolve().parent.parent
    mapper = str(proyecto / "mapreduce" / "mapper.py")
    reducer = str(proyecto / "mapreduce" / "reducer.py")

    p_map = subprocess.Popen(
        [sys.executable, mapper],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    p_sort = subprocess.Popen(
        ["sort"],
        stdin=p_map.stdout,
        stdout=subprocess.PIPE,
        text=True,
    )
    p_red = subprocess.Popen(
        [sys.executable, reducer],
        stdin=p_sort.stdout,
        stdout=subprocess.PIPE,
        text=True,
    )
    p_map.stdout.close()
    p_sort.stdout.close()

    for ruta in archivos_jsonl:
        with open(ruta, "r", encoding="utf-8") as f:
            for linea in f:
                p_map.stdin.write(linea)
    p_map.stdin.close()

    salida, _ = p_red.communicate()
    p_map.wait()
    p_sort.wait()
    if p_red.returncode != 0:
        raise RuntimeError(f"MapReduce falló con código {p_red.returncode}")

    resultado = {}
    for linea in salida.splitlines():
        if not linea.strip():
            continue
        clave, reg_s, mat_s = linea.split("\t")
        resultado[clave] = (int(reg_s), int(mat_s))
    return resultado


def _ejecutar_mapreduce_desde_spark_lineas(spark: SparkSession, patron: str) -> Dict[str, Tuple[int, int]]:
    """Lee líneas de HDFS en streaming hacia mapper.py | sort | reducer.py."""
    proyecto = Path(__file__).resolve().parent.parent
    mapper = str(proyecto / "mapreduce" / "mapper.py")
    reducer = str(proyecto / "mapreduce" / "reducer.py")

    p_map = subprocess.Popen(
        [sys.executable, mapper],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    p_sort = subprocess.Popen(
        ["sort"],
        stdin=p_map.stdout,
        stdout=subprocess.PIPE,
        text=True,
    )
    p_red = subprocess.Popen(
        [sys.executable, reducer],
        stdin=p_sort.stdout,
        stdout=subprocess.PIPE,
        text=True,
    )
    p_map.stdout.close()
    p_sort.stdout.close()

    for fila in spark.read.text(patron).toLocalIterator():
        p_map.stdin.write(fila["value"] + "\n")
    p_map.stdin.close()

    salida, _ = p_red.communicate()
    p_map.wait()
    p_sort.wait()

    resultado = {}
    for linea in salida.splitlines():
        if not linea.strip():
            continue
        clave, reg_s, mat_s = linea.split("\t")
        resultado[clave] = (int(reg_s), int(mat_s))
    return resultado


def _ejecutar_spark(spark: SparkSession, patron: str) -> Dict[str, Tuple[int, int]]:
    esquema_sobre = T.StructType([T.StructField("payload", T.StringType())])
    esquema_payload = T.StructType(
        [
            T.StructField("a_o", T.StringType()),
            T.StructField("id_sector", T.StringType()),
            T.StructField("matriculados_2015", T.StringType()),
        ]
    )

    df = (
        spark.read.text(patron)
        .select(F.from_json("value", esquema_sobre).alias("s"))
        .select(F.from_json("s.payload", esquema_payload).alias("p"))
        .select(
            F.coalesce(F.trim(F.col("p.a_o")), F.lit("DESCONOCIDO")).alias("anio"),
            F.coalesce(F.trim(F.col("p.id_sector")), F.lit("DESCONOCIDO")).alias("sector"),
            F.coalesce(
                F.expr("try_cast(try_cast(p.matriculados_2015 AS DOUBLE) AS BIGINT)"),
                F.lit(0),
            ).alias("mat"),
        )
        .groupBy("anio", "sector")
        .agg(F.count("*").alias("reg"), F.sum("mat").alias("total_mat"))
    )

    resultado = {}
    for r in df.collect():
        resultado[f"{r['anio']}|{r['sector']}"] = (int(r["reg"]), int(r["total_mat"]))
    return resultado


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Fase 4b: MapReduce Python vs PySpark")
    parser.add_argument("--origen", default=f"{HDFS}/datalake/raw")
    parser.add_argument("--salida-evidencia", default="evidencias/mapreduce_vs_spark.json")
    args = parser.parse_args(argv)

    spark = (
        SparkSession.builder.appName("comparar-mapreduce-spark")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    patron = f"{args.origen}/*/*/fecha_ingesta=*/*.jsonl"
    print(f"\nComparando MapReduce (Python) vs. PySpark sobre: {patron}\n")

    if args.origen.startswith("hdfs://"):
        mr_res = _ejecutar_mapreduce_desde_spark_lineas(spark, patron)
    else:
        archivos = sorted(Path(args.origen).glob("*/*/fecha_ingesta=*/*.jsonl"))
        mr_res = _ejecutar_mapreduce_subprocess(archivos)

    sp_res = _ejecutar_spark(spark, patron)
    spark.stop()

    claves = sorted(set(mr_res) | set(sp_res))
    diferencias = 0
    filas_reporte = []

    encabezado = (
        f"{'Año':<6} {'Sector':<10} | {'MR Regs':>10} {'Spark Regs':>10} | "
        f"{'MR Matric.':>12} {'Spark Matric.':>13} | {'Estado'}"
    )
    print(encabezado)
    print("-" * len(encabezado))

    for clave in claves:
        anio, sector = clave.split("|", 1)
        mr_r, mr_m = mr_res.get(clave, (0, 0))
        sp_r, sp_m = sp_res.get(clave, (0, 0))
        ok = (mr_r == sp_r) and (mr_m == sp_m)
        if not ok:
            diferencias += 1
        nombre_sec = NOMBRES_SECTOR.get(sector, sector)
        estado = "OK" if ok else "DIFERENCIA"
        print(
            f"{anio:<6} {nombre_sec:<10} | {mr_r:>10,} {sp_r:>10,} | "
            f"{mr_m:>12,} {sp_m:>13,} | {estado}"
        )
        filas_reporte.append(
            {
                "anio": anio,
                "id_sector": sector,
                "sector": nombre_sec,
                "mapreduce_registros": mr_r,
                "spark_registros": sp_r,
                "mapreduce_matriculados": mr_m,
                "spark_matriculados": sp_m,
                "coincide": ok,
            }
        )

    tot_mr_r = sum(v[0] for v in mr_res.values())
    tot_mr_m = sum(v[1] for v in mr_res.values())
    tot_sp_r = sum(v[0] for v in sp_res.values())
    tot_sp_m = sum(v[1] for v in sp_res.values())
    print("-" * len(encabezado))
    print(
        f"{'TOTAL':<17} | {tot_mr_r:>10,} {tot_sp_r:>10,} | "
        f"{tot_mr_m:>12,} {tot_sp_m:>13,} | "
        f"{'0 DIFERENCIAS (EXACTO)' if diferencias == 0 else f'{diferencias} DIFERENCIAS'}"
    )

    ruta_out = Path(args.salida_evidencia)
    ruta_out.parent.mkdir(parents=True, exist_ok=True)
    ruta_out.write_text(
        json.dumps(
            {
                "origen": patron,
                "total_mapreduce_registros": tot_mr_r,
                "total_spark_registros": tot_sp_r,
                "total_mapreduce_matriculados": tot_mr_m,
                "total_spark_matriculados": tot_sp_m,
                "diferencias": diferencias,
                "coincidencia_exacta": diferencias == 0,
                "detalle": filas_reporte,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nEvidencia guardada en {ruta_out}")
    return 0 if diferencias == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

