"""
Fases 5 y 6 — Trabajo Spark: zona raw (HDFS) -> zona curada (Parquet particionado).

Ejecución con Docker:
    docker compose run --rm spark

Qué hace:
  1. Lee todos los .jsonl de `/datalake/raw/*/*/fecha_ingesta=*/*.jsonl`.
  2. Deduplica por `id_registro` (corrige duplicación 2x de 2015-2019 y re-ingestas).
  3. Corrige tipos, normaliza 90 variantes de departamentos a 33 DANE y aplica
     catálogos oficiales SNIES.
  4. Ejecuta 5 validaciones automáticas de calidad; si una crítica falla, DETIENE
     el pipeline sin escribir en la zona curada.
  5. Escribe Parquet particionado por `anio` (`anio=2015`, ..., `anio=2021`).
     Justificación de partición por `anio`:
       - Cardinalidad ideal (7 particiones balanceadas, ~30k filas c/u).
       - Evita el problema de archivos pequeños (a diferencia de particionar por
         municipio o programa) y acelera filtros temporales y consultas en Hive/Spark.
  6. Compara el tamaño en bytes de JSONL (raw) vs. Parquet (curada) y guarda el
     reporte de calidad en `evidencias/reporte_calidad.json`.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from procesamiento import calidad, transformaciones as tr

HDFS = "hdfs://namenode:8020"


def _tamano_ruta_spark(spark, ruta: str) -> int:
    """Obtiene el tamaño total en bytes de una ruta HDFS o local vía Hadoop FileSystem."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(ruta)
    fs = path.getFileSystem(conf)
    if not fs.exists(path):
        return 0
    return int(fs.getContentSummary(path).getLength())


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Fases 5-6: raw -> curada Parquet + calidad")
    parser.add_argument("--origen", default=f"{HDFS}/datalake/raw")
    parser.add_argument("--destino", default=f"{HDFS}/datalake/curated/matricula_es")
    parser.add_argument("--reporte", default="evidencias/reporte_calidad.json")
    parser.add_argument("--master", default=None)
    args = parser.parse_args(argv)

    inicio = time.time()
    spark = tr.crear_sesion("curar-matricula-es", master=args.master)
    spark.sparkContext.setLogLevel("ERROR")

    patron = f"{args.origen}/*/*/fecha_ingesta=*/*.jsonl"
    print(f"\n[Fase 5] Leyendo zona raw: {patron}")
    lineas = spark.read.text(patron)

    etapas = tr.curar(lineas)
    etapas["sobres"].cache()
    etapas["curado"].cache()

    print("[Fase 6] Ejecutando validaciones automáticas de calidad...")
    reporte = calidad.validar_y_guardar(etapas, ruta_reporte=Path(args.reporte))

    # Escritura en Parquet particionado por anio (1 archivo por año con repartition)
    print(f"[Fase 5] Escribiendo zona curada en Parquet particionado por anio -> {args.destino}")
    (
        etapas["curado"]
        .repartition("anio")
        .write.mode("overwrite")
        .partitionBy("anio")
        .parquet(args.destino)
    )

    # Comparación de tamaño JSONL vs Parquet
    bytes_raw = _tamano_ruta_spark(spark, args.origen)
    bytes_parquet = _tamano_ruta_spark(spark, args.destino)
    ratio = round(bytes_raw / bytes_parquet, 2) if bytes_parquet else 0.0

    reporte["almacenamiento"] = {
        "origen_raw": args.origen,
        "destino_curado": args.destino,
        "columna_particion": "anio",
        "bytes_raw_jsonl": bytes_raw,
        "mb_raw_jsonl": round(bytes_raw / (1024 * 1024), 2),
        "bytes_curado_parquet": bytes_parquet,
        "mb_curado_parquet": round(bytes_parquet / (1024 * 1024), 2),
        "factor_reduccion_x": ratio,
        "reduccion_pct": round((1 - bytes_parquet / bytes_raw) * 100, 1) if bytes_raw else 0.0,
    }
    Path(args.reporte).write_text(json.dumps(reporte, ensure_ascii=False, indent=2), encoding="utf-8")

    # Impresión limpia para consola y evidencias
    r = reporte["resumen"]
    print("\n================ REPORTE DE CALIDAD (FASES 5 y 6) ================")
    print(f"Estado general:         {reporte['estado']}")
    print(f"Filas raw leídas:       {r['filas_raw_leidas']:,}")
    print(f"Duplicados eliminados:  {r['duplicados_eliminados']:,}")
    print(f"Registros descartados:  {r['registros_descartados']:,}")
    print(f"Filas curadas finales:  {r['filas_curadas']:,}")
    print("\n--- Antes y después por año ---")
    for row in reporte["antes_despues_por_anio"]:
        print(
            f"  {row['anio']}: raw={row['filas_raw']:>6,} -> curado={row['filas_curadas']:>6,} "
            f"(eliminados={row['duplicados_y_descartes']:>6,}) | matriculados={row['total_matriculados_curados']:>10,}"
        )
    print("\n--- Validaciones automáticas ---")
    for v in reporte["validaciones"]:
        marca = "OK" if v["aprobada"] else "FALLO"
        print(f"  [{marca}] {v['nombre']}: {v['detalle']}")
    print("\n--- Comparación de tamaño JSONL vs. Parquet ---")
    a = reporte["almacenamiento"]
    print(
        f"  Raw JSONL:      {a['mb_raw_jsonl']:.2f} MB ({a['bytes_raw_jsonl']:,} B)\n"
        f"  Curado Parquet: {a['mb_curado_parquet']:.2f} MB ({a['bytes_curado_parquet']:,} B)\n"
        f"  Compresión:     {a['factor_reduccion_x']}x menos ({a['reduccion_pct']} % ahorro)"
    )
    print(f"\nReporte guardado en {args.reporte} (duración: {time.time() - inicio:.1f} s)")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

