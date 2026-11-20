"""
Fase 6 — Validaciones automáticas de calidad de datos, reporte y fail-fast.

Programa 5 validaciones automáticas sobre la zona curada:
  1. [CRÍTICA] conteo_mayor_cero: debe haber al menos 1 registro curado.
  2. [CRÍTICA] sin_nulos_columnas_clave: 0 nulos en columnas clave del modelo.
  3. [CRÍTICA] sin_id_registro_duplicados: unicidad estricta de `id_registro`.
  4. [CRÍTICA] rangos_validos: `total_matriculados > 0`, `anio` en [2015, 2030],
     `semestre` en {1, 2}.
  5. [CRÍTICA] departamentos_dane_canonicos: todos los departamentos normalizados
     pertenecen al catálogo oficial de 33 departamentos DANE.

Si cualquiera de las validaciones críticas falla, el reporte se guarda con estado
`FALLIDO` y se lanza `ErrorCalidadCritica`, deteniendo el pipeline antes de
sobrescribir la zona curada o la zona de servicio.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from procesamiento import catalogo


class ErrorCalidadCritica(RuntimeError):
    """Se lanza cuando falla al menos una validación crítica de calidad."""


COLUMNAS_CLAVE = [
    "id_registro",
    "fecha_periodo",
    "anio",
    "semestre",
    "codigo_ies",
    "codigo_snies",
    "departamento_oferta",
    "total_matriculados",
]


def ejecutar_validaciones(etapas: Dict[str, DataFrame]) -> dict:
    """Evalúa las 5 reglas de calidad y construye el reporte auditable."""
    sobres = etapas["sobres"]
    unicos = etapas["unicos"]
    normalizado = etapas["normalizado"]
    curado = etapas["curado"]

    n_leidos = sobres.count()
    n_unicos = unicos.count()
    n_duplicados = n_leidos - n_unicos
    n_curado = curado.count()
    n_descartados = n_unicos - n_curado

    # Desglose exacto de descartes sobre los registros únicos
    descartes_row = normalizado.agg(
        F.sum(F.col("fecha_periodo").isNull().cast("int")).alias("sin_fecha_periodo"),
        F.sum(F.col("codigo_ies").isNull().cast("int")).alias("sin_codigo_ies"),
        F.sum(F.col("codigo_snies").isNull().cast("int")).alias("sin_codigo_snies"),
        F.sum(F.col("cod_depto_oferta").isNull().cast("int")).alias("sin_depto_oferta"),
        F.sum(
            (F.col("total_matriculados").isNull() | (F.col("total_matriculados") <= 0)).cast("int")
        ).alias("matriculados_invalidos"),
    ).collect()[0]

    # Conteo antes y después por año (evidencia clara de la deduplicación 2015-2019)
    raw_anio = (
        sobres.select(F.get_json_object("payload", "$.a_o").alias("anio_raw"))
        .groupBy("anio_raw")
        .count()
        .collect()
    )
    cur_anio = curado.groupBy("anio").agg(
        F.count("*").alias("filas_curadas"),
        F.sum("total_matriculados").alias("matriculados"),
    ).collect()
    mapa_raw_anio = {str(r["anio_raw"]): int(r["count"]) for r in raw_anio}
    antes_despues_anio = []
    for r in sorted(cur_anio, key=lambda x: x["anio"]):
        a_str = str(r["anio"])
        raw_n = mapa_raw_anio.get(a_str, 0)
        cur_n = int(r["filas_curadas"])
        antes_despues_anio.append(
            {
                "anio": int(r["anio"]),
                "filas_raw": raw_n,
                "filas_curadas": cur_n,
                "duplicados_y_descartes": raw_n - cur_n,
                "total_matriculados_curados": int(r["matriculados"]),
            }
        )

    # Validación 1: conteo > 0
    v1_ok = n_curado > 0

    # Validación 2: 0 nulos en columnas clave
    nulos_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in COLUMNAS_CLAVE]
    nulos_row = curado.agg(*nulos_exprs).collect()[0].asDict()
    total_nulos_clave = sum(int(v or 0) for v in nulos_row.values())
    v2_ok = total_nulos_clave == 0

    # Validación 3: 0 duplicados en id_registro
    n_ids_distintos = curado.select("id_registro").distinct().count()
    duplicados_remanentes = n_curado - n_ids_distintos
    v3_ok = duplicados_remanentes == 0

    # Validación 4: rangos válidos
    fuera_rango = curado.where(
        (F.col("total_matriculados") <= 0)
        | (F.col("total_matriculados") > 100000)
        | (~F.col("anio").between(2015, 2030))
        | (~F.col("semestre").isin(1, 2))
    ).count()
    v4_ok = fuera_rango == 0

    # Validación 5: departamentos canónicos DANE (sin variantes sucias)
    deptos_validos = list(catalogo.DEPARTAMENTOS_DANE.values())
    deptos_fuera = curado.where(~F.col("departamento_oferta").isin(deptos_validos)).count()
    n_deptos_unicos = curado.select("departamento_oferta").distinct().count()
    v5_ok = deptos_fuera == 0

    validaciones: List[dict] = [
        {
            "nombre": "conteo_mayor_cero",
            "critica": True,
            "aprobada": v1_ok,
            "detalle": f"Filas curadas = {n_curado:,}",
        },
        {
            "nombre": "sin_nulos_columnas_clave",
            "critica": True,
            "aprobada": v2_ok,
            "detalle": f"Nulos totales en columnas clave = {total_nulos_clave}",
            "nulos_por_columna": {k: int(v or 0) for k, v in nulos_row.items()},
        },
        {
            "nombre": "sin_id_registro_duplicados",
            "critica": True,
            "aprobada": v3_ok,
            "detalle": f"Duplicados remanentes = {duplicados_remanentes} (IDs únicos = {n_ids_distintos:,})",
        },
        {
            "nombre": "rangos_validos",
            "critica": True,
            "aprobada": v4_ok,
            "detalle": f"Registros fuera de rango (matriculados/año/semestre) = {fuera_rango}",
        },
        {
            "nombre": "departamentos_dane_canonicos",
            "critica": True,
            "aprobada": v5_ok,
            "detalle": (
                f"Departamentos distintos = {n_deptos_unicos} (máx. 33 DANE), "
                f"registros fuera de catálogo = {deptos_fuera}"
            ),
        },
    ]

    fallos_criticos = [v["nombre"] for v in validaciones if v["critica"] and not v["aprobada"]]

    return {
        "generado_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "estado": "APROBADO" if not fallos_criticos else "FALLIDO",
        "fallos_criticos": fallos_criticos,
        "resumen": {
            "filas_raw_leidas": n_leidos,
            "duplicados_eliminados": n_duplicados,
            "filas_unicas": n_unicos,
            "registros_descartados": n_descartados,
            "filas_curadas": n_curado,
            "descarte_detalle": {k: int(v or 0) for k, v in descartes_row.asDict().items()},
        },
        "antes_despues_por_anio": antes_despues_anio,
        "validaciones": validaciones,
    }


def validar_y_guardar(
    etapas: Dict[str, DataFrame],
    ruta_reporte: Optional[Path] = None,
    extras: Optional[dict] = None,
) -> dict:
    """Ejecuta las validaciones, guarda el reporte JSON y detiene el pipeline si
    alguna validación crítica falla."""
    reporte = ejecutar_validaciones(etapas)
    if extras:
        reporte.update(extras)

    if ruta_reporte is not None:
        ruta_reporte = Path(ruta_reporte)
        ruta_reporte.parent.mkdir(parents=True, exist_ok=True)
        ruta_reporte.write_text(
            json.dumps(reporte, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if reporte["fallos_criticos"]:
        raise ErrorCalidadCritica(
            "Validación crítica de calidad fallida; pipeline detenido: "
            + ", ".join(reporte["fallos_criticos"])
        )
    return reporte

