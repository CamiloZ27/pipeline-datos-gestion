"""
Fase 5 — Transformaciones de la zona raw (JSONL) a la zona curada (Parquet).

Decisiones de limpieza documentadas (aplicadas sobre el dataset real 5wck-szir):
  1. Deduplicación por `id_registro` con `Window + row_number()` conservando
     la ingesta más reciente (`fecha_ingesta DESC`). Elimina tanto re-ingestas
     como las ~160.000 filas duplicadas 2x presentes en la fuente oficial 2015-2019.
  2. Tipado seguro con `try_cast` (Spark 4 activa modo ANSI): convierte texto a
     enteros y fechas sin abortar por valores anómalos como `id_area = "Sin clasificar"`.
  3. Renombrado semántico: `matriculados_2015` -> `total_matriculados` (INT) y
     creación de `fecha_periodo` (DATE: YYYY-01-01 semestre 1, YYYY-07-01 semestre 2).
  4. Normalización de códigos DANE con ceros a la izquierda (2 dígitos depto, 5 municipio):
     corrige la inconsistencia entre 2015-2019 ('5', '5001') y 2020-2021 ('05', '05001').
  5. Catálogo oficial SNIES + DANE (mapas in-memory): colapsa las 90 variantes
     sucias de departamentos ('BOGOTA D.C', 'NARINIO', 'ARCHIPIÉLAGO DE SA') a
     33 departamentos oficiales y traduce los códigos numéricos a nombres claros.
  6. Limpieza de nulos / valores inválidos: se descartan filas sin `fecha_periodo`,
     sin `codigo_ies`/`codigo_snies`, o con `total_matriculados <= 0`, registrando
     cada descarte en el reporte de calidad.
"""
from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from procesamiento import catalogo

ESQUEMA_SOBRE = T.StructType(
    [
        T.StructField("fuente", T.StringType()),
        T.StructField("dataset", T.StringType()),
        T.StructField("id_registro", T.StringType()),
        T.StructField("fecha_ingesta", T.StringType()),
        T.StructField("payload", T.StringType()),
    ]
)

ESQUEMA_PAYLOAD = T.StructType(
    [
        T.StructField("c_digo_de_la_instituci_n", T.StringType()),
        T.StructField("ies_padre", T.StringType()),
        T.StructField("instituci_n_de_educaci_n_superior_ies", T.StringType()),
        T.StructField("principal_oseccional", T.StringType()),
        T.StructField("id_sector", T.StringType()),
        T.StructField("id_caracter", T.StringType()),
        T.StructField("c_digo_del_departamento_ies", T.StringType()),
        T.StructField("departamento_de_domicilio_de_la_ies", T.StringType()),
        T.StructField("c_digo_del_municipio_ies", T.StringType()),
        T.StructField("municipio_dedomicilio_de_la_ies", T.StringType()),
        T.StructField("c_digo_snies_delprograma", T.StringType()),
        T.StructField("programa_acad_mico", T.StringType()),
        T.StructField("id_nivel", T.StringType()),
        T.StructField("id_nivel_formacion", T.StringType()),
        T.StructField("id_metodologia", T.StringType()),
        T.StructField("id_area", T.StringType()),
        T.StructField("id_nucleo", T.StringType()),
        T.StructField("n_cleo_b_sico_del_conocimiento_nbc", T.StringType()),
        T.StructField("c_digo_del_departamento_programa", T.StringType()),
        T.StructField("departamento_de_oferta_del_programa", T.StringType()),
        T.StructField("c_digo_del_municipio_programa", T.StringType()),
        T.StructField("municipio_de_oferta_del_programa", T.StringType()),
        T.StructField("id_g_nero", T.StringType()),
        T.StructField("a_o", T.StringType()),
        T.StructField("semestre", T.StringType()),
        T.StructField("matriculados_2015", T.StringType()),
    ]
)

COLUMNAS_CURADAS = [
    "id_registro",
    "fecha_periodo",
    "anio",
    "semestre",
    "codigo_ies",
    "ies_padre",
    "nombre_ies",
    "sede_tipo",
    "id_sector",
    "sector_nombre",
    "id_caracter",
    "caracter_nombre",
    "cod_depto_ies",
    "departamento_ies",
    "cod_muni_ies",
    "municipio_ies",
    "codigo_snies",
    "programa_academico",
    "id_nivel",
    "nivel_nombre",
    "id_nivel_formacion",
    "nivel_formacion_nombre",
    "id_metodologia",
    "metodologia_nombre",
    "id_area",
    "area_nombre",
    "nbc_nombre",
    "cod_depto_oferta",
    "departamento_oferta",
    "cod_muni_oferta",
    "municipio_oferta",
    "id_genero",
    "genero_nombre",
    "total_matriculados",
    "fuente",
    "dataset",
    "fecha_ingesta",
]


def _mapa_spark(diccionario: dict):
    """Convierte un dict Python en expresión MAP nativa de Spark (sin shuffle)."""
    return F.create_map(*[F.lit(x) for par in diccionario.items() for x in par])


def _cod_dane(col_expr, ancho: int):
    """Normaliza código DANE ('5' -> '05', '5001' -> '05001') usando try_cast."""
    num = F.expr(f"try_cast(trim({col_expr}) AS INT)")
    return F.when(num.isNotNull(), F.lpad(num.cast("string"), ancho, "0"))


def leer_sobres(lineas: DataFrame) -> DataFrame:
    """Parsea líneas JSONL crudas al esquema del sobre y descarta líneas corruptas."""
    return (
        lineas.select(F.from_json("value", ESQUEMA_SOBRE).alias("s"))
        .select("s.*")
        .where(F.col("fuente").isNotNull() & F.col("id_registro").isNotNull())
        .withColumn(
            "fecha_ingesta",
            F.expr("try_to_timestamp(fecha_ingesta, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
        )
    )


def deduplicar(sobres: DataFrame) -> DataFrame:
    """Conserva una sola fila por `id_registro` (la de `fecha_ingesta` más reciente)."""
    ventana = Window.partitionBy("id_registro").orderBy(F.col("fecha_ingesta").desc_nulls_last())
    return (
        sobres.withColumn("_rn", F.row_number().over(ventana))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def normalizar_matricula(sobres_unicos: DataFrame) -> DataFrame:
    """Parsea el payload SNIES, corrige tipos, normaliza textos y aplica catálogos."""
    m_depto = _mapa_spark(catalogo.DEPARTAMENTOS_DANE)
    m_sector = _mapa_spark(catalogo.SECTORES)
    m_caracter = _mapa_spark(catalogo.CARACTERES)
    m_nivel = _mapa_spark(catalogo.NIVELES)
    m_formacion = _mapa_spark(catalogo.NIVELES_FORMACION)
    m_metodologia = _mapa_spark(catalogo.METODOLOGIAS)
    m_area = _mapa_spark(catalogo.AREAS_CONOCIMIENTO)
    m_genero = _mapa_spark(catalogo.GENEROS)

    df = sobres_unicos.withColumn("p", F.from_json("payload", ESQUEMA_PAYLOAD))

    anio = F.expr("try_cast(trim(p.a_o) AS INT)")
    semestre = F.expr("try_cast(trim(p.semestre) AS INT)")
    fecha_periodo = F.when(
        anio.isNotNull() & semestre.isin(1, 2),
        F.make_date(anio, F.when(semestre == 1, F.lit(1)).otherwise(F.lit(7)), F.lit(1)),
    )

    id_sector = F.expr("try_cast(trim(p.id_sector) AS INT)")
    id_caracter = F.expr("try_cast(trim(p.id_caracter) AS INT)")
    id_nivel = F.expr("try_cast(trim(p.id_nivel) AS INT)")
    id_nivel_form = F.expr("try_cast(trim(p.id_nivel_formacion) AS INT)")
    id_metodologia = F.expr("try_cast(trim(p.id_metodologia) AS INT)")
    # id_area contiene texto "Sin clasificar" en 192 filas -> try_cast a INT + coalesce(0)
    id_area = F.coalesce(F.expr("try_cast(trim(p.id_area) AS INT)"), F.lit(0))
    id_genero = F.expr("try_cast(trim(p.id_g_nero) AS INT)")
    total_mat = F.expr("try_cast(try_cast(trim(p.matriculados_2015) AS DOUBLE) AS INT)")

    cod_depto_ies = _cod_dane("p.c_digo_del_departamento_ies", 2)
    cod_muni_ies = _cod_dane("p.c_digo_del_municipio_ies", 5)
    cod_depto_of = _cod_dane("p.c_digo_del_departamento_programa", 2)
    cod_muni_of = _cod_dane("p.c_digo_del_municipio_programa", 5)

    sede_limpia = F.when(
        F.upper(F.trim(F.col("p.principal_oseccional"))).startswith("PRIN"),
        F.lit("Principal"),
    ).when(
        F.upper(F.trim(F.col("p.principal_oseccional"))).startswith("SEC"),
        F.lit("Seccional"),
    ).otherwise(F.coalesce(F.initcap(F.trim(F.col("p.principal_oseccional"))), F.lit("Sin especificar")))

    return df.select(
        F.col("id_registro"),
        fecha_periodo.alias("fecha_periodo"),
        anio.alias("anio"),
        semestre.alias("semestre"),
        F.expr("try_cast(trim(p.c_digo_de_la_instituci_n) AS INT)").alias("codigo_ies"),
        F.expr("try_cast(trim(p.ies_padre) AS INT)").alias("ies_padre"),
        F.regexp_replace(F.trim(F.col("p.instituci_n_de_educaci_n_superior_ies")), r"\s+", " ").alias("nombre_ies"),
        sede_limpia.alias("sede_tipo"),
        id_sector.alias("id_sector"),
        F.coalesce(m_sector[id_sector], F.lit("Sin especificar")).alias("sector_nombre"),
        id_caracter.alias("id_caracter"),
        F.coalesce(m_caracter[id_caracter], F.lit("Sin especificar")).alias("caracter_nombre"),
        cod_depto_ies.alias("cod_depto_ies"),
        F.coalesce(
            m_depto[cod_depto_ies],
            F.initcap(F.trim(F.col("p.departamento_de_domicilio_de_la_ies"))),
            F.lit("Sin especificar"),
        ).alias("departamento_ies"),
        cod_muni_ies.alias("cod_muni_ies"),
        F.coalesce(
            F.initcap(F.trim(F.col("p.municipio_dedomicilio_de_la_ies"))),
            F.lit("Sin especificar"),
        ).alias("municipio_ies"),
        F.expr("try_cast(trim(p.c_digo_snies_delprograma) AS INT)").alias("codigo_snies"),
        F.regexp_replace(F.trim(F.col("p.programa_acad_mico")), r"\s+", " ").alias("programa_academico"),
        id_nivel.alias("id_nivel"),
        F.coalesce(m_nivel[id_nivel], F.lit("Sin especificar")).alias("nivel_nombre"),
        id_nivel_form.alias("id_nivel_formacion"),
        F.coalesce(m_formacion[id_nivel_form], F.lit("Sin especificar")).alias("nivel_formacion_nombre"),
        id_metodologia.alias("id_metodologia"),
        F.coalesce(m_metodologia[id_metodologia], F.lit("Otra metodología")).alias("metodologia_nombre"),
        id_area.alias("id_area"),
        F.coalesce(m_area[id_area], F.lit("Sin clasificar")).alias("area_nombre"),
        F.coalesce(
            F.regexp_replace(F.initcap(F.trim(F.col("p.n_cleo_b_sico_del_conocimiento_nbc"))), r"\s+", " "),
            F.lit("Sin clasificar"),
        ).alias("nbc_nombre"),
        cod_depto_of.alias("cod_depto_oferta"),
        F.coalesce(
            m_depto[cod_depto_of],
            F.initcap(F.trim(F.col("p.departamento_de_oferta_del_programa"))),
            F.lit("Sin especificar"),
        ).alias("departamento_oferta"),
        cod_muni_of.alias("cod_muni_oferta"),
        F.coalesce(
            F.initcap(F.trim(F.col("p.municipio_de_oferta_del_programa"))),
            F.lit("Sin especificar"),
        ).alias("municipio_oferta"),
        id_genero.alias("id_genero"),
        F.coalesce(m_genero[id_genero], F.lit("Sin especificar")).alias("genero_nombre"),
        total_mat.alias("total_matriculados"),
        F.col("fuente"),
        F.col("dataset"),
        F.col("fecha_ingesta"),
    ).select(*COLUMNAS_CURADAS)


def filtrar_validos(normalizado: DataFrame) -> DataFrame:
    """Conserva únicamente registros con columnas clave válidas y matriculados > 0."""
    return normalizado.where(
        F.col("id_registro").isNotNull()
        & F.col("fecha_periodo").isNotNull()
        & F.col("codigo_ies").isNotNull()
        & F.col("codigo_snies").isNotNull()
        & F.col("cod_depto_oferta").isNotNull()
        & F.col("total_matriculados").isNotNull()
        & (F.col("total_matriculados") > 0)
    )


def curar(lineas: DataFrame) -> Dict[str, DataFrame]:
    """Ejecuta todas las etapas de limpieza y retorna los DataFrames intermedios
    para medir el antes y después de cada decisión."""
    sobres = leer_sobres(lineas)
    unicos = deduplicar(sobres)
    normalizado = normalizar_matricula(unicos)
    valido = filtrar_validos(normalizado)
    return {
        "sobres": sobres,
        "unicos": unicos,
        "normalizado": normalizado,
        "curado": valido,
    }


def crear_sesion(nombre: str = "curar-matricula-es", master: Optional[str] = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(nombre)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()

