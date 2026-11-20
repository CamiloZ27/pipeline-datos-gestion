"""Pruebas con PySpark real (Fases 5 y 6): deduplicación, limpieza DANE/SNIES y fail-fast."""
from __future__ import annotations

import json
import pytest

pyspark = pytest.importorskip("pyspark")

from procesamiento import calidad, transformaciones as tr


@pytest.fixture(scope="module")
def spark():
    s = tr.crear_sesion("tests-matricula-es", master="local[1]")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _sobre(id_reg, fecha_ing, payload):
    return json.dumps(
        {
            "fuente": "datos_gov_co",
            "dataset": "men_matricula_es",
            "id_registro": id_reg,
            "fecha_ingesta": fecha_ing,
            "payload": payload,
        },
        ensure_ascii=False,
    )


def test_deduplicacion_y_normalizacion_90_a_33_deptos_y_area_sin_clasificar(spark):
    payload_base = {
        "c_digo_de_la_instituci_n": "3107",
        "ies_padre": "3107",
        "instituci_n_de_educaci_n_superior_ies": "INSTITUCIÓN UNIVERSITARIA PASCUAL BRAVO",
        "principal_oseccional": "PRINCIPAL",
        "id_sector": "1",
        "id_caracter": "3",
        "c_digo_del_departamento_ies": "5",
        "departamento_de_domicilio_de_la_ies": "ANTIOQUIA",
        "c_digo_del_municipio_ies": "5001",
        "municipio_dedomicilio_de_la_ies": "MEDELLIN",
        "c_digo_snies_delprograma": "2076",
        "programa_acad_mico": "TECNOLOGIA ELECTRICA",
        "id_nivel": "1",
        "id_nivel_formacion": "5",
        "id_metodologia": "1",
        "id_area": "Sin clasificar",  # caso real que rompería cast ANSI
        "id_nucleo": "828",
        "n_cleo_b_sico_del_conocimiento_nbc": "INGENIERIA ELECTRICA Y AFINES",
        "c_digo_del_departamento_programa": "88",
        "departamento_de_oferta_del_programa": "ARCHIPIÉLAGO DE SA",  # variante truncada real
        "c_digo_del_municipio_programa": "88001",
        "municipio_de_oferta_del_programa": "SAN ANDRES",
        "id_g_nero": "1",
        "a_o": "2015",
        "semestre": "1",
        "matriculados_2015": "465",
    }
    lineas = [
        _sobre("2015|1|3107|2076|88001|1|1", "2026-11-20T10:00:00Z", payload_base),
        _sobre("2015|1|3107|2076|88001|1|1", "2026-11-20T12:00:00Z", payload_base),  # duplicado
    ]
    df_txt = spark.createDataFrame([(l,) for l in lineas], ["value"])
    etapas = tr.curar(df_txt)
    reporte = calidad.ejecutar_validaciones(etapas)

    assert reporte["estado"] == "APROBADO"
    assert reporte["resumen"]["filas_raw_leidas"] == 2
    assert reporte["resumen"]["duplicados_eliminados"] == 1
    assert reporte["resumen"]["filas_curadas"] == 1

    fila = etapas["curado"].collect()[0]
    assert fila["departamento_ies"] == "Antioquia"
    assert fila["departamento_oferta"] == "San Andrés y Providencia"
    assert fila["area_nombre"] == "Sin clasificar"
    assert fila["total_matriculados"] == 465
    assert str(fila["fecha_periodo"]) == "2015-01-01"


def test_validacion_critica_detiene_pipeline_si_dataset_vacio(spark, tmp_path):
    df_txt = spark.createDataFrame([("{}",)], ["value"])
    etapas = tr.curar(df_txt)
    with pytest.raises(calidad.ErrorCalidadCritica):
        calidad.validar_y_guardar(etapas, ruta_reporte=tmp_path / "reporte_fallido.json")
    assert (tmp_path / "reporte_fallido.json").exists()

