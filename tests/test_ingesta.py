"""Pruebas unitarias offline de la Fase 2 (ingesta SODA2, paginación y marca de agua)."""
from __future__ import annotations

import json
from pathlib import Path

from ingesta import comun, matricula


class RespuestaFalsa:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class SesionFalsa:
    def __init__(self, paginas):
        self.paginas = list(paginas)
        self.llamadas = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.llamadas.append({"url": url, "params": dict(params or {})})
        return RespuestaFalsa(self.paginas.pop(0) if self.paginas else [])


def _fila(anio="2021", sem="1", ies="3107", snies="2076", muni="05001", met="1", gen="1", mat="45"):
    return {
        "a_o": anio,
        "semestre": sem,
        "c_digo_de_la_instituci_n": ies,
        "c_digo_snies_delprograma": snies,
        "c_digo_del_municipio_programa": muni,
        "id_metodologia": met,
        "id_g_nero": gen,
        "matriculados_2015": mat,
    }


def test_id_registro_normaliza_ceros_sin_alterar_payload():
    f_2015 = _fila(anio="2015", muni="5001")
    f_2021 = _fila(anio="2015", muni="05001")
    assert matricula.construir_id_registro(f_2015) == matricula.construir_id_registro(f_2021)
    assert f_2015["c_digo_del_municipio_programa"] == "5001"  # payload intacto


def test_paginacion_y_manifiesto_y_marca_agua(tmp_path: Path):
    sesion = SesionFalsa(
        [
            [_fila(anio="2020", sem="1", snies="1001"), _fila(anio="2020", sem="2", snies="1002")],
            [_fila(anio="2021", sem="1", snies="1003")],
        ]
    )
    wm = tmp_path / "_marca_agua.json"
    archivo = matricula.ejecutar(
        tamano_pagina=2,
        sesion=sesion,
        raw_dir=tmp_path / "raw",
        watermark_path=wm,
    )
    assert archivo is not None and archivo.exists()
    assert len(sesion.llamadas) == 2
    assert sesion.llamadas[0]["params"]["$offset"] == 0
    assert sesion.llamadas[1]["params"]["$offset"] == 2

    lineas = [json.loads(l) for l in archivo.read_text(encoding="utf-8").splitlines()]
    assert len(lineas) == 3
    assert lineas[0]["fuente"] == "datos_gov_co"
    assert lineas[0]["payload"]["c_digo_snies_delprograma"] == "1001"

    manifiestos = list(archivo.parent.glob("_manifiesto_*.json"))
    assert len(manifiestos) == 1
    manifiesto = json.loads(manifiestos[0].read_text(encoding="utf-8"))
    assert manifiesto["registros"] == 3

    marca = comun.leer_marca_agua(wm)
    assert marca["ultimo_anio"] == 2021
    assert marca["ultimo_semestre"] == 1
    assert marca["ultima_fecha_periodo"] == "2021-01-01"


def test_carga_incremental_usa_marca_de_agua(tmp_path: Path):
    wm = tmp_path / "_marca_agua.json"
    comun.guardar_marca_agua(2020, 2, "2020-07-01", "2026-11-20T12:00:00Z", 10, ruta=wm)
    sesion = SesionFalsa([[]])

    res = matricula.ejecutar(
        incremental=True,
        sesion=sesion,
        raw_dir=tmp_path / "raw",
        watermark_path=wm,
    )
    assert res is None
    assert "a_o > 2020 OR (a_o = 2020 AND semestre > 2)" in sesion.llamadas[0]["params"]["$where"]

