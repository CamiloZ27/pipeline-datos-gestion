"""Pruebas unitarias offline de la Fase 4a (WebHDFS y consumidor por lotes)."""
from __future__ import annotations

from collections import namedtuple

from datalake import consumidor_hdfs
from datalake.webhdfs import WebHDFS

TP = namedtuple("TP", ["topic", "partition"])
Msg = namedtuple("Msg", ["offset", "value"])


class HDFSFalso:
    def __init__(self):
        self.carpetas = []
        self.archivos = {}

    def mkdirs(self, ruta):
        self.carpetas.append(ruta)
        return True

    def escribir(self, ruta, contenido, sobrescribir=True):
        self.archivos[ruta] = contenido


def test_traduccion_datanode_webhdfs():
    cliente = WebHDFS(url="http://localhost:9870", datanode_publico="localhost:9864")
    traducida = cliente._traducir_datanode(
        "http://datanode:9864/webhdfs/v1/datalake/raw/test.jsonl?op=CREATE"
    )
    assert traducida.startswith("http://localhost:9864/webhdfs/v1/")


def test_agrupar_lote_genera_un_archivo_por_lote_y_no_por_mensaje():
    tp = TP("raw.datos_gov_co", 0)
    lote = {
        tp: [
            Msg(10, {"fuente": "datos_gov_co", "dataset": "men_matricula_es", "fecha_ingesta": "2026-11-20T12:00:00Z", "id_registro": "a", "payload": {}}),
            Msg(11, {"fuente": "datos_gov_co", "dataset": "men_matricula_es", "fecha_ingesta": "2026-11-20T12:00:00Z", "id_registro": "b", "payload": {}}),
            Msg(12, {"fuente": "datos_gov_co", "dataset": "men_matricula_es", "fecha_ingesta": "2026-11-20T12:00:00Z", "id_registro": "c", "payload": {}}),
        ]
    }
    archivos = consumidor_hdfs.agrupar_lote(lote)
    assert len(archivos) == 1
    ruta = next(iter(archivos))
    assert "fecha_ingesta=2026-11-20/part-raw.datos_gov_co-p0-o000000000010.jsonl" in ruta
    assert len(archivos[ruta]) == 3

    hdfs = HDFSFalso()
    n = consumidor_hdfs.escribir_lote(hdfs, archivos)
    assert n == 3
    assert len(hdfs.archivos[ruta].decode("utf-8").splitlines()) == 3

