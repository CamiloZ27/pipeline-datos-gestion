"""Pruebas unitarias offline de la Fase 3 (Kafka productor + checkpoint .kafka_ok)."""
from __future__ import annotations

import json
from pathlib import Path

from streaming import productor


class FuturoFalso:
    def add_errback(self, fn):
        return self


class ProductorFalso:
    def __init__(self):
        self.enviados = []
        self.flushed = 0

    def send(self, topic, key=None, value=None, headers=None):
        self.enviados.append({"topic": topic, "key": key, "value": value, "headers": headers})
        return FuturoFalso()

    def flush(self):
        self.flushed += 1

    def close(self):
        pass


def test_productor_usa_id_registro_como_key_y_no_duplica_al_recorrer(tmp_path: Path):
    carpeta = tmp_path / "datos_gov_co" / "men_matricula_es" / "fecha_ingesta=2026-11-20"
    carpeta.mkdir(parents=True)
    archivo = carpeta / "men_matricula_es_20261120T120000Z.jsonl"
    sobres = [
        {
            "fuente": "datos_gov_co",
            "dataset": "men_matricula_es",
            "id_registro": "2021|1|3107|2076|05001|1|1",
            "fecha_ingesta": "2026-11-20T12:00:00Z",
            "payload": {"a_o": "2021", "matriculados_2015": "100"},
        },
        {
            "fuente": "datos_gov_co",
            "dataset": "men_matricula_es",
            "id_registro": "2021|1|3107|2076|05001|1|2",
            "fecha_ingesta": "2026-11-20T12:00:00Z",
            "payload": {"a_o": "2021", "matriculados_2015": "40"},
        },
    ]
    archivo.write_text("\n".join(json.dumps(s) for s in sobres) + "\n", encoding="utf-8")

    prod = ProductorFalso()
    res1 = productor.ejecutar(productor=prod, raw_dir=tmp_path)
    assert sum(res1.values()) == 2
    assert prod.enviados[0]["key"] == "2021|1|3107|2076|05001|1|1"
    assert prod.enviados[0]["topic"] == "raw.datos_gov_co"
    assert archivo.with_name(archivo.name + ".kafka_ok").exists()

    # Segunda corrida: no reenvía porque existe .kafka_ok
    res2 = productor.ejecutar(productor=prod, raw_dir=tmp_path)
    assert res2 == {}
    assert len(prod.enviados) == 2

