"""Pruebas unitarias offline de la Fase 4b (MapReduce en Python)."""
from __future__ import annotations

import json

from mapreduce.reducer import ejecutar_en_memoria


def test_mapreduce_cuenta_y_suma_sobre_raw():
    lineas = [
        json.dumps({"payload": {"a_o": "2021", "id_sector": "1", "matriculados_2015": "100"}}),
        json.dumps({"payload": {"a_o": "2021", "id_sector": "1", "matriculados_2015": "250"}}),
        json.dumps({"payload": {"a_o": "2021", "id_sector": "2", "matriculados_2015": "80"}}),
        json.dumps({"payload": {"a_o": "2020", "id_sector": "1", "matriculados_2015": "50"}}),
    ]
    res = ejecutar_en_memoria(lineas)
    assert res["2021|1"] == (2, 350)
    assert res["2021|2"] == (1, 80)
    assert res["2020|1"] == (1, 50)

