"""Construye el modelo de datos listo para renderizar en el dashboard Plotly.js."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List


def construir(tablas: Dict[str, List[dict]], reporte_calidad: dict) -> dict:
    meta = tablas["metadatos_pipeline"][0]
    evol = sorted(tablas["indicador_evolucion_semestral"], key=lambda r: (r["anio"], r["semestre"]))
    sem1 = [r for r in evol if int(r["semestre"]) == 1]
    ultimo_sem1 = sem1[-1] if sem1 else evol[-1]
    primero_sem1 = sem1[0] if sem1 else evol[0]
    anio_max = int(meta["anio_hasta"])

    # Variación 2015-1 -> 2021-1
    var_col_pct = round(
        100.0 * (ultimo_sem1["total_matriculados"] / primero_sem1["total_matriculados"] - 1),
        1,
    )
    var_ant_pct = round(
        100.0 * (ultimo_sem1["matriculados_antioquia"] / primero_sem1["matriculados_antioquia"] - 1),
        1,
    )

    # Top 12 departamentos del último año (semestre 1)
    deptos_ultimo = [
        r for r in tablas["indicador_departamentos"] if int(r["anio"]) == anio_max
    ]
    deptos_ultimo.sort(key=lambda r: r["matriculados_sem1"], reverse=True)
    top_deptos = deptos_ultimo[:12]

    # Sector x Metodología por año (semestre 1 para serie comparable)
    sm_sem1 = [
        r for r in tablas["indicador_sector_metodologia"] if int(r["semestre"]) == 1
    ]
    anios = sorted({int(r["anio"]) for r in sm_sem1})
    por_met = defaultdict(lambda: defaultdict(int))
    por_sector = defaultdict(lambda: defaultdict(int))
    for r in sm_sem1:
        a = int(r["anio"])
        met = r["metodologia_nombre"]
        if met not in ("Presencial", "Distancia (tradicional)", "Virtual"):
            met = "Otras modalidades"
        por_met[met][a] += int(r["total_matriculados"])
        por_sector[r["sector_nombre"]][a] += int(r["total_matriculados"])

    # Crecimiento de virtualidad 2015-1 -> 2021-1
    virt_ini = por_met["Virtual"].get(anios[0], 1)
    virt_fin = por_met["Virtual"].get(anios[-1], 0)
    crec_virtual_pct = round(100.0 * (virt_fin / virt_ini - 1), 1) if virt_ini else 0.0

    # Áreas x Género (último año)
    ag_ultimo = [
        r for r in tablas["indicador_areas_genero"]
        if int(r["anio"]) == anio_max and r["area_nombre"] != "Sin clasificar"
    ]
    areas_map = defaultdict(lambda: {"Femenino": 0, "Masculino": 0})
    for r in ag_ultimo:
        if r["genero_nombre"] in ("Femenino", "Masculino"):
            areas_map[r["area_nombre"]][r["genero_nombre"]] += int(r["total_matriculados"])
    areas_ordenadas = sorted(
        [
            {
                "area": area,
                "femenino": vals["Femenino"],
                "masculino": vals["Masculino"],
                "total": vals["Femenino"] + vals["Masculino"],
                "pct_femenino": round(
                    100.0 * vals["Femenino"] / max(1, vals["Femenino"] + vals["Masculino"]), 1
                ),
            }
            for area, vals in areas_map.items()
        ],
        key=lambda x: x["total"],
        reverse=True,
    )

    # Top 10 IES de Antioquia en el último año + asegurar I.U. Pascual Bravo visible
    ies_ant = [
        r for r in tablas["indicador_top_ies"]
        if int(r["anio"]) == anio_max and bool(r["es_antioquia"])
    ]
    ies_ant.sort(key=lambda r: r["matriculados_sem1"], reverse=True)
    top_ies_ant = ies_ant[:10]
    pascual_row = next((r for r in ies_ant if bool(r["es_pascual_bravo"])), None)
    if pascual_row and not any(bool(r["es_pascual_bravo"]) for r in top_ies_ant):
        top_ies_ant.append(pascual_row)

    # Resumen capa de velocidad (Lambda)
    stream_rows = tablas.get("stream_ventanas", [])
    stream_por_anio = defaultdict(int)
    for r in stream_rows:
        stream_por_anio[int(r["anio"])] += int(r["total_matriculados"])
    batch_por_anio = defaultdict(int)
    for r in evol:
        batch_por_anio[int(r["anio"])] += int(r["total_matriculados"])

    reconciliacion_lambda = [
        {
            "anio": a,
            "batch_matriculados": batch_por_anio[a],
            "stream_matriculados": stream_por_anio.get(a, 0),
        }
        for a in sorted(batch_por_anio)
    ]

    cal_res = reporte_calidad.get("resumen", {})
    alm = reporte_calidad.get("almacenamiento", {})

    return {
        "meta": meta,
        "titulo_respuesta": (
            f"La educación superior en Colombia alcanzó {ultimo_sem1['total_matriculados']:,} "
            f"matriculados en {ultimo_sem1['periodo_label']}: la virtualidad creció +{crec_virtual_pct}% "
            f"y Antioquia concentró el {ultimo_sem1['pct_antioquia']}% del país"
        ).replace(",", "."),
        "kpi": {
            "periodo_corte": ultimo_sem1["periodo_label"],
            "matriculados_colombia": int(ultimo_sem1["total_matriculados"]),
            "variacion_colombia_pct": var_col_pct,
            "matriculados_antioquia": int(ultimo_sem1["matriculados_antioquia"]),
            "pct_antioquia": float(ultimo_sem1["pct_antioquia"]),
            "variacion_antioquia_pct": var_ant_pct,
            "pct_virtual": float(ultimo_sem1["pct_virtual"]),
            "crec_virtual_pct": crec_virtual_pct,
            "pct_femenino": float(ultimo_sem1["pct_femenino"]),
            "pct_oficial": round(
                100.0 * ultimo_sem1["matriculados_oficial"] / ultimo_sem1["total_matriculados"], 1
            ),
            "ies_unicas": int(meta["ies_unicas"]),
            "programas_unicos": int(meta["programas_unicos"]),
            "duplicados_corregidos": int(cal_res.get("duplicados_eliminados", 0)),
            "compresion_parquet_x": float(alm.get("factor_reduccion_x", 0)),
        },
        "evolucion_sem1": sem1,
        "evolucion_completa": evol,
        "top_deptos": top_deptos,
        "anios": anios,
        "metodologias": {
            m: [por_met[m][a] for a in anios]
            for m in ["Presencial", "Distancia (tradicional)", "Virtual"]
        },
        "sectores": {
            s: [por_sector[s][a] for a in anios] for s in ["Oficial", "Privada"]
        },
        "areas_genero": areas_ordenadas,
        "top_ies_antioquia": top_ies_ant,
        "reconciliacion_lambda": reconciliacion_lambda,
    }

