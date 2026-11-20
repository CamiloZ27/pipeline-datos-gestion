"""Generador del tablero HTML autocontenido (Plotly.js + fuente Inter embebida)."""
from __future__ import annotations

import base64
import json
import shutil
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from visualizacion import config, datos, modelo


def _seguro_script(texto: str) -> str:
    return texto.replace("</", "<\\/").replace("\ufffd", "\\ufffd")


def _cargar_plotly() -> str:
    plotly_js = (config.VENDOR / "plotly-basic.min.js").read_text(encoding="utf-8")
    locale_es = (config.VENDOR / "plotly-locale-es.js").read_text(encoding="utf-8")
    return (
        plotly_js
        + "\n;(function(){var module={exports:{}};\n"
        + locale_es
        + "\nPlotly.register(module.exports);})();"
    )


def _cargar_fuentes() -> dict:
    carpeta = config.VENDOR / "fuentes"
    return {
        peso: base64.b64encode((carpeta / f"inter-latin-{peso}-normal.woff2").read_bytes()).decode("ascii")
        for peso in (400, 600, 800)
    }


def generar(
    origen: str = "postgres",
    salida: Optional[Path] = None,
    publicar: bool = False,
) -> Path:
    tablas = datos.cargar(origen)
    reporte_calidad = datos.cargar_reporte_calidad()
    d = modelo.construir(tablas, reporte_calidad)

    salida_dir = Path(salida or config.SALIDA)
    salida_dir.mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=FileSystemLoader(config.PLANTILLAS),
        autoescape=select_autoescape(["html"]),
    )
    html = env.get_template("dashboard.html").render(
        d=d,
        datos_json=Markup(_seguro_script(json.dumps(d, ensure_ascii=False, separators=(",", ":")))),
        plotly_js=Markup(_seguro_script(_cargar_plotly())),
        fuentes=_cargar_fuentes(),
        autores=config.AUTORES,
        institucion=config.INSTITUCION,
    )

    destino = salida_dir / "dashboard.html"
    destino.write_text(html, encoding="utf-8")

    # Trazabilidad: guarda el JSON exacto con el que se renderizó el tablero
    (salida_dir / "datos_tablero.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if publicar:
        config.DOCS.mkdir(parents=True, exist_ok=True)
        shutil.copy2(destino, config.DOCS / "index.html")

    return destino

