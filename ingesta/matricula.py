"""
Extractor de Matrícula en Educación Superior (MEN - SNIES) desde datos.gov.co.

Dataset: MEN_MATRICULA_ESTADISTICA_ES (ID: 5wck-szir, ~390.903 filas, 2015-2021)
API:     Socrata SODA2 (https://www.datos.gov.co/resource/5wck-szir.json)

Características clave (Fases 2 y 6):
  - Paginación estable con $limit, $offset y $order (:id) para no perder ni
    repetir filas entre páginas.
  - Carga incremental mediante marca de agua ($where sobre a_o y semestre).
  - Envoltura intacta en sobre JSON con id_registro natural normalizado.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Optional, Tuple

import requests

from ingesta import comun, config

log = logging.getLogger("ingesta.matricula")


def _norm_cod(valor: Optional[str], ancho: int = 0) -> str:
    """Normaliza códigos numéricos solo para construir id_registro consistente
    (p. ej. '5001' en 2015 y '05001' en 2021 representan el mismo municipio).
    El payload original NO se altera."""
    texto = str(valor or "").strip()
    if texto.isdigit():
        num = str(int(texto))
        return num.zfill(ancho) if ancho else num
    return texto.upper()


def construir_id_registro(fila: dict) -> str:
    """Llave natural que identifica un registro único de matrícula en SNIES:

        anio | semestre | cod_ies | cod_snies | cod_muni_oferta | metodologia | genero

    Justificación:
      En el SNIES cada fila representa el conteo de estudiantes matriculados
      en un (año, semestre) para una institución (sede/seccional), un programa
      SNIES específico, un municipio donde se oferta, una metodología y un género.
      Los años 2015-2019 vienen duplicados 2x en la fuente oficial; esta llave
      permite detectarlos y eliminarlos de forma exacta en PySpark.
    """
    partes = [
        _norm_cod(fila.get("a_o")),
        _norm_cod(fila.get("semestre")),
        _norm_cod(fila.get("c_digo_de_la_instituci_n")),
        _norm_cod(fila.get("c_digo_snies_delprograma")),
        _norm_cod(fila.get("c_digo_del_municipio_programa"), ancho=5),
        _norm_cod(fila.get("id_metodologia")),
        _norm_cod(fila.get("id_g_nero")),
    ]
    return "|".join(partes)


def fecha_periodo_iso(anio: int, semestre: int) -> str:
    """Convierte (año, semestre) a fecha ISO oficial de inicio de periodo:
    semestre 1 -> YYYY-01-01, semestre 2 -> YYYY-07-01."""
    mes = "01" if semestre == 1 else "07"
    return f"{anio:04d}-{mes}-01"


def construir_where(
    desde_anio: Optional[int] = None,
    desde_semestre: Optional[int] = None,
    hasta_anio: Optional[int] = None,
    hasta_semestre: Optional[int] = None,
    estrictamente_mayor: bool = False,
) -> Optional[str]:
    """Construye cláusula SoQL $where para carga total o incremental."""
    conds = []
    if desde_anio is not None:
        if desde_semestre is not None:
            op = ">" if estrictamente_mayor else ">="
            conds.append(
                f"(a_o > {desde_anio} OR (a_o = {desde_anio} AND semestre {op} {desde_semestre}))"
            )
        else:
            op = ">" if estrictamente_mayor else ">="
            conds.append(f"a_o {op} {desde_anio}")

    if hasta_anio is not None:
        if hasta_semestre is not None:
            conds.append(
                f"(a_o < {hasta_anio} OR (a_o = {hasta_anio} AND semestre <= {hasta_semestre}))"
            )
        else:
            conds.append(f"a_o <= {hasta_anio}")

    return " AND ".join(conds) or None


def extraer_matricula(
    sesion: requests.Session,
    desde_anio: Optional[int] = None,
    desde_semestre: Optional[int] = None,
    hasta_anio: Optional[int] = None,
    hasta_semestre: Optional[int] = None,
    estrictamente_mayor: bool = False,
    tamano_pagina: Optional[int] = None,
    max_paginas: Optional[int] = None,
) -> Iterator[dict]:
    """Recorre todas las páginas de la API SODA2 y entrega cada registro crudo."""
    limite = tamano_pagina or config.TAMANO_PAGINA
    params = {
        "$order": "a_o ASC, semestre ASC, :id ASC",
        "$limit": limite,
        "$offset": 0,
    }
    where = construir_where(
        desde_anio=desde_anio,
        desde_semestre=desde_semestre,
        hasta_anio=hasta_anio,
        hasta_semestre=hasta_semestre,
        estrictamente_mayor=estrictamente_mayor,
    )
    if where:
        params["$where"] = where

    headers = {}
    if config.SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = config.SOCRATA_APP_TOKEN

    paginas = 0
    while True:
        resp = sesion.get(
            config.SOCRATA_URL,
            params=params,
            headers=headers,
            timeout=config.HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        pagina = resp.json()
        if not isinstance(pagina, list):
            raise RuntimeError(f"Respuesta inesperada de Socrata: {str(pagina)[:200]}")

        paginas += 1
        log.info(
            "Página %d (offset=%d): %d filas recibidas",
            paginas,
            params["$offset"],
            len(pagina),
        )
        yield from pagina

        if len(pagina) < limite:
            break
        if max_paginas and paginas >= max_paginas:
            log.info("Alcanzado límite de páginas solicitado (%d).", max_paginas)
            break
        params["$offset"] += limite


def _max_periodo(registros: list) -> Optional[Tuple[int, int]]:
    max_p: Optional[Tuple[int, int]] = None
    for sobre in registros:
        p = sobre["payload"]
        try:
            par = (int(p.get("a_o")), int(p.get("semestre")))
        except (TypeError, ValueError):
            continue
        if max_p is None or par > max_p:
            max_p = par
    return max_p


def ejecutar(
    desde_anio: Optional[int] = None,
    desde_semestre: Optional[int] = None,
    hasta_anio: Optional[int] = None,
    hasta_semestre: Optional[int] = None,
    incremental: bool = False,
    tamano_pagina: Optional[int] = None,
    max_paginas: Optional[int] = None,
    sesion: Optional[requests.Session] = None,
    raw_dir: Optional[Path] = None,
    watermark_path: Optional[Path] = None,
) -> Optional[Path]:
    """Ejecuta la ingesta a la zona raw y actualiza la marca de agua."""
    sesion = sesion or comun.crear_sesion()
    fecha_ingesta = comun.ahora_utc()
    estrictamente_mayor = False

    if incremental:
        marca = comun.leer_marca_agua(watermark_path)
        if marca:
            desde_anio = int(marca["ultimo_anio"])
            desde_semestre = int(marca["ultimo_semestre"])
            estrictamente_mayor = True
            log.info(
                "Modo incremental: consultando registros posteriores a %d-%d (%s)",
                desde_anio,
                desde_semestre,
                marca["ultima_fecha_periodo"],
            )
        else:
            log.info("Modo incremental sin marca de agua previa: descarga inicial completa.")

    registros = []
    for fila in extraer_matricula(
        sesion=sesion,
        desde_anio=desde_anio,
        desde_semestre=desde_semestre,
        hasta_anio=hasta_anio,
        hasta_semestre=hasta_semestre,
        estrictamente_mayor=estrictamente_mayor,
        tamano_pagina=tamano_pagina,
        max_paginas=max_paginas,
    ):
        registros.append(
            comun.construir_sobre(
                fuente=config.FUENTE,
                dataset=config.DATASET,
                id_registro=construir_id_registro(fila),
                payload=fila,
                fecha_ingesta=fecha_ingesta,
            )
        )

    if not registros:
        log.info("Sin registros nuevos para el rango solicitado (marca de agua al día).")
        return None

    parametros = {
        "desde_anio": desde_anio,
        "desde_semestre": desde_semestre,
        "hasta_anio": hasta_anio,
        "hasta_semestre": hasta_semestre,
        "incremental": incremental,
        "estrictamente_mayor": estrictamente_mayor,
        "tamano_pagina": tamano_pagina or config.TAMANO_PAGINA,
    }
    archivo = comun.guardar_jsonl(
        registros=registros,
        fuente=config.FUENTE,
        dataset=config.DATASET,
        fecha_ingesta=fecha_ingesta,
        url=config.SOCRATA_URL,
        parametros=parametros,
        raw_dir=raw_dir,
    )

    max_p = _max_periodo(registros)
    if max_p:
        comun.guardar_marca_agua(
            ultimo_anio=max_p[0],
            ultimo_semestre=max_p[1],
            fecha_periodo=fecha_periodo_iso(max_p[0], max_p[1]),
            fecha_ingesta=fecha_ingesta,
            registros_ejecucion=len(registros),
            ruta=watermark_path,
        )
    return archivo

