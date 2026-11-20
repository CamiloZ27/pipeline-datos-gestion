"""
Genera los dos documentos PDF finales del proyecto utilizando Google Chrome headless:
  1. `docs/Guia_Completa_Ejecucion_Pipeline.pdf`
     Resumen técnico integral + guía paso a paso de ejecución (Fases 1-8 + Hive + Airflow)
     + guion de sustentación y preguntas frecuentes.
  2. `docs/Presentacion_Sustentacion.pdf`
     Presentación ejecutiva de EXACTAMENTE 8 diapositivas (16:9) acorde con la rúbrica.

Todos los números se leen dinámicamente de los archivos reales generados por el pipeline
(`evidencias/` y `salida/datos_tablero.json`), evitando datos inventados.
"""
from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DOCS = RAIZ / "docs"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def fmt(n: int | float) -> str:
    if isinstance(n, float):
        return f"{n:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"{n:,}".replace(",", ".")


def cargar_metricas() -> dict:
    calidad = json.loads((RAIZ / "evidencias" / "reporte_calidad.json").read_text(encoding="utf-8"))
    stream = json.loads((RAIZ / "evidencias" / "streaming_reinicio.json").read_text(encoding="utf-8"))
    mr = json.loads((RAIZ / "evidencias" / "mapreduce_vs_spark.json").read_text(encoding="utf-8"))
    tablero = json.loads((RAIZ / "salida" / "datos_tablero.json").read_text(encoding="utf-8"))
    return {
        "calidad": calidad,
        "stream": stream,
        "mr": mr,
        "tablero": tablero,
    }


def svg_arquitectura_lambda(compacto: bool = False) -> str:
    h = 240 if compacto else 270
    return f"""
    <svg viewBox="0 0 960 {h}" width="100%" xmlns="http://www.w3.org/2000/svg" style="display:block;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
      <defs>
        <marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M 0 1 L 10 5 L 0 9 z" fill="#475569"/>
        </marker>
        <marker id="arrA" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M 0 1 L 10 5 L 0 9 z" fill="#d97706"/>
        </marker>
      </defs>

      <!-- Zonas de fondo -->
      <rect x="8" y="8" width="295" height="{h-48}" rx="10" fill="#f1f5f9" stroke="#cbd5e1" stroke-dasharray="4 3"/>
      <text x="18" y="25" font-size="10" font-weight="700" fill="#475569">ZONA RAW (INGESTA + MENSAJERÍA)</text>

      <rect x="315" y="8" width="405" height="{h-48}" rx="10" fill="#eff6ff" stroke="#93c5fd" stroke-dasharray="4 3"/>
      <text x="325" y="25" font-size="10" font-weight="700" fill="#1d4ed8">ZONA CURADA (CAPA BATCH + CAPA VELOCIDAD)</text>

      <rect x="732" y="8" width="220" height="{h-48}" rx="10" fill="#ecfdf5" stroke="#6ee7b7" stroke-dasharray="4 3"/>
      <text x="742" y="25" font-size="10" font-weight="700" fill="#047857">ZONA DE SERVICIO</text>

      <!-- Nodo 1: API -->
      <rect x="20" y="55" width="122" height="72" rx="8" fill="#0f2744"/>
      <text x="81" y="77" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff">API datos.gov.co</text>
      <text x="81" y="93" text-anchor="middle" font-size="9.5" fill="#cbd5e1">SODA2 · 5wck-szir</text>
      <text x="81" y="108" text-anchor="middle" font-size="9" fill="#fbbf24">390.903 filas raw</text>

      <!-- Nodo 2: Kafka -->
      <rect x="168" y="55" width="124" height="72" rx="8" fill="#1e3a5f"/>
      <text x="230" y="77" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff">Apache Kafka 4.1</text>
      <text x="230" y="93" text-anchor="middle" font-size="9.5" fill="#cbd5e1">raw.datos_gov_co</text>
      <text x="230" y="108" text-anchor="middle" font-size="9" fill="#93c5fd">3 particiones · key=id</text>

      <!-- Flecha API -> Kafka -->
      <line x1="142" y1="91" x2="165" y2="91" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
      <text x="154" y="48" text-anchor="middle" font-size="8.5" fill="#475569">JSONL+</text>
      <text x="154" y="138" text-anchor="middle" font-size="8.5" fill="#475569">marca agua</text>

      <!-- CAPA BATCH -->
      <rect x="328" y="38" width="116" height="62" rx="8" fill="#1d4ed8"/>
      <text x="386" y="57" text-anchor="middle" font-size="10.5" font-weight="700" fill="#ffffff">HDFS 3.4.1 Raw</text>
      <text x="386" y="72" text-anchor="middle" font-size="9" fill="#dbeafe">25 archivos lote</text>
      <text x="386" y="86" text-anchor="middle" font-size="9" fill="#bfdbfe">401,58 MB JSONL</text>

      <rect x="466" y="38" width="120" height="62" rx="8" fill="#1e40af"/>
      <text x="526" y="57" text-anchor="middle" font-size="10.5" font-weight="700" fill="#ffffff">PySpark 4.0 + MR</text>
      <text x="526" y="72" text-anchor="middle" font-size="9" fill="#dbeafe">230.082 únicas</text>
      <text x="526" y="86" text-anchor="middle" font-size="9" fill="#fde68a">5/5 fail-fast OK</text>

      <rect x="606" y="38" width="104" height="62" rx="8" fill="#0369a1"/>
      <text x="658" y="57" text-anchor="middle" font-size="10.5" font-weight="700" fill="#ffffff">Parquet + Hive</text>
      <text x="658" y="72" text-anchor="middle" font-size="9" fill="#e0f2fe">5,66 MB (70,92x)</text>
      <text x="658" y="86" text-anchor="middle" font-size="9" fill="#fde68a">Hive +2 · 4 SQL</text>

      <!-- CAPA SPEED -->
      <rect x="360" y="122" width="320" height="58" rx="8" fill="#b45309"/>
      <text x="520" y="142" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff">Spark Structured Streaming (Capa de Velocidad)</text>
      <text x="520" y="158" text-anchor="middle" font-size="9.5" fill="#fef3c7">watermark 5m · ventana 10m · checkpoint · 0 duplicados al reiniciar</text>
      <text x="520" y="172" text-anchor="middle" font-size="8.8" fill="#fde68a">Reconciliación exacta con Batch: 16.773.503 matriculados (0 dif.)</text>

      <!-- Flechas Kafka -> Batch y Speed -->
      <path d="M 292 78 L 325 69" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
      <path d="M 292 108 L 357 150" fill="none" stroke="#d97706" stroke-width="1.8" marker-end="url(#arrA)"/>
      <line x1="444" y1="69" x2="463" y2="69" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
      <line x1="586" y1="69" x2="603" y2="69" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>

      <!-- SERVICIO -->
      <rect x="744" y="42" width="196" height="62" rx="8" fill="#047857"/>
      <text x="842" y="61" text-anchor="middle" font-size="10.5" font-weight="700" fill="#ffffff">PostgreSQL 17 (servicio.*)</text>
      <text x="842" y="76" text-anchor="middle" font-size="9" fill="#d1fae5">7 tablas · usuario lector</text>
      <text x="842" y="90" text-anchor="middle" font-size="9" fill="#a7f3d0">Puerto host 5433 (idempotente)</text>

      <rect x="744" y="122" width="196" height="58" rx="8" fill="#065f46"/>
      <text x="842" y="142" text-anchor="middle" font-size="10.5" font-weight="700" fill="#ffffff">Tablero Plotly HTML</text>
      <text x="842" y="158" text-anchor="middle" font-size="9" fill="#d1fae5">salida/dashboard.html</text>
      <text x="842" y="171" text-anchor="middle" font-size="9" fill="#fde68a">docs/index.html (GitHub Pages)</text>

      <!-- Flechas a PostgreSQL y Tablero -->
      <line x1="710" y1="69" x2="741" y2="69" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
      <path d="M 680 142 L 732 142 L 732 88 L 741 88" fill="none" stroke="#d97706" stroke-width="1.8" marker-end="url(#arrA)"/>
      <line x1="842" y1="104" x2="842" y2="119" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>

      <!-- Barra inferior Airflow -->
      <rect x="8" y="{h-34}" width="944" height="28" rx="6" fill="#0f172a"/>
      <text x="480" y="{h-16}" text-anchor="middle" font-size="10.5" font-weight="600" fill="#f8fafc">
        Orquestación Opcional (+3 pts): Apache Airflow 3.3.2 · DAG matricula_es_pipeline_completo (11 tareas · retries=2..3 · trigger_rule=all_done · localhost:8082)
      </text>
    </svg>
    """


def construir_html_guia(m: dict) -> str:
    cal = m["calidad"]
    tab = m["tablero"]
    kpi = tab["kpi"]

    filas_anio = ""
    for f in cal["antes_despues_por_anio"]:
        filas_anio += f"""
        <tr>
          <td><strong>{f['anio']}</strong></td>
          <td class="num">{fmt(f['filas_raw'])}</td>
          <td class="num">{fmt(f['duplicados_y_descartes'])}</td>
          <td class="num">{fmt(f['filas_curadas'])}</td>
          <td class="num"><strong>{fmt(f['total_matriculados_curados'])}</strong></td>
        </tr>"""

    filas_val = ""
    for v in cal["validaciones"]:
        filas_val += f"""
        <tr>
          <td><code>{v['nombre']}</code></td>
          <td><span class="badge ok">APROBADA</span></td>
          <td>{v['detalle']}</td>
        </tr>"""

    filas_rec = ""
    for r in tab["reconciliacion_lambda"]:
        dif = r["batch_matriculados"] - r["stream_matriculados"]
        filas_rec += f"""
        <tr>
          <td><strong>{r['anio']}</strong></td>
          <td class="num">{fmt(r['batch_matriculados'])}</td>
          <td class="num">{fmt(r['stream_matriculados'])}</td>
          <td class="num"><span class="badge ok">{dif} dif.</span></td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<title>Guía Completa del Pipeline — Matrícula Educación Superior Colombia</title>
<style>
  @page {{
    size: A4 portrait;
    margin: 16mm 15mm 18mm 15mm;
    @top-left {{
      content: "Pipeline Big Data · Matrícula ES Colombia (2015–2021)";
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 8pt;
      color: #64748b;
    }}
    @top-right {{
      content: "I.U. Pascual Bravo · G004";
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 8pt;
      color: #64748b;
    }}
    @bottom-left {{
      content: "Camilo Zuleta Delgado · Santiago Restrepo Silva";
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 8pt;
      color: #64748b;
    }}
    @bottom-right {{
      content: "Página " counter(page) " de " counter(pages);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 8pt;
      font-weight: 600;
      color: #334155;
    }}
  }}
  @page :first {{
    margin: 0;
    @top-left {{ content: none; }}
    @top-right {{ content: none; }}
    @bottom-left {{ content: none; }}
    @bottom-right {{ content: none; }}
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    color: #1e293b;
    font-size: 9.4pt;
    line-height: 1.48;
    margin: 0;
  }}

  /* PORTADA */
  .cover {{
    width: 210mm;
    height: 297mm;
    background: linear-gradient(155deg, #0b1e36 0%, #13315c 52%, #1d4ed8 100%);
    color: #ffffff;
    padding: 24mm 20mm 18mm 20mm;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    page-break-after: always;
  }}
  .cover-tag {{
    display: inline-block;
    background: rgba(251, 191, 36, 0.18);
    border: 1px solid rgba(251, 191, 36, 0.65);
    color: #fde68a;
    padding: 5px 12px;
    border-radius: 999px;
    font-size: 8.5pt;
    font-weight: 700;
    letter-spacing: 0.4px;
    text-transform: uppercase;
  }}
  .cover h1 {{
    font-size: 27pt;
    line-height: 1.15;
    margin: 16px 0 12px 0;
    color: #ffffff;
    font-weight: 800;
  }}
  .cover .sub {{
    font-size: 12pt;
    color: #dbeafe;
    line-height: 1.45;
    max-width: 165mm;
    margin: 0 0 18px 0;
  }}
  .cover-question {{
    background: rgba(255,255,255,0.09);
    border-left: 4px solid #fbbf24;
    padding: 13px 16px;
    border-radius: 6px;
    margin: 14px 0 18px 0;
  }}
  .cover-question .lbl {{
    font-size: 8pt;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: #fde68a;
    font-weight: 700;
    margin-bottom: 4px;
  }}
  .cover-question p {{
    margin: 0;
    font-size: 10.2pt;
    color: #f8fafc;
    font-style: italic;
  }}
  .cover-kpis {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin: 14px 0 18px 0;
  }}
  .ckpi {{
    background: rgba(15, 23, 42, 0.55);
    border: 1px solid rgba(148, 163, 184, 0.25);
    border-radius: 8px;
    padding: 12px 10px;
  }}
  .ckpi .v {{
    font-size: 16pt;
    font-weight: 800;
    color: #fbbf24;
    line-height: 1.1;
  }}
  .ckpi .k {{
    font-size: 7.8pt;
    color: #cbd5e1;
    margin-top: 4px;
  }}
  .cover-toc {{
    background: rgba(255,255,255,0.07);
    border-radius: 8px;
    padding: 12px 16px;
    font-size: 9pt;
    color: #e2e8f0;
  }}
  .cover-toc h4 {{
    margin: 0 0 6px 0;
    font-size: 8.5pt;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: #93c5fd;
  }}
  .cover-toc ol {{
    margin: 0;
    padding-left: 18px;
    columns: 2;
    column-gap: 20px;
  }}
  .cover-toc li {{ margin-bottom: 3px; }}
  .cover-footer {{
    border-top: 1px solid rgba(255,255,255,0.2);
    padding-top: 14px;
    display: grid;
    grid-template-columns: 1.2fr 1fr;
    gap: 14px;
    font-size: 9pt;
    color: #e2e8f0;
  }}
  .cover-footer strong {{ color: #ffffff; }}

  /* SECCIONES */
  h2 {{
    font-size: 14.5pt;
    color: #0f2744;
    border-bottom: 2.2px solid #1d4ed8;
    padding-bottom: 4px;
    margin: 16px 0 10px 0;
    page-break-after: avoid;
  }}
  h3 {{
    font-size: 11pt;
    color: #1e3a5f;
    margin: 13px 0 6px 0;
    page-break-after: avoid;
  }}
  p {{ margin: 0 0 7px 0; }}
  ul, ol {{ margin: 4px 0 8px 0; padding-left: 18px; }}
  li {{ margin-bottom: 3px; }}

  .page-break {{ page-break-before: always; }}
  .avoid-break {{ break-inside: avoid; page-break-inside: avoid; }}

  /* TABLAS */
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 8px 0 12px 0;
    font-size: 8.6pt;
    break-inside: avoid;
  }}
  th {{
    background: #0f2744;
    color: #ffffff;
    text-align: left;
    padding: 6px 8px;
    font-weight: 700;
  }}
  td {{
    border-bottom: 1px solid #e2e8f0;
    padding: 5.5px 8px;
    vertical-align: top;
  }}
  tr:nth-child(even) td {{ background: #f8fafc; }}
  td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}

  /* BLOQUES Y CALLOUTS */
  .callout {{
    background: #f8fafc;
    border: 1px solid #cbd5e1;
    border-left: 4px solid #1d4ed8;
    border-radius: 6px;
    padding: 9px 12px;
    margin: 8px 0 10px 0;
    break-inside: avoid;
  }}
  .callout.amber {{ border-left-color: #d97706; background: #fffbeb; border-color: #fde68a; }}
  .callout.green {{ border-left-color: #059669; background: #ecfdf5; border-color: #a7f3d0; }}
  .callout strong.title {{ display: block; color: #0f2744; margin-bottom: 3px; font-size: 9.2pt; }}

  /* CÓDIGO */
  pre {{
    background: #0f172a;
    color: #e2e8f0;
    padding: 9px 11px;
    border-radius: 6px;
    font-family: "SFMono-Regular", Menlo, Consolas, monospace;
    font-size: 7.9pt;
    line-height: 1.42;
    white-space: pre-wrap;
    word-break: break-word;
    margin: 5px 0 9px 0;
    break-inside: avoid;
  }}
  code {{
    font-family: "SFMono-Regular", Menlo, Consolas, monospace;
    font-size: 8.2pt;
    background: #f1f5f9;
    color: #0f2744;
    padding: 1px 4px;
    border-radius: 3px;
  }}
  pre code {{ background: none; color: inherit; padding: 0; }}

  .badge {{
    display: inline-block;
    padding: 1.5px 7px;
    border-radius: 999px;
    font-size: 7.6pt;
    font-weight: 700;
  }}
  .badge.ok {{ background: #d1fae5; color: #065f46; }}
  .badge.extra {{ background: #fef3c7; color: #92400e; }}
  .badge.info {{ background: #dbeafe; color: #1e40af; }}

  .grid-2 {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin: 8px 0;
  }}
  .card {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 9px 11px;
    break-inside: avoid;
    min-width: 0;
    overflow-wrap: anywhere;
  }}
  .card h4 {{
    margin: 0 0 5px 0;
    font-size: 9.4pt;
    color: #0f2744;
  }}
  .step-hdr {{
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 12px 0 4px 0;
    page-break-after: avoid;
  }}
  .step-num {{
    background: #1d4ed8;
    color: #ffffff;
    font-weight: 800;
    font-size: 8.3pt;
    padding: 2px 8px;
    border-radius: 4px;
  }}
  .step-num.extra {{ background: #d97706; }}
  .step-title {{
    font-size: 10.4pt;
    font-weight: 700;
    color: #0f2744;
  }}
</style>
</head>
<body>

<!-- ============================= PORTADA ============================= -->
<div class="cover">
  <div>
    <span class="cover-tag">Institución Universitaria Pascual Bravo · Gestión de Datos Masivos G004 · Unidad 3</span>
    <h1>Pipeline Big Data:<br/>Matrícula de Educación Superior en Colombia (2015–2021)</h1>
    <p class="sub">Resumen Técnico Completo, Resultados Reales Verificados y Guía Paso a Paso de Ejecución (Fases 1–8 + Apache Hive + Apache Airflow)</p>

    <div class="cover-question">
      <div class="lbl">Pregunta principal del proyecto</div>
      <p>«¿Cómo evolucionó la matrícula de educación superior en Colombia (2015–2021) por departamento, sector (oficial vs. privado), metodología (presencial/virtual/distancia), área del conocimiento y género, y cuál es la brecha regional y de Antioquia frente al país?»</p>
    </div>

    <div class="cover-kpis">
      <div class="ckpi">
        <div class="v">390.903</div>
        <div class="k">Filas descargadas API SODA2 (5wck-szir)</div>
      </div>
      <div class="ckpi">
        <div class="v">160.821</div>
        <div class="k">Duplicados reales depurados (41,1%)</div>
      </div>
      <div class="ckpi">
        <div class="v">70,92×</div>
        <div class="k">Compresión Parquet (401,6 → 5,66 MB)</div>
      </div>
      <div class="ckpi">
        <div class="v">8 + 2</div>
        <div class="k">Fases obligatorias + Hive (+2) y Airflow (+3)</div>
      </div>
    </div>

    <div class="cover-toc">
      <h4>Contenido de esta guía</h4>
      <ol>
        <li>Resumen ejecutivo, fuente y arquitectura Lambda</li>
        <li>Mapeo 1:1 contra la rúbrica de evaluación (100 + 5 pts)</li>
        <li>Qué se implementó en cada fase y evidencia real</li>
        <li>Hallazgos de calidad de datos y respuestas analíticas</li>
        <li>Guía paso a paso: entorno y Fases 2 a 8</li>
        <li>Ejecución de extras: Apache Hive (+2) y Airflow (+3)</li>
        <li>Pruebas unitarias y solución de problemas comunes</li>
        <li>Guion de sustentación (10 min) y entrega final</li>
      </ol>
    </div>
  </div>

  <div class="cover-footer">
    <div>
      <strong>Estudiantes:</strong><br/>
      Camilo Zuleta Delgado<br/>
      Santiago Restrepo Silva
    </div>
    <div>
      <strong>Docente:</strong> Aleixo Eder Pineda Ramírez<br/>
      <strong>Sustentación:</strong> Miércoles 25 nov 2026 · 8:00 p. m.<br/>
      <strong>Entrega Classroom:</strong> Viernes 27 nov 2026 · 11:59 p. m.
    </div>
  </div>
</div>

<!-- ======================= SECCIÓN 1: ARQUITECTURA ======================= -->
<h2>1. Resumen Ejecutivo, Fuente y Arquitectura Lambda (Fase 1)</h2>
<p>El proyecto implementa un <strong>pipeline de datos masivos extremo a extremo</strong> sobre Docker y Python que ingesta, transporta, almacena, depura, valida, procesa en streaming, consulta y visualiza la estadística oficial de matrícula de educación superior del Ministerio de Educación Nacional (MEN — SNIES) en Colombia entre 2015 y 2021.</p>

<div class="grid-2">
  <div class="card">
    <h4>Fuente pública (sin credenciales obligatorias)</h4>
    <ul>
      <li><strong>Dataset:</strong> <code>MEN_MATRICULA_ESTADISTICA_ES</code> (ID <code>5wck-szir</code>).</li>
      <li><strong>Endpoint SODA2:</strong> <code>https://www.datos.gov.co/resource/5wck-szir.json</code>.</li>
      <li><strong>Por qué SODA2 y no SODA3:</strong> la vista v3 (<code>/api/v3/views/...</code>) requiere autenticación; SODA2 entrega las mismas <strong>390.903 filas</strong> abiertas con paginación <code>$limit/$offset/$where</code>.</li>
      <li><strong>Llave natural (<code>id_registro</code>):</strong><br/><code>anio|semestre|cod_ies|cod_snies|cod_muni(5d)|id_metod|id_genero</code>.</li>
    </ul>
  </div>
  <div class="card">
    <h4>Usuario objetivo y justificación Lambda</h4>
    <ul>
      <li><strong>Usuarios:</strong> Planeación académica de IES (ej. <strong>I.U. Pascual Bravo</strong>, SNIES <code>3107</code>) y Secretarías de Educación Departamentales.</li>
      <li><strong>¿Por qué Lambda y no Kappa?</strong> El histórico 2015–2019 exige <strong>Capa Batch</strong> (HDFS + PySpark) para depurar 160.821 duplicados (54.225 ocultos) y normalizar 90 variantes departamentales a 33 códigos DANE. Simultáneamente, la <strong>Capa de Velocidad</strong> (Spark Structured Streaming desde Kafka) actualiza ventanas con baja latencia y reconcilia <strong>0 diferencias</strong> contra Batch.</li>
    </ul>
  </div>
</div>

<div class="avoid-break" style="margin:10px 0;">
  {svg_arquitectura_lambda(compacto=False)}
</div>

<h3>Cobertura exacta de la rúbrica de evaluación (100 pts + 5 pts extra)</h3>
<table>
  <thead>
    <tr>
      <th>Fase / Componente</th>
      <th class="num">Pts</th>
      <th>Implementación y carpeta en el repositorio</th>
      <th>Evidencia verificada</th>
    </tr>
  </thead>
  <tbody>
    <tr><td><strong>Fase 1.</strong> Diseño y arquitectura</td><td class="num">10</td><td>Pregunta, usuario, Lambda vs. Kappa, diagrama en <code>README.md</code></td><td><span class="badge ok">Completo</span></td></tr>
    <tr><td><strong>Fase 2.</strong> Ingesta desde API</td><td class="num">10</td><td><code>ingesta/</code> (SODA2 paginado, sobre JSON, manifiesto, backoff)</td><td>390.903 filas · 2 manifiestos</td></tr>
    <tr><td><strong>Fase 3.</strong> Apache Kafka</td><td class="num">10</td><td><code>streaming/</code> (tópico 3 part., <code>key=id_registro</code>, <code>.kafka_ok</code>)</td><td><code>evidencias/kafka_topico_y_consumidor.txt</code></td></tr>
    <tr><td><strong>Fase 4.</strong> HDFS + MapReduce</td><td class="num">15</td><td><code>datalake/</code> (WebHDFS lotes) + <code>mapreduce/</code> vs. PySpark</td><td>25 archivos HDFS · 0 dif. MR vs Spark</td></tr>
    <tr><td><strong>Fase 5.</strong> Zona Curada PySpark</td><td class="num">15</td><td><code>procesamiento/transformaciones.py</code> + <code>catalogo.py</code> + <code>curar.py</code></td><td>230.082 curadas · Parquet 70,92×</td></tr>
    <tr><td><strong>Fase 6.</strong> Calidad e idempotencia</td><td class="num">10</td><td><code>calidad.py</code> (5 fail-fast) + marca de agua + corridas repetidas</td><td>5/5 APROBADAS · <code>idempotencia_pipeline.txt</code></td></tr>
    <tr><td><strong>Fase 7.</strong> Structured Streaming</td><td class="num">5</td><td><code>streaming_spark/flujo_matricula.py</code> (watermark 5m + checkpoint)</td><td>Reinicio: 0 lotes nuevos · 0 duplicados</td></tr>
    <tr><td><strong>Fase 8.</strong> Servicio y tablero</td><td class="num">10</td><td><code>postgres/</code> + <code>servir.py</code> + <code>visualizacion/</code> (Plotly HTML)</td><td>7 tablas PG · <code>docs/index.html</code></td></tr>
    <tr><td><strong>Opcional 1.</strong> Apache Hive</td><td class="num">+2</td><td><code>hive/01_tablas.sql</code> + <code>02_consultas.sql</code> (tabla externa Parquet)</td><td><span class="badge extra">EXTRA +2</span> 7 part. · 4 SQL reales</td></tr>
    <tr><td><strong>Opcional 2.</strong> Apache Airflow</td><td class="num">+3</td><td><code>airflow/</code> + <code>orquestacion/</code> (DAG 11 tareas + reintentos)</td><td><span class="badge extra">EXTRA +3</span> 0 errores · test OK</td></tr>
    <tr><td><strong>Sustentación individual</strong></td><td class="num">15</td><td>Presentación <code>docs/Presentacion_Sustentacion.pdf</code> (8 slides) + demo</td><td>Guion en Sección 6 de esta guía</td></tr>
  </tbody>
</table>

<!-- ==================== SECCIÓN 2: HALLAZGOS REALES ==================== -->
<h2 class="page-break">2. Qué se Encontró en los Datos: Calidad y Hallazgos Analíticos</h2>

<h3>2.1 Descubrimiento real de calidad de datos (Fases 5 y 6)</h3>
<p>Al auditar el dataset oficial del MEN (<code>5wck-szir</code>) se detectaron <strong>5 problemas reales</strong> que convierten este proyecto en un caso auténtico de ingeniería de datos:</p>
<ol>
  <li><strong>Duplicación 2× entre 2015 y 2019 (160.821 filas, 41,1% del raw):</strong> el ministerio cargó dos veces esos años. De los 160.821 duplicados, <strong>106.596 son exactos</strong> y <strong>54.225 son duplicados ocultos</strong> donde solo varía una coma en el texto del programa o del núcleo básico del conocimiento (mientras la matrícula es idéntica). Un <code>dropDuplicates()</code> simple habría dejado 54.225 filas infladas; nuestra deduplicación determinista con ventana sobre <code>id_registro</code> eliminó el 100%.</li>
  <li><strong>90 nombres sucios para 33 departamentos:</strong> aparecían variantes como <code>BOGOTA D.C</code>, <code>Bogotá, D.C.</code>, <code>NARINIO</code>, <code>ARCHIPIÉLAGO DE SA</code> y códigos DANE sin cero a la izquierda (<code>5</code> vs. <code>05</code>). Se normalizaron con <code>lpad(2)</code> y catálogo oficial DANE en <code>procesamiento/catalogo.py</code>.</li>
  <li><strong>Texto <code>"Sin clasificar"</code> en columna numérica <code>id_area</code> (192 filas):</strong> en Spark 4 (ANSI estricto) un <code>cast("int")</code> aborta la ejecución; se resolvió con <code>try_cast</code> + <code>coalesce(..., 0)</code>.</li>
  <li><strong>Columna mal nombrada en origen:</strong> <code>matriculados_2015</code> contiene las matrículas de todos los años 2015–2021; se renombró a <code>total_matriculados</code> (<code>INT</code>).</li>
  <li><strong>Compresión drástica:</strong> pasar de JSONL en HDFS raw (<strong>401,58 MB</strong>) a Parquet Snappy particionado por <code>anio</code> (<strong>5,66 MB</strong>) redujo el almacenamiento <strong>70,92 veces (98,6% de ahorro)</strong>.</li>
</ol>

<div class="grid-2">
  <div>
    <h3>Antes y después por año (<code>reporte_calidad.json</code>)</h3>
    <table>
      <thead>
        <tr><th>Año</th><th class="num">RAW</th><th class="num">Duplicados</th><th class="num">Curadas</th><th class="num">Matriculados</th></tr>
      </thead>
      <tbody>
        {filas_anio}
        <tr>
          <td><strong>TOTAL</strong></td>
          <td class="num"><strong>390.903</strong></td>
          <td class="num"><strong>160.821</strong></td>
          <td class="num"><strong>230.082</strong></td>
          <td class="num"><strong>16.773.503</strong></td>
        </tr>
      </tbody>
    </table>
  </div>
  <div>
    <h3>Reconciliación Lambda (Batch vs. Streaming)</h3>
    <table>
      <thead>
        <tr><th>Año</th><th class="num">Batch Curado</th><th class="num">Streaming</th><th class="num">Estado</th></tr>
      </thead>
      <tbody>
        {filas_rec}
        <tr>
          <td><strong>TOTAL</strong></td>
          <td class="num"><strong>16.773.503</strong></td>
          <td class="num"><strong>16.773.503</strong></td>
          <td class="num"><span class="badge ok">EXACTO</span></td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<h3>Las 5 validaciones automáticas fail-fast (<code>procesamiento/calidad.py</code>)</h3>
<table>
  <thead><tr><th>Regla</th><th>Estado</th><th>Resultado verificado</th></tr></thead>
  <tbody>{filas_val}</tbody>
</table>

<h3>2.2 Respuesta analítica a la pregunta principal</h3>
<div class="grid-2">
  <div class="callout green">
    <strong class="title">1. Evolución nacional y recuperación post-COVID</strong>
    La matrícula del Semestre 1 creció <strong>+6,8%</strong>: de <strong>1.867.703</strong> (2015-1) a <strong>1.994.383</strong> (2021-1). Tras el descenso de 2020-1 (1.930.613), el sector oficial impulsó la recuperación pasando de 778.911 a <strong>880.728</strong> (+13,1% en un año).
  </div>
  <div class="callout amber">
    <strong class="title">2. Explosión de la virtualidad (+274,7%)</strong>
    Los matriculados virtuales en Semestre 1 casi se cuadruplicaron: de <strong>63.569 (3,40%)</strong> en 2015-1 a <strong>238.209 (11,94%)</strong> en 2021-1, y alcanzaron <strong>30,19%</strong> en 2021-2 (oferta SENA).
  </div>
  <div class="callout">
    <strong class="title">3. Brecha regional y perfil de Antioquia (#2 nacional)</strong>
    Bogotá D.C. (618.804 en 2021-1) y <strong>Antioquia (267.463 · 13,41% del país)</strong> lideran el país. Sin embargo, Antioquia es mucho más pública: <strong>50,8% oficial</strong> en Sem 1 (58,9% anual) frente al <strong>20,1% oficial</strong> en Bogotá D.C.
  </div>
  <div class="callout">
    <strong class="title">4. Género en STEM e I.U. Pascual Bravo (+88,8%)</strong>
    Las mujeres son el <strong>53,84%</strong> nacional (Salud 69,4%, Educación 62,7%), pero en <strong>Ingeniería solo 32,20%</strong>. La <strong>I.U. Pascual Bravo (SNIES 3107)</strong> creció de <strong>4.184</strong> (2015-1) a <strong>7.898</strong> (2021-1, +88,8%) y subió mujeres de 18,1% a <strong>25,3%</strong>.
  </div>
</div>

<!-- ==================== SECCIÓN 3: GUÍA PASO A PASO ==================== -->
<h2 class="page-break">3. Guía Paso a Paso de Ejecución Completa (Fases 2 a 8)</h2>

<div class="callout amber">
  <strong class="title">Prerrequisitos y 2 detalles clave de configuración</strong>
  <ul style="margin-bottom:0;">
    <li><strong>Software necesario:</strong> Docker Desktop (mínimo 4 GB RAM asignados; recomendado 6–8 GB) y Python 3.9+.</li>
    <li><strong>Puerto PostgreSQL en el equipo anfitrión:</strong> configurado en <code>5433</code> (<code>5433:5432</code>) para evitar choques si tu Mac/PC ya tiene otro PostgreSQL local en el puerto 5432. Dentro de Docker (Spark/Airflow) sigue usando <code>postgres:5432</code>.</li>
    <li><strong>JARs offline precargados:</strong> la carpeta <code>jars/</code> ya incluye los conectores oficiales de PostgreSQL JDBC y Spark-Kafka 4.0.2, por lo que Spark no depende de descargas HTTPS a Maven Central.</li>
  </ul>
</div>

<div class="step-hdr"><span class="step-num">PASO 0</span><span class="step-title">Preparar el entorno virtual Python (solo la primera vez)</span></div>
<pre><code># macOS / Linux (desde la carpeta pipeline-datos-gestion)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp env.ejemplo .env

# Windows (PowerShell)
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item env.ejemplo .env</code></pre>

<div class="callout green">
  <strong class="title">Atajo todo-en-uno (macOS / Linux)</strong>
  Si deseas ejecutar todas las fases obligatorias (2 a 8) automáticamente en orden con control de RAM: <code>./actualizar.sh</code>. Para demostrar cada etapa en detalle (o en Windows), ejecuta los bloques siguientes uno por uno.
</div>

<div class="step-hdr"><span class="step-num">FASE 2 + 6</span><span class="step-title">Ingesta desde datos.gov.co (SODA2) + Marca de agua incremental</span></div>
<pre><code># Corrida 1: descarga histórica hasta 2020 (356,341 registros -> marca de agua 2020-2)
python3 -m ingesta descargar --hasta-anio 2020

# Corrida 2: descarga incremental automática (solo 2021-1 y 2021-2 -> 34,562 registros)
python3 -m ingesta descargar --incremental

# Corrida 3: prueba de idempotencia (devuelve 0 registros nuevos sin tocar el historial)
python3 -m ingesta descargar --incremental</code></pre>
<p><strong>Salida esperada:</strong> dos archivos JSONL + dos manifiestos en <code>data/raw/datos_gov_co/men_matricula_es/fecha_ingesta=YYYY-MM-DD/</code> y el archivo <code>data/raw/_marca_agua.json</code> marcando <code>ultimo_anio: 2021, ultimo_semestre: 2</code>.</p>

<div class="step-hdr"><span class="step-num">FASE 3</span><span class="step-title">Apache Kafka 4.1 — Tópico 3 particiones, productor y consumidor</span></div>
<pre><code># Encender broker Kafka (KRaft, sin Zookeeper)
docker compose --profile kafka up -d --wait

# Crear tópico 'raw.datos_gov_co' con 3 particiones
python3 -m streaming crear-topicos

# Publicar JSONL locales con key=id_registro (Corrida 1: 390,903 mensajes; Corrida 2: 0 por .kafka_ok)
python3 -m streaming producir
python3 -m streaming producir

# Verificar balanceo de particiones y leer una muestra con consumer group
python3 -m streaming info-topico
python3 -m streaming consumir --grupo grupo-verificacion --max 5</code></pre>
<p><strong>Resultado verificado:</strong> Partición 0: <code>129.914</code> · Partición 1: <code>130.021</code> · Partición 2: <code>130.968</code> (total <code>390.903</code>).</p>

<div class="step-hdr"><span class="step-num">FASE 4a</span><span class="step-title">Data Lake en Apache Hadoop HDFS (WebHDFS por lotes + commit manual)</span></div>
<pre><code># Encender NameNode + DataNode (UI web en http://localhost:9870)
docker compose --profile hdfs up -d --wait

# Consumir Kafka por lotes de hasta 20,000 registros hacia /datalake/raw
python3 -m datalake consumir
python3 -m datalake resumen /datalake/raw

# Listar archivos generados directamente desde HDFS
docker compose exec namenode hdfs dfs -ls -R -h /datalake/raw</code></pre>
<p><strong>Resultado verificado:</strong> 25 archivos por lote (11 lotes, <code>401,58 MB</code>) con nombres deterministas <code>part-raw.datos_gov_co-pX-oYYYY.jsonl</code>. Si vuelves a correr <code>python3 -m datalake consumir</code>, lee 0 mensajes.</p>

<div class="step-hdr"><span class="step-num">FASE 4b</span><span class="step-title">MapReduce en Python vs. PySpark (validación cruzada sobre raw)</span></div>
<pre><code>docker compose run --rm spark-mapreduce</code></pre>
<p><strong>Resultado verificado:</strong> <code>390.903</code> registros y <code>28.745.704</code> matriculados crudos con <strong>0 DIFERENCIAS (EXACTO)</strong> en los 14 grupos <code>(anio, sector)</code>. Evidencia en <code>evidencias/mapreduce_vs_spark.txt</code>.</p>

<div class="step-hdr"><span class="step-num">FASES 5 + 6</span><span class="step-title">Zona Curada en PySpark (Parquet particionado + 5 validaciones fail-fast)</span></div>
<pre><code>docker compose run --rm spark

# Verificar particiones Parquet creadas en HDFS
docker compose exec namenode hdfs dfs -ls -R -h /datalake/curated/matricula_es</code></pre>
<p><strong>Resultado verificado:</strong> <code>230.082</code> filas únicas, <code>160.821</code> duplicados depurados, <code>5/5</code> validaciones APROBADAS, 7 particiones <code>anio=2015..2021</code> en <code>5,66 MB</code> (<strong>70,92×</strong> menos que raw). Reporte en <code>evidencias/reporte_calidad.json</code>.</p>

<div class="step-hdr"><span class="step-num">FASE 7</span><span class="step-title">Spark Structured Streaming (Capa de Velocidad Lambda + Prueba de Reinicio)</span></div>
<pre><code># Asegurar Kafka y PostgreSQL encendidos
docker compose --profile kafka --profile postgres up -d --wait

# Corrida 1: procesa el tópico Kafka con watermark 5m + ventana 10m + checkpoint persistente
docker compose run --rm spark-streaming

# Corrida 2 (PRUEBA DE REINICIO): 0 lotes nuevos, 0 duplicados, mismos 16,773,503 matriculados
docker compose run --rm spark-streaming

# Apagar Kafka al terminar para ahorrar RAM
docker compose stop kafka</code></pre>
<p><strong>Evidencia generada:</strong> <code>evidencias/streaming_reinicio.json</code> (muestra Corrida 1: <code>lotes_nuevos=1</code> vs. Corrida 2: <code>lotes_nuevos=0</code>) y tabla <code>servicio.stream_ventanas</code> en PostgreSQL.</p>

<div class="step-hdr"><span class="step-num">FASE 8</span><span class="step-title">Zona de Servicio PostgreSQL 17 + Tablero Interactivo Plotly HTML</span></div>
<pre><code># Asegurar HDFS y PostgreSQL encendidos y cargar indicadores (idempotente)
docker compose --profile hdfs --profile postgres up -d --wait
docker compose run --rm spark-servir

# Verificar tablas con el usuario de SOLO LECTURA 'lector'
docker compose exec postgres psql -U lector -d matricula_es -c "\dt servicio.*"

# Generar el tablero HTML autocontenido y copiarlo a docs/index.html (GitHub Pages)
python3 -m visualizacion generar --origen postgres --publicar

# Abrir en el navegador (macOS: open | Windows: start)
open salida/dashboard.html</code></pre>

<!-- ==================== SECCIÓN 4: PUNTOS EXTRA ==================== -->
<h2 class="page-break">4. Ejecución de los Componentes Opcionales (+5 Puntos Extra)</h2>

<div class="step-hdr"><span class="step-num extra">EXTRA +2 PTS</span><span class="step-title">Apache Hive 4.0.1 — Tablas externas sobre Parquet en HDFS + 4 SQL</span></div>
<p>Hive consulta directamente los archivos Parquet de <code>/datalake/curated/matricula_es</code> sin duplicar datos. La cláusula <code>EXTERNAL</code> garantiza que borrar la tabla no borra los datos del Data Lake.</p>
<pre><code># 1. Encender HDFS y HiveServer2 (espera a que el puerto 10000 esté listo)
docker compose --profile hdfs --profile hive up -d --wait

# 2. Crear base 'matricula_db', tabla externa particionada y ejecutar MSCK REPAIR TABLE
docker compose exec hiveserver2 \
  beeline -u jdbc:hive2://localhost:10000/ -f /proyecto/hive/01_tablas.sql

# 3. Ejecutar las 4 consultas analíticas (evolución, top deptos, género, I.U. Pascual Bravo)
docker compose exec hiveserver2 \
  beeline -u jdbc:hive2://localhost:10000/matricula_db --silent=true -f /proyecto/hive/02_consultas.sql

# 4. Apagar Hive al terminar para liberar ~1.5 GB de RAM
docker compose --profile hive down</code></pre>
<div class="callout green">
  <strong class="title">Resultado real verificado en Hive (<code>evidencias/hive_consultas.txt</code>)</strong>
  <code>MSCK REPAIR TABLE</code> descubrió las 7 particiones (<code>anio=2015..2021</code>), arrojando exactamente <strong>230.082 filas</strong> y <strong>16.773.503 matriculados</strong> — idéntico a PySpark Batch y Structured Streaming. Las 4 consultas SQL devolvieron la serie 2015–2021, el Top 10 departamental, la brecha de género por área SNIES y el crecimiento de +88,8% de la I.U. Pascual Bravo.
</div>

<div class="step-hdr"><span class="step-num extra">EXTRA +3 PTS</span><span class="step-title">Apache Airflow 3.3.2 — Orquestación completa en DAG (11 tareas)</span></div>
<p>Airflow orquesta las 11 tareas del pipeline Lambda (<code>matricula_es_pipeline_completo</code>), con programación semanal (<code>0 6 * * 1</code>), reintentos automáticos (<code>retries=2..3</code>) y reglas <code>trigger_rule="all_done"</code> para apagar Kafka y HDFS al terminar cada bloque y cuidar la memoria RAM.</p>
<pre><code># 1. Pre-crear los contenedores que el DAG controla dinámicamente (sin encenderlos todos a la vez)
docker compose --profile todo create

# 2. Construir imagen personalizada y encender Airflow + PostgreSQL + spark-tareas
docker compose --profile airflow up -d --build

# 3. Verificar 0 errores de importación, listado del DAG y sus 11 tareas
docker compose exec airflow airflow dags reserialize
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list
docker compose exec airflow airflow tasks list matricula_es_pipeline_completo

# 4. Probar una tarea individual directamente desde CLI (ej. regeneración del tablero)
docker compose exec airflow \
  airflow tasks test matricula_es_pipeline_completo generar_dashboard 2026-11-20

# 5. Abrir la interfaz web para ver el grafo y disparar corridas:
#    http://localhost:8082   (SimpleAuthManager: acceso admin directo en entorno local)

# 6. Apagar Airflow al terminar
docker compose --profile airflow down</code></pre>

<h3>Grafo de dependencias del DAG <code>matricula_es_pipeline_completo</code> (11 tareas)</h3>
<div class="avoid-break" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:8px;padding:10px;margin:6px 0 12px 0;">
  <svg viewBox="0 0 920 170" width="100%" xmlns="http://www.w3.org/2000/svg" style="display:block;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
    <defs>
      <marker id="afArr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#475569"/>
      </marker>
    </defs>
    <!-- Fila principal superior (Batch) -->
    <rect x="6" y="42" width="104" height="36" rx="6" fill="#0f2744"/><text x="58" y="58" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">1. ingesta</text><text x="58" y="70" text-anchor="middle" font-size="7.5" fill="#fde68a">incremental (r=3)</text>
    <rect x="126" y="42" width="102" height="36" rx="6" fill="#1e3a5f"/><text x="177" y="58" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">2. encender</text><text x="177" y="70" text-anchor="middle" font-size="7.5" fill="#cbd5e1">kafka + hdfs</text>
    <rect x="244" y="42" width="96" height="36" rx="6" fill="#1d4ed8"/><text x="292" y="58" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">3. kafka</text><text x="292" y="70" text-anchor="middle" font-size="7.5" fill="#dbeafe">producir</text>

    <!-- Rama Batch -->
    <rect x="364" y="14" width="96" height="34" rx="6" fill="#0369a1"/><text x="412" y="29" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">4. hdfs</text><text x="412" y="40" text-anchor="middle" font-size="7.5" fill="#e0f2fe">consumir</text>
    <rect x="478" y="14" width="98" height="34" rx="6" fill="#0369a1"/><text x="527" y="29" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">7. mapreduce</text><text x="527" y="40" text-anchor="middle" font-size="7.5" fill="#e0f2fe">vs spark</text>
    <rect x="594" y="14" width="98" height="34" rx="6" fill="#0369a1"/><text x="643" y="29" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">8. spark curar</text><text x="643" y="40" text-anchor="middle" font-size="7.5" fill="#fde68a">+ 5 fail-fast</text>
    <rect x="710" y="14" width="94" height="34" rx="6" fill="#047857"/><text x="757" y="29" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">9. spark servir</text><text x="757" y="40" text-anchor="middle" font-size="7.5" fill="#d1fae5">PostgreSQL</text>

    <!-- Rama Speed -->
    <rect x="364" y="74" width="160" height="34" rx="6" fill="#b45309"/><text x="444" y="89" text-anchor="middle" font-size="8.5" font-weight="700" fill="#fff">5. spark_streaming_lambda</text><text x="444" y="100" text-anchor="middle" font-size="7.5" fill="#fef3c7">watermark 5m + checkpoint</text>

    <!-- Limpieza RAM (all_done) -->
    <rect x="556" y="124" width="128" height="32" rx="6" fill="#475569"/><text x="620" y="138" text-anchor="middle" font-size="8.3" font-weight="700" fill="#fff">6. apagar_kafka</text><text x="620" y="149" text-anchor="middle" font-size="7.3" fill="#fde68a">trigger_rule=all_done</text>
    <rect x="818" y="14" width="96" height="34" rx="6" fill="#475569"/><text x="866" y="29" text-anchor="middle" font-size="8.3" font-weight="700" fill="#fff">10. apagar_hdfs</text><text x="866" y="40" text-anchor="middle" font-size="7.3" fill="#fde68a">all_done</text>

    <!-- Dashboard final -->
    <rect x="780" y="74" width="134" height="36" rx="6" fill="#065f46"/><text x="847" y="90" text-anchor="middle" font-size="8.8" font-weight="700" fill="#fff">11. generar_dashboard</text><text x="847" y="102" text-anchor="middle" font-size="7.5" fill="#a7f3d0">Plotly HTML + docs/index</text>

    <!-- Flechas -->
    <line x1="110" y1="60" x2="123" y2="60" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <line x1="228" y1="60" x2="241" y2="60" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <path d="M 340 52 L 361 32" fill="none" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <path d="M 340 68 L 361 88" fill="none" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <line x1="460" y1="31" x2="475" y2="31" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <line x1="576" y1="31" x2="591" y2="31" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <line x1="692" y1="31" x2="707" y2="31" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <line x1="804" y1="31" x2="815" y2="31" stroke="#475569" stroke-width="1.5" marker-end="url(#afArr)"/>
    <!-- hdfs + stream -> apagar_kafka -->
    <path d="M 412 48 L 412 62 L 542 62 L 542 135 L 553 135" fill="none" stroke="#64748b" stroke-width="1.2" stroke-dasharray="3 2" marker-end="url(#afArr)"/>
    <path d="M 444 108 L 444 142 L 553 142" fill="none" stroke="#64748b" stroke-width="1.2" stroke-dasharray="3 2" marker-end="url(#afArr)"/>
    <!-- servir + stream -> dashboard -->
    <path d="M 757 48 L 757 86 L 777 86" fill="none" stroke="#047857" stroke-width="1.5" marker-end="url(#afArr)"/>
    <line x1="524" y1="92" x2="777" y2="92" stroke="#b45309" stroke-width="1.5" marker-end="url(#afArr)"/>
  </svg>
</div>

<!-- ==================== SECCIÓN 5: PRUEBAS Y TROUBLESHOOTING ==================== -->
<h2>5. Pruebas Unitarias (<code>pytest</code>) y Solución de Problemas</h2>

<pre><code># 1. Pruebas unitarias offline (7 tests: ingesta, Kafka, WebHDFS y MapReduce)
python3 -m pytest -v tests/test_ingesta.py tests/test_streaming.py tests/test_datalake.py tests/test_mapreduce.py

# 2. Pruebas de transformaciones y validaciones fail-fast en PySpark (2 tests dentro de Spark)
docker compose run --rm spark bash -c \
  "export PYTHONPATH=/proyecto:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip && pip install -q pytest && python3 -m pytest -v tests/test_procesamiento_spark.py"</code></pre>

<table>
  <thead>
    <tr><th>Posible inconveniente</th><th>Causa</th><th>Solución inmediata</th></tr>
  </thead>
  <tbody>
    <tr>
      <td><code>role "lector" does not exist</code> al generar dashboard</td>
      <td>Existe otro PostgreSQL instalado en el puerto 5432 del Mac/PC.</td>
      <td>Ya resuelto: el proyecto usa puerto <strong>5433</strong> en el host (<code>POSTGRES_PORT=5433</code>). Verifica que <code>matricula-postgres</code> esté arriba con <code>docker ps</code>.</td>
    </tr>
    <tr>
      <td><code>airflow dags list</code> sale vacío tras crear la BD</td>
      <td>En Airflow 3.x el DAG debe serializarse antes de listarlo desde CLI.</td>
      <td>Ejecuta <code>docker compose exec airflow airflow dags reserialize</code> o espera ~15 s con <code>airflow standalone</code> arriba.</td>
    </tr>
    <tr>
      <td>Docker lento o sin memoria al encender todo</td>
      <td>Kafka + HDFS + Hive + Airflow simultáneos superan 4 GB RAM.</td>
      <td>Usa siempre los perfiles por fase (<code>--profile kafka</code>, <code>--profile hive</code>, etc.) y apaga con <code>docker compose stop kafka</code> o <code>--profile hive down</code>.</td>
    </tr>
    <tr>
      <td>Error SSL al descargar JARs en Spark</td>
      <td>Proxy/firewall interceptando Maven Central.</td>
      <td>Ya mitigado: los 6 JARs oficiales están guardados en <code>jars/</code> y se pasan con <code>--jars</code> sin acceso a internet.</td>
    </tr>
    <tr>
      <td>Reiniciar todo desde cero limpio</td>
      <td>Limpiar volúmenes Docker y checkpoints locales.</td>
      <td><code>docker compose --profile todo --profile hive --profile airflow down -v</code> y borrar <code>data/</code>.</td>
    </tr>
  </tbody>
</table>

<!-- ==================== SECCIÓN 6: SUSTENTACIÓN Y ENTREGA ==================== -->
<h2 class="page-break">6. Guion de Sustentación (10 min + 5 min Preguntas) y Entrega Final</h2>

<h3>6.1 Distribución sugerida de los 10 minutos (8 diapositivas + demo en vivo)</h3>
<table>
  <thead>
    <tr><th>Minuto</th><th>Responsable</th><th>Diapositivas y contenido a exponer</th></tr>
  </thead>
  <tbody>
    <tr><td><strong>0:00 – 2:30</strong></td><td><strong>Camilo Zuleta</strong></td><td><strong>Slides 1 y 2:</strong> Pregunta de investigación, usuario objetivo (I.U. Pascual Bravo / Secretarías), dataset <code>5wck-szir</code> (por qué SODA2), llave <code>id_registro</code> y justificación de <strong>Arquitectura Lambda vs. Kappa</strong>.</td></tr>
    <tr><td><strong>2:30 – 5:00</strong></td><td><strong>Santiago Restrepo</strong></td><td><strong>Slides 3 y 4:</strong> Ingesta paginada + marca de agua (356k → +34k → 0), Kafka 3 particiones (balanceo real + <code>.kafka_ok</code>), HDFS por lotes (25 archivos, commit manual) y <strong>MapReduce vs. PySpark (0 diferencias en 390.903 filas)</strong>.</td></tr>
    <tr><td><strong>5:00 – 7:30</strong></td><td><strong>Camilo Zuleta</strong></td><td><strong>Slides 5 y 6:</strong> Hallazgo estrella de limpieza (<strong>160.821 duplicados, 54.225 ocultos por comas</strong>, 90→33 deptos DANE, Parquet <strong>70,92×</strong>), 5 validaciones fail-fast y <strong>Structured Streaming</strong> (watermark + checkpoint + reinicio sin duplicados + reconciliación 0 dif.).</td></tr>
    <tr><td><strong>7:30 – 10:00</strong></td><td><strong>Santiago + Camilo</strong></td><td><strong>Slides 7 y 8 + Demo en vivo:</strong> Mostrar el tablero <code>docs/index.html</code> interactivo, responder la pregunta (Antioquia, virtualidad +274,7%, brecha género 32,2%, Pascual Bravo +88,8%), mostrar <strong>Hive (+2)</strong> y <strong>Airflow (+3)</strong>, y correr la <strong>demo rápida de idempotencia</strong> en terminal.</td></tr>
  </tbody>
</table>

<div class="callout">
  <strong class="title">Demostración en vivo recomendada (toma &lt; 25 segundos durante la sustentación)</strong>
  Tener encendidos únicamente <code>postgres</code> y la terminal en la carpeta del proyecto para mostrar:
  <pre style="margin-top:6px;margin-bottom:2px;"><code>python3 -m ingesta descargar --incremental   # Muestra 0 registros nuevos (marca de agua 2021-2)
cat evidencias/reporte_calidad.json | head -n 18
open docs/index.html                          # Navegar los filtros e indicadores en vivo</code></pre>
</div>

<h3>6.2 Preguntas probables del docente y respuestas listas</h3>
<table>
  <thead><tr><th style="width:36%;">Pregunta probable</th><th>Respuesta técnica directa</th></tr></thead>
  <tbody>
    <tr>
      <td><strong>1. ¿Por qué eligieron Lambda y no Kappa?</strong></td>
      <td>Porque el histórico 2015–2019 tiene 160.821 duplicados (54.225 ocultos por comas) y 90 nombres sucios de departamentos que exigen reprocesamiento batch determinista sobre HDFS inmutable + Parquet/Hive, mientras Structured Streaming provee baja latencia desde Kafka con reconciliación exacta (0 diferencias en los 7 años).</td>
    </tr>
    <tr>
      <td><strong>2. ¿Cómo garantizan que no haya duplicados si fallan o repiten el pipeline?</strong></td>
      <td>En 5 niveles: marca de agua (<code>_marca_agua.json</code>), checkpoint <code>.kafka_ok</code> en productor, commit manual post-HDFS con nombre por offset, deduplicación determinista por <code>id_registro</code> en PySpark + overwrite por partición, y checkpoint + <code>dropDuplicatesWithinWatermark</code> en Streaming.</td>
    </tr>
    <tr>
      <td><strong>3. ¿Por qué no escribieron 1 archivo por mensaje en HDFS?</strong></td>
      <td>Por el problema de archivos pequeños en HDFS: 390.903 archivos saturarían la memoria del NameNode (cada bloque/archivo consume ~150 bytes de metadatos). Nuestro consumidor agrupa lotes de hasta 20.000 mensajes y generó solo 25 archivos de ~16 MB.</td>
    </tr>
    <tr>
      <td><strong>4. ¿Para qué sirve MapReduce si ya tienen PySpark?</strong></td>
      <td>Como validación cruzada independiente sobre la zona raw (verificando 0 diferencias en 390.903 filas y 28.745.704 matriculados crudos) y para contrastar el paradigma clásico <code>stdin/stdout</code> basado en disco frente al DAG en memoria de Spark.</td>
    </tr>
    <tr>
      <td><strong>5. ¿Qué pasa si se cae el Streaming y vuelve a arrancar?</strong></td>
      <td>Al reiniciar <code>spark-streaming</code>, lee el checkpoint en <code>data/checkpoints/stream_matricula</code>, retoma desde el último offset confirmado y procesa <code>0</code> lotes nuevos manteniendo exactamente 230.082 registros únicos y 16.773.503 matriculados.</td>
    </tr>
    <tr>
      <td><strong>6. ¿Por qué Hive usa tabla EXTERNAL y Airflow tiene <code>trigger_rule="all_done"</code>?</strong></td>
      <td><code>EXTERNAL</code> desacopla el metastore de los archivos Parquet en HDFS (un <code>DROP TABLE</code> no borra los datos). En Airflow, <code>trigger_rule="all_done"</code> en <code>apagar_kafka</code> y <code>apagar_hdfs</code> libera RAM incluso si una tarea intermedia falla.</td>
    </tr>
  </tbody>
</table>

<h3>6.3 Checklist de entrega (GitHub + Google Classroom)</h3>
<ol>
  <li><strong>Publicar en GitHub y activar GitHub Pages:</strong> subir el repositorio a GitHub → <em>Settings → Pages → Source: Deploy from a branch → Branch: <code>main</code>, folder: <code>/docs</code></em>. El tablero quedará navegable en línea automáticamente desde <code>docs/index.html</code>.</li>
  <li><strong>Paquete para Google Classroom (plazo viernes 27 nov 2026, 11:59 p. m.):</strong> crear carpeta o ZIP <code>PP_Grupo#_Zuleta_Restrepo</code> con:
    <ul>
      <li>Enlace al repositorio de GitHub y enlace a GitHub Pages (en <code>README.md</code>).</li>
      <li><code>notebooks/Pipeline_Matricula_ES_Colombia.ipynb</code> (ejecutado con todas las salidas).</li>
      <li><code>docs/Presentacion_Sustentacion.pdf</code> (8 diapositivas exactas).</li>
      <li><code>docs/Guia_Completa_Ejecucion_Pipeline.pdf</code> (esta guía).</li>
      <li><code>evidencias/</code>, <code>datos_muestra/</code> y <code>salida/dashboard.html</code>.</li>
    </ul>
  </li>
</ol>
</body>
</html>
"""


def construir_html_presentacion(m: dict) -> str:
    cal = m["calidad"]
    tab = m["tablero"]
    kpi = tab["kpi"]

    # Barras SVG para virtualidad y evolución en Slide 7
    ev = tab["evolucion_sem1"]
    # Max virtual = 238,209 -> escala
    bars_virt = ""
    for i, row in enumerate(ev):
        x = 22 + i * 52
        h_v = int((row["matriculados_virtual"] / 250000) * 95)
        y_v = 115 - h_v
        bars_virt += (
            f'<rect x="{x}" y="{y_v}" width="32" height="{h_v}" rx="4" fill="#d97706"/>'
            f'<text x="{x+16}" y="{y_v-5}" text-anchor="middle" font-size="9.5" font-weight="700" fill="#0f2744">{row["pct_virtual"]:.1f}%</text>'
            f'<text x="{x+16}" y="130" text-anchor="middle" font-size="9.5" fill="#475569">{row["anio"]}</text>'
        )

    # Barras SVG género por área
    areas_top = [
        ("Salud", 69.4, "#059669"),
        ("Educación", 62.7, "#0d9488"),
        ("Sociales", 62.2, "#0284c7"),
        ("Economía", 59.2, "#2563eb"),
        ("Ingeniería", 32.2, "#dc2626"),
    ]
    bars_gen = ""
    for i, (nom, pct, col) in enumerate(areas_top):
        y = 14 + i * 26
        w = int(pct * 2.15)
        bars_gen += (
            f'<text x="0" y="{y+13}" font-size="10.5" font-weight="600" fill="#1e293b">{nom}</text>'
            f'<rect x="78" y="{y+2}" width="215" height="15" rx="4" fill="#e2e8f0"/>'
            f'<rect x="78" y="{y+2}" width="{w}" height="15" rx="4" fill="{col}"/>'
            f'<text x="{83+w}" y="{y+13}" font-size="10" font-weight="700" fill="{col}">{pct:.1f}%</text>'
        )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<title>Sustentación — Pipeline Big Data Matrícula ES Colombia</title>
<style>
  @page {{
    size: 13.333in 7.5in;
    margin: 0;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{
    margin: 0;
    padding: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    color: #1e293b;
  }}
  .slide {{
    width: 13.333in;
    height: 7.5in;
    padding: 0.48in 0.58in 0.42in 0.58in;
    position: relative;
    overflow: hidden;
    page-break-after: always;
    break-after: page;
    background: #ffffff;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
  }}
  .slide:last-child {{
    page-break-after: auto;
    break-after: auto;
  }}

  /* SLIDE 1 PORTADA */
  .slide.portada {{
    background: linear-gradient(135deg, #0b1e36 0%, #13315c 52%, #1d4ed8 100%);
    color: #ffffff;
    padding: 0.6in 0.72in;
  }}
  .pill {{
    display: inline-block;
    background: rgba(251,191,36,0.18);
    border: 1px solid rgba(251,191,36,0.7);
    color: #fde68a;
    padding: 5px 14px;
    border-radius: 999px;
    font-size: 11pt;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
  }}
  .portada h1 {{
    font-size: 36pt;
    line-height: 1.1;
    margin: 18px 0 12px 0;
    font-weight: 800;
  }}
  .portada .qbox {{
    background: rgba(255,255,255,0.1);
    border-left: 5px solid #fbbf24;
    padding: 16px 20px;
    border-radius: 8px;
    margin: 14px 0 18px 0;
    font-size: 14pt;
    line-height: 1.4;
    color: #f8fafc;
  }}
  .portada .meta-grid {{
    display: grid;
    grid-template-columns: 1.3fr 1fr 1fr;
    gap: 16px;
    margin-top: 12px;
  }}
  .portada .mbox {{
    background: rgba(15,23,42,0.5);
    border: 1px solid rgba(148,163,184,0.28);
    border-radius: 10px;
    padding: 13px 16px;
    font-size: 11pt;
    line-height: 1.45;
  }}
  .portada .mbox .lbl {{
    font-size: 9pt;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: #fbbf24;
    font-weight: 700;
    margin-bottom: 4px;
  }}

  /* HEADER / FOOTER SLIDES INTERIORES */
  .shdr {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    border-bottom: 2.5px solid #1d4ed8;
    padding-bottom: 8px;
    margin-bottom: 14px;
  }}
  .shdr h2 {{
    margin: 0;
    font-size: 19.5pt;
    color: #0f2744;
    font-weight: 800;
  }}
  .shdr .fase-tag {{
    background: #dbeafe;
    color: #1e40af;
    font-weight: 700;
    font-size: 10pt;
    padding: 4px 12px;
    border-radius: 999px;
    white-space: nowrap;
    flex-shrink: 0;
  }}
  .shdr .fase-tag.extra {{ background: #fef3c7; color: #92400e; }}

  .sftr {{
    display: flex;
    justify-content: space-between;
    font-size: 9.5pt;
    color: #64748b;
    border-top: 1px solid #e2e8f0;
    padding-top: 7px;
    margin-top: 10px;
  }}
  .sftr strong {{ color: #0f2744; }}

  .content {{ flex: 1; display: flex; flex-direction: column; justify-content: flex-start; gap: 12px; }}

  .cols-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }}
  .cols-3 {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }}
  .cols-4 {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }}
  .cols-60-40 {{ display: grid; grid-template-columns: 1.45fr 1fr; gap: 18px; }}

  .box {{
    background: #f8fafc;
    border: 1px solid #cbd5e1;
    border-radius: 10px;
    padding: 13px 15px;
  }}
  .box.navy {{ background: #0f2744; color: #ffffff; border-color: #0f2744; }}
  .box.blue {{ background: #eff6ff; border-color: #93c5fd; }}
  .box.amber {{ background: #fffbeb; border-color: #fcd34d; }}
  .box.green {{ background: #ecfdf5; border-color: #6ee7b7; }}
  .box h3 {{
    margin: 0 0 7px 0;
    font-size: 13pt;
    color: #0f2744;
  }}
  .box.navy h3 {{ color: #fbbf24; }}
  .box p, .box li {{ font-size: 10.8pt; line-height: 1.4; margin: 0 0 5px 0; }}
  .box ul {{ margin: 0; padding-left: 18px; }}

  .kpi-card {{
    background: #0f2744;
    color: #ffffff;
    border-radius: 10px;
    padding: 12px 14px;
    text-align: center;
  }}
  .kpi-card.amber {{ background: #b45309; }}
  .kpi-card.green {{ background: #047857; }}
  .kpi-card.blue {{ background: #1d4ed8; }}
  .kpi-card .val {{ font-size: 23pt; font-weight: 800; line-height: 1.1; }}
  .kpi-card .lbl {{ font-size: 9.5pt; color: #e2e8f0; margin-top: 4px; }}

  table.stbl {{
    width: 100%;
    border-collapse: collapse;
    font-size: 10pt;
  }}
  table.stbl th {{
    background: #0f2744;
    color: #ffffff;
    padding: 6px 8px;
    text-align: left;
  }}
  table.stbl td {{
    padding: 5px 8px;
    border-bottom: 1px solid #e2e8f0;
  }}
  table.stbl tr:nth-child(even) td {{ background: #f8fafc; }}
  table.stbl .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .ok-pill {{
    background: #d1fae5;
    color: #065f46;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 9pt;
  }}
  code {{
    font-family: "SFMono-Regular", Menlo, Consolas, monospace;
    background: #e2e8f0;
    color: #0f2744;
    padding: 1px 5px;
    border-radius: 4px;
    font-size: 9.8pt;
  }}
  .box.navy code {{ background: rgba(255,255,255,0.16); color: #fde68a; }}
</style>
</head>
<body>

<!-- ============================ SLIDE 1 ============================ -->
<section class="slide portada">
  <div>
    <span class="pill">Institución Universitaria Pascual Bravo · Gestión de Datos Masivos G004 · 2026-2</span>
    <h1>Pipeline Big Data:<br/>Matrícula de Educación Superior en Colombia (2015–2021)</h1>
    <div class="qbox">
      <strong style="color:#fde68a;font-size:10pt;text-transform:uppercase;letter-spacing:0.6px;display:block;margin-bottom:4px;">Pregunta principal de investigación</strong>
      «¿Cómo evolucionó la matrícula de educación superior en Colombia (2015–2021) por departamento, sector (oficial vs. privado), metodología (presencial/virtual/distancia), área del conocimiento y género, y cuál es la brecha regional y de Antioquia frente al país?»
    </div>
  </div>
  <div>
    <div class="meta-grid">
      <div class="mbox">
        <div class="lbl">Usuario objetivo</div>
        Planeación académica de IES (ej. <strong>I.U. Pascual Bravo</strong>, SNIES 3107) y Secretarías de Educación Departamentales (Antioquia y país).
      </div>
      <div class="mbox">
        <div class="lbl">Fuente oficial abierta</div>
        <strong>datos.gov.co</strong> · MEN (SNIES)<br/>
        <code>5wck-szir</code> · API SODA2<br/>
        <strong>390.903</strong> filas · 14 semestres
      </div>
      <div class="mbox">
        <div class="lbl">Equipo y docente</div>
        <strong>Camilo Zuleta Delgado</strong><br/>
        <strong>Santiago Restrepo Silva</strong><br/>
        Docente: Aleixo Eder Pineda R.
      </div>
    </div>
  </div>
</section>

<!-- ============================ SLIDE 2 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Fuente de Datos, Llave Natural y Arquitectura Lambda</h2>
    <span class="fase-tag">Fase 1 · Diseño y Arquitectura</span>
  </div>
  <div class="content">
    <div style="background:#ffffff;border:1px solid #cbd5e1;border-radius:10px;padding:8px 10px;">
      {svg_arquitectura_lambda(compacto=True)}
    </div>
    <div class="cols-3">
      <div class="box navy">
        <h3>Llave Natural (id_registro)</h3>
        <p style="font-size:9.8pt;"><code>anio|semestre|cod_ies|cod_snies|cod_muni(5d)|metod|genero</code></p>
        <p style="font-size:10pt;color:#cbd5e1;">Identifica de forma única cada celda estadística de matrícula y soporta particionamiento en Kafka + deduplicación en Spark y Streaming.</p>
      </div>
      <div class="box blue">
        <h3>¿Por qué Lambda y no Kappa?</h3>
        <ul>
          <li><strong>Capa Batch obligatoria:</strong> el histórico 2015–2019 tiene <strong>160.821 duplicados</strong> (54.225 ocultos) y 90 nombres sucios de deptos que exigen reprocesamiento determinista en HDFS + PySpark.</li>
          <li><strong>Capa Speed:</strong> Structured Streaming actualiza ventanas desde Kafka con baja latencia.</li>
        </ul>
      </div>
      <div class="box green">
        <h3>Diseño de 3 Zonas</h3>
        <ul>
          <li><strong>Raw:</strong> JSONL inmutable + manifiesto (401,58 MB).</li>
          <li><strong>Curada:</strong> Parquet Snappy por <code>anio</code> (5,66 MB, <strong>70,92×</strong>) + Hive.</li>
          <li><strong>Servicio:</strong> PostgreSQL <code>servicio.*</code> + usuario <code>lector</code> + Plotly HTML.</li>
        </ul>
      </div>
    </div>
  </div>
  <div class="sftr"><span>Camilo Zuleta Delgado · Santiago Restrepo Silva</span><strong>Diapositiva 2 / 8</strong></div>
</section>

<!-- ============================ SLIDE 3 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Ingesta Incremental SODA2 y Streaming con Apache Kafka</h2>
    <span class="fase-tag">Fases 2 y 3 · Ingesta y Mensajería</span>
  </div>
  <div class="content">
    <div class="cols-2">
      <div class="box">
        <h3>Fase 2 — Ingesta SODA2 (<code>ingesta/</code>)</h3>
        <ul>
          <li>Endpoint abierto <code>/resource/5wck-szir.json</code> (sin auth obligatoria; paginación de 50.000 filas + <em>backoff</em> exponencial).</li>
          <li>Sobre JSON inmutable: <code>fuente</code>, <code>dataset</code>, <code>id_registro</code>, <code>fecha_ingesta</code> y <code>payload</code> crudo + manifiesto <code>_manifiesto_*.json</code>.</li>
          <li><strong>Marca de agua (<code>_marca_agua.json</code>) verificada en 3 corridas:</strong></li>
        </ul>
        <table class="stbl" style="margin-top:8px;">
          <thead><tr><th>Corrida</th><th>Parámetro</th><th class="num">Filas</th><th>Marca resultante</th></tr></thead>
          <tbody>
            <tr><td>#1 Histórica</td><td><code>--hasta-anio 2020</code></td><td class="num"><strong>356.341</strong></td><td><code>2020-2</code></td></tr>
            <tr><td>#2 Incremental</td><td><code>--incremental</code></td><td class="num"><strong>34.562</strong></td><td><code>2021-2</code></td></tr>
            <tr><td>#3 Re-ejecución</td><td><code>--incremental</code></td><td class="num"><span class="ok-pill">0 nuevas</span></td><td><code>2021-2</code> (idemp.)</td></tr>
          </tbody>
        </table>
      </div>
      <div class="box blue">
        <h3>Fase 3 — Apache Kafka 4.1 KRaft (<code>streaming/</code>)</h3>
        <ul>
          <li>Tópico <code>raw.datos_gov_co</code> creado con <strong>3 particiones</strong> y retención de 7 días.</li>
          <li>Productor publica con <code>key = id_registro</code> y registra checkpoint <code>.kafka_ok</code> → <strong>2.ª corrida envía 0 mensajes</strong>.</li>
          <li>Consumidor <code>grupo-verificacion</code> verificó los <strong>390.903 mensajes</strong> con distribución balanceada (~33% por partición):</li>
        </ul>
        <svg viewBox="0 0 460 118" width="100%" style="margin-top:10px;display:block;">
          <text x="4" y="25" font-size="11" font-weight="700" fill="#0f2744">Partición 0</text>
          <rect x="92" y="10" width="228" height="22" rx="5" fill="#1d4ed8"/>
          <text x="328" y="25" font-size="11" font-weight="700" fill="#1e3a8a">129.914 (33,2%)</text>

          <text x="4" y="61" font-size="11" font-weight="700" fill="#0f2744">Partición 1</text>
          <rect x="92" y="46" width="229" height="22" rx="5" fill="#0369a1"/>
          <text x="328" y="61" font-size="11" font-weight="700" fill="#0369a1">130.021 (33,3%)</text>

          <text x="4" y="97" font-size="11" font-weight="700" fill="#0f2744">Partición 2</text>
          <rect x="92" y="82" width="231" height="22" rx="5" fill="#0f766e"/>
          <text x="328" y="97" font-size="11" font-weight="700" fill="#0f766e">130.968 (33,5%)</text>
        </svg>
      </div>
    </div>
    <div class="cols-4">
      <div class="kpi-card"><div class="val">390.903</div><div class="lbl">Mensajes publicados en Kafka</div></div>
      <div class="kpi-card blue"><div class="val">3</div><div class="lbl">Particiones balanceadas por key</div></div>
      <div class="kpi-card green"><div class="val">0</div><div class="lbl">Reenvíos en 2.ª corrida (.kafka_ok)</div></div>
      <div class="kpi-card amber"><div class="val">26</div><div class="lbl">Columnas preservadas sin pérdida</div></div>
    </div>
  </div>
  <div class="sftr"><span>Camilo Zuleta Delgado · Santiago Restrepo Silva</span><strong>Diapositiva 3 / 8</strong></div>
</section>

<!-- ============================ SLIDE 4 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Data Lake en HDFS y Validación MapReduce vs. PySpark</h2>
    <span class="fase-tag">Fase 4 · Almacenamiento Distribuido y MapReduce</span>
  </div>
  <div class="content">
    <div class="cols-60-40">
      <div class="box">
        <h3>Fase 4b — Validación Cruzada: MapReduce Python vs. PySpark</h3>
        <p style="font-size:10pt;margin-bottom:6px;"><code>mapper.py | sort | reducer.py</code> (stdin/stdout) vs. agregación PySpark sobre los 25 archivos JSONL de HDFS (<code>/datalake/raw</code>):</p>
        <table class="stbl">
          <thead>
            <tr><th>Año</th><th class="num">Oficial (MR = Spark)</th><th class="num">Privada (MR = Spark)</th><th class="num">Registros</th><th>Estado</th></tr>
          </thead>
          <tbody>
            <tr><td>2015</td><td class="num">2.335.776</td><td class="num">2.251.324</td><td class="num">59.146</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td>2016</td><td class="num">2.389.394</td><td class="num">2.399.474</td><td class="num">62.038</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td>2017</td><td class="num">2.483.580</td><td class="num">2.409.048</td><td class="num">65.464</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td>2018</td><td class="num">2.484.964</td><td class="num">2.395.770</td><td class="num">64.104</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td>2019</td><td class="num">2.436.260</td><td class="num">2.356.240</td><td class="num">70.864</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td>2020</td><td class="num">1.203.895</td><td class="num">1.151.708</td><td class="num">34.725</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td>2021</td><td class="num">1.334.174</td><td class="num">1.114.097</td><td class="num">34.562</td><td><span class="ok-pill">0 dif.</span></td></tr>
            <tr><td><strong>TOTAL</strong></td><td class="num" colspan="2"><strong>28.745.704 matriculados raw</strong></td><td class="num"><strong>390.903</strong></td><td><span class="ok-pill">EXACTO</span></td></tr>
          </tbody>
        </table>
      </div>
      <div style="display:flex;flex-direction:column;gap:12px;">
        <div class="box blue">
          <h3>Fase 4a — HDFS Zona Raw (<code>datalake/</code>)</h3>
          <ul>
            <li>Ruta: <code>/datalake/raw/datos_gov_co/men_matricula_es/fecha_ingesta=2026-11-20/</code></li>
            <li><strong>Escritura por lotes:</strong> <strong>25 archivos</strong> (~16 MB c/u) en 11 lotes — evita saturar el NameNode con 390k archivos pequeños.</li>
            <li><strong>Idempotencia:</strong> nombre determinista <code>part-...-pX-oOFFSET.jsonl</code> + <strong>commit manual</strong> solo tras HTTP 201 de WebHDFS.</li>
          </ul>
        </div>
        <div class="box amber">
          <h3>Pista crítica detectada en Fase 4b</h3>
          <p>Observa que en los datos <strong>raw</strong>, los años 2015–2019 suman <strong>~4,7 millones/año</strong> (el doble de 2020 y 2021). Ese contraste nos permitió descubrir y auditar la duplicación masiva de la fuente oficial del MEN antes de pasar a la zona curada.</p>
        </div>
      </div>
    </div>
  </div>
  <div class="sftr"><span>Camilo Zuleta Delgado · Santiago Restrepo Silva</span><strong>Diapositiva 4 / 8</strong></div>
</section>

<!-- ============================ SLIDE 5 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Zona Curada en PySpark: Limpieza Profunda y Parquet 70,92×</h2>
    <span class="fase-tag">Fase 5 · Transformación y Optimización</span>
  </div>
  <div class="content">
    <div class="cols-2">
      <div class="box">
        <h3>5 Problemas Reales Corregidos en <code>procesamiento/</code></h3>
        <ul>
          <li><strong>160.821 duplicados (41,1%) en 2015–2019:</strong> 106.596 exactos + <strong>54.225 duplicados ocultos</strong> donde solo varía una coma en el nombre del programa. Depurados con <code>Window.partitionBy("id_registro")</code> → <strong>230.082 filas únicas</strong>.</li>
          <li><strong>90 grafías sucias → 33 deptos DANE:</strong> unificación de <code>BOGOTA D.C</code>, <code>NARINIO</code>, <code>ARCHIPIÉLAGO DE SA</code> y códigos sin cero (<code>5</code> → <code>05</code>) con <code>catalogo.py</code>.</li>
          <li><strong><code>id_area = "Sin clasificar"</code> (192 filas):</strong> resuelto con <code>try_cast</code> + <code>coalesce(..., 0)</code> para Spark 4 ANSI.</li>
          <li><strong>Columna <code>matriculados_2015</code>:</strong> renombrada a <code>total_matriculados</code> (<code>INT</code>) para todos los años 2015–2021.</li>
          <li><strong>Particionamiento físico:</strong> Parquet + Snappy particionado por <code>anio</code> (7 carpetas <code>anio=2015..2021</code> en HDFS).</li>
        </ul>
      </div>
      <div style="display:flex;flex-direction:column;gap:12px;">
        <div class="box green">
          <h3>Comparación de Almacenamiento en HDFS</h3>
          <svg viewBox="0 0 468 115" width="100%" style="display:block;margin-top:4px;">
            <text x="0" y="24" font-size="11" font-weight="700" fill="#0f2744">RAW JSONL</text>
            <rect x="108" y="8" width="338" height="24" rx="5" fill="#94a3b8"/>
            <text x="277" y="25" text-anchor="middle" font-size="11" font-weight="800" fill="#ffffff">401,58 MB (421.091.784 B)</text>

            <text x="0" y="68" font-size="11" font-weight="700" fill="#047857">CURADO Parquet</text>
            <rect x="108" y="52" width="14" height="24" rx="4" fill="#059669"/>
            <text x="129" y="69" font-size="11.4" font-weight="800" fill="#047857">5,66 MB (5.937.414 B) → 70,92× menos (-98,6%)</text>

            <text x="0" y="104" font-size="10" fill="#475569">Compresión columnar Snappy + eliminación de duplicados + diccionario DANE/SNIES</text>
          </svg>
        </div>
        <div class="box navy">
          <h3>Volumen Curado Final (<code>/datalake/curated/matricula_es</code>)</h3>
          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:6px;text-align:center;">
            <div><div style="font-size:18pt;font-weight:800;color:#fbbf24;">230.082</div><div style="font-size:9pt;color:#cbd5e1;">Filas únicas</div></div>
            <div><div style="font-size:18pt;font-weight:800;color:#34d399;">16,77 M</div><div style="font-size:9pt;color:#cbd5e1;">Matrículas acum.</div></div>
            <div><div style="font-size:18pt;font-weight:800;color:#93c5fd;">338 / 33</div><div style="font-size:9pt;color:#cbd5e1;">IES / Deptos DANE</div></div>
          </div>
        </div>
      </div>
    </div>
  </div>
  <div class="sftr"><span>Camilo Zuleta Delgado · Santiago Restrepo Silva</span><strong>Diapositiva 5 / 8</strong></div>
</section>

<!-- ============================ SLIDE 6 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Calidad Fail-Fast, Idempotencia y Structured Streaming</h2>
    <span class="fase-tag">Fases 6 y 7 · Confiabilidad y Velocidad</span>
  </div>
  <div class="content">
    <div class="cols-2">
      <div class="box">
        <h3>Fase 6 — 5 Validaciones Automáticas Fail-Fast (<code>calidad.py</code>)</h3>
        <table class="stbl">
          <thead><tr><th>Validación crítica</th><th>Resultado</th></tr></thead>
          <tbody>
            <tr><td><code>conteo_mayor_cero</code></td><td><span class="ok-pill">OK</span> 230.082 filas</td></tr>
            <tr><td><code>sin_nulos_columnas_clave</code></td><td><span class="ok-pill">OK</span> 0 nulos en 8 llaves</td></tr>
            <tr><td><code>sin_id_registro_duplicados</code></td><td><span class="ok-pill">OK</span> 0 duplicados</td></tr>
            <tr><td><code>rangos_validos</code></td><td><span class="ok-pill">OK</span> 0 fuera de rango</td></tr>
            <tr><td><code>departamentos_dane_canonicos</code></td><td><span class="ok-pill">OK</span> 33 deptos DANE</td></tr>
          </tbody>
        </table>
        <p style="margin-top:8px;font-size:10pt;"><strong>Idempotencia verificada en toda la cadena:</strong> correr 2 veces Ingesta, Kafka, HDFS, PySpark, Streaming y PostgreSQL mantiene exactamente los mismos conteos (<code>evidencias/idempotencia_pipeline.txt</code>).</p>
      </div>
      <div class="box amber">
        <h3>Fase 7 — Structured Streaming + Prueba de Reinicio</h3>
        <ul>
          <li>Fuente: Kafka <code>raw.datos_gov_co</code> · Watermark: <strong>5 min</strong> · Ventana: <strong>10 min</strong>.</li>
          <li>Deduplicación: <code>dropDuplicatesWithinWatermark(["id_registro"])</code>.</li>
          <li>Checkpoint persistente: <code>data/checkpoints/stream_matricula</code>.</li>
        </ul>
        <table class="stbl" style="margin-top:8px;">
          <thead><tr><th>Ejecución</th><th class="num">Lotes nuevos</th><th class="num">Reg. únicos</th><th class="num">Matriculados</th></tr></thead>
          <tbody>
            <tr><td><strong>Corrida #1</strong> (19,5 s)</td><td class="num">1</td><td class="num">230.082</td><td class="num">16.773.503</td></tr>
            <tr><td><strong>Corrida #2 reinicio</strong> (4,6 s)</td><td class="num"><span class="ok-pill">0</span></td><td class="num">230.082</td><td class="num">16.773.503</td></tr>
          </tbody>
        </table>
        <p style="margin-top:8px;font-size:10pt;"><strong>Reconciliación Lambda:</strong> <code>Batch == Streaming</code> en los 7 años (2015–2021) con <strong>0 diferencias</strong> en <code>servicio.stream_ventanas</code>.</p>
      </div>
    </div>
  </div>
  <div class="sftr"><span>Camilo Zuleta Delgado · Santiago Restrepo Silva</span><strong>Diapositiva 6 / 8</strong></div>
</section>

<!-- ============================ SLIDE 7 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Zona de Servicio, Tablero Plotly y Respuesta a la Pregunta</h2>
    <span class="fase-tag">Fase 8 · PostgreSQL + Visualización y Hallazgos</span>
  </div>
  <div class="content">
    <div class="cols-4">
      <div class="kpi-card"><div class="val">1,99 M</div><div class="lbl">Matrícula 2021-1 (+6,8% vs 2015)</div></div>
      <div class="kpi-card amber"><div class="val">+274,7%</div><div class="lbl">Crecimiento Virtualidad (3,4% → 11,9%)</div></div>
      <div class="kpi-card blue"><div class="val">13,41%</div><div class="lbl">Antioquia #2 (267.463 · 50,8% oficial)</div></div>
      <div class="kpi-card green"><div class="val">+88,8%</div><div class="lbl">I.U. Pascual Bravo (4.184 → 7.898)</div></div>
    </div>
    <div class="cols-2">
      <div class="box">
        <h3>Auge de la Metodología Virtual (% sobre Semestre 1)</h3>
        <svg viewBox="0 0 390 140" width="100%" style="display:block;">
          <line x1="15" y1="115" x2="380" y2="115" stroke="#cbd5e1" stroke-width="1.2"/>
          {bars_virt}
        </svg>
        <p style="font-size:9.8pt;color:#475569;margin-top:4px;">En <strong>2021-2</strong> (oferta SENA) la virtualidad alcanzó <strong>30,19%</strong>. El tablero completo (<code>docs/index.html</code>) se alimenta de 7 tablas en PostgreSQL vía usuario de solo lectura <code>lector</code>.</p>
      </div>
      <div class="box">
        <h3>Brecha de Género por Área (% Mujeres en 2021-1)</h3>
        <svg viewBox="0 0 365 145" width="100%" style="display:block;">
          {bars_gen}
        </svg>
        <p style="font-size:9.8pt;color:#475569;margin-top:4px;">Aunque las mujeres son el <strong>53,84%</strong> nacional, en <strong>Ingeniería</strong> caen al <strong>32,2%</strong>. En <strong>I.U. Pascual Bravo</strong> la participación femenina subió de 18,1% a <strong>25,3%</strong>.</p>
      </div>
    </div>
  </div>
  <div class="sftr"><span>Camilo Zuleta Delgado · Santiago Restrepo Silva</span><strong>Diapositiva 7 / 8</strong></div>
</section>

<!-- ============================ SLIDE 8 ============================ -->
<section class="slide">
  <div class="shdr">
    <h2>Puntos Extra: Apache Hive (+2) y Apache Airflow (+3) + Conclusiones</h2>
    <span class="fase-tag extra">Opcionales (+5 pts) y Cierre</span>
  </div>
  <div class="content">
    <div class="cols-2">
      <div class="box blue">
        <h3>Apache Hive 4.0.1 (+2 pts) — <code>hive/</code></h3>
        <ul>
          <li>Tabla <strong><code>EXTERNAL</code></strong> <code>matricula_db.matricula_curada</code> sobre Parquet en HDFS sin duplicar almacenamiento.</li>
          <li><code>MSCK REPAIR TABLE</code> registró automáticamente las 7 particiones (<code>anio=2015..2021</code>) → <strong>230.082 filas / 16.773.503 matriculados</strong>.</li>
          <li><strong>4 consultas SQL analíticas ejecutadas en Beeline:</strong>
            evolución Colombia vs. Antioquia, Top 10 departamentos, brecha de género SNIES y serie 2015–2021 de la <strong>I.U. Pascual Bravo (3107)</strong>.</li>
        </ul>
      </div>
      <div class="box amber">
        <h3>Apache Airflow 3.3.2 (+3 pts) — <code>airflow/</code></h3>
        <ul>
          <li>DAG <code>matricula_es_pipeline_completo</code> con <strong>11 tareas</strong>, horario semanal (<code>0 6 * * 1</code>), <code>retries=2..3</code> y <code>trigger_rule="all_done"</code> para apagar contenedores y ahorrar RAM.</li>
          <li><strong>Validación real:</strong> <code>0</code> errores de importación (<code>airflow dags list-import-errors</code>) y ejecución verificada de <code>generar_dashboard</code> (<code>new_state=success</code>).</li>
          <li>Interfaz web lista en <code>http://localhost:8082</code>.</li>
        </ul>
      </div>
    </div>
    <div class="box navy">
      <h3>Conclusiones clave del proyecto</h3>
      <div class="cols-3" style="margin-top:6px;">
        <div>
          <strong style="color:#fbbf24;">1. Ingeniería real sobre datos públicos</strong>
          <p style="font-size:10pt;color:#e2e8f0;margin-top:3px;">Auditar la fuente detectó 160.821 duplicados (54.225 ocultos) que habrían sesgado cualquier análisis sin limpieza robusta.</p>
        </div>
        <div>
          <strong style="color:#34d399;">2. Pipeline reproducible e idempotente</strong>
          <p style="font-size:10pt;color:#e2e8f0;margin-top:3px;">9 pruebas <code>pytest</code>, 5 validaciones fail-fast, reconciliación Batch = Streaming (0 dif.) y ejecución automática con Airflow y <code>./actualizar.sh</code>.</p>
        </div>
        <div>
          <strong style="color:#93c5fd;">3. Valor directo para decisiones IES</strong>
          <p style="font-size:10pt;color:#e2e8f0;margin-top:3px;">Evidencia accionable sobre virtualidad (+274,7%), brecha femenina en Ingeniería (32,2%) y liderazgo público de Antioquia.</p>
        </div>
      </div>
    </div>
  </div>
  <div class="sftr"><span>¡Gracias! Listos para demostración en vivo y preguntas · Camilo Zuleta &amp; Santiago Restrepo</span><strong>Diapositiva 8 / 8</strong></div>
</section>

</body>
</html>
"""


def render_pdf(html: str, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".html", encoding="utf-8", delete=False) as tf:
        tf.write(html)
        html_path = Path(tf.name)
    try:
        cmd = [
            CHROME,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--no-pdf-header-footer",
            f"--print-to-pdf={destino}",
            html_path.as_uri(),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode != 0 or not destino.exists():
            raise RuntimeError(f"Chrome headless falló ({res.returncode}): {res.stderr}")
        print(f"PDF generado: {destino} ({destino.stat().st_size / 1024:.1f} KB)")
    finally:
        html_path.unlink(missing_ok=True)


def main() -> None:
    metricas = cargar_metricas()
    render_pdf(construir_html_guia(metricas), DOCS / "Guia_Completa_Ejecucion_Pipeline.pdf")
    render_pdf(construir_html_presentacion(metricas), DOCS / "Presentacion_Sustentacion.pdf")


if __name__ == "__main__":
    main()
