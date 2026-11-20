"""
Construye y ejecuta el notebook entregable del proyecto:
`notebooks/Pipeline_Matricula_ES_Colombia.ipynb`

Ejecuta cada celda de código Python en un contexto real del repositorio
(leyendo las evidencias reales, muestras, módulos y datos de servicio generados
durante la corrida completa del pipeline) y guarda el archivo en formato nbformat 4
con salidas reales capturadas.
"""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DESTINO = RAIZ / "notebooks" / "Pipeline_Matricula_ES_Colombia.ipynb"


def celda_md(texto: str) -> dict:
    lineas = [l + "\n" for l in texto.strip().splitlines()]
    if lineas:
        lineas[-1] = lineas[-1].rstrip("\n")
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": lineas,
    }


def celda_code(codigo: str, namespace: dict, exec_count: int) -> dict:
    codigo_limpio = codigo.strip()
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exec(compile(codigo_limpio, f"<cell-{exec_count}>", "exec"), namespace)
    salida = buffer.getvalue()

    outputs = []
    if salida:
        lineas_out = [l + "\n" for l in salida.splitlines()]
        outputs.append(
            {
                "name": "stdout",
                "output_type": "stream",
                "text": lineas_out,
            }
        )

    lineas_src = [l + "\n" for l in codigo_limpio.splitlines()]
    if lineas_src:
        lineas_src[-1] = lineas_src[-1].rstrip("\n")

    return {
        "cell_type": "code",
        "execution_count": exec_count,
        "metadata": {},
        "outputs": outputs,
        "source": lineas_src,
    }


def construir() -> Path:
    DESTINO.parent.mkdir(parents=True, exist_ok=True)
    ns: dict = {}
    celdas: list[dict] = []
    n = 0

    def add_md(texto: str) -> None:
        celdas.append(celda_md(texto))

    def add_code(codigo: str) -> None:
        nonlocal n
        n += 1
        celdas.append(celda_code(codigo, ns, n))

    # ------------------------------------------------------------------
    # PORTADA Y FASE 1
    # ------------------------------------------------------------------
    add_md(
        """# Pipeline Big Data End-to-End: Matrícula de Educación Superior en Colombia (2015–2021)

**Institución Universitaria Pascual Bravo** — **Gestión de Datos Masivos (G004 — Unidad 3)**  
**Docente:** Aleixo Eder Pineda Ramírez · **Periodo:** 2026-2  
**Estudiantes:** **Camilo Zuleta Delgado** · **Santiago Restrepo Silva**

---

## Fase 1 — Pregunta Principal, Fuente de Datos y Arquitectura Lambda

### Pregunta de investigación
> **¿Cómo evolucionó la matrícula de educación superior en Colombia (2015–2021) por departamento, sector (oficial vs. privado), metodología (presencial/virtual/distancia), área del conocimiento y género, y cuál es la brecha regional y de Antioquia frente al país?**

### Usuario objetivo
- **Direcciones de Planeación Académica de IES** (ej. **Institución Universitaria Pascual Bravo**, código SNIES `3107`).
- **Secretarías de Educación Departamentales** (ej. Antioquia) y analistas de política pública educativa.

### Fuente pública y llave natural
- **Dataset:** `MEN_MATRICULA_ESTADISTICA_ES` (Ministerio de Educación Nacional — SNIES, `datos.gov.co`, ID `5wck-szir`).
- **API SODA2 (abierta sin credenciales):** `https://www.datos.gov.co/resource/5wck-szir.json`
- **Volumen:** `390.903` filas crudas, `26` columnas, `14` semestres (`2015-1` a `2021-2`).
- **Llave natural (`id_registro`):** `anio|semestre|cod_ies|cod_snies|cod_muni_oferta(5d)|id_metodologia|id_genero`.

### Justificación de Arquitectura Lambda (Batch + Speed)
1. **Capa Batch (`HDFS → MapReduce/PySpark → Parquet → Hive → PostgreSQL`):** necesaria para depurar los **160.821 duplicados** del histórico 2015–2019 (incluidos **54.225 duplicados ocultos** por variaciones de comas), normalizar 90 grafías departamentales a **33 departamentos DANE** y lograr una compresión **70,92×** en Parquet.
2. **Capa de Velocidad (`Kafka → Spark Structured Streaming → PostgreSQL`):** procesa los eventos desde Kafka con `watermark` de 5 min, ventana de 10 min, `dropDuplicatesWithinWatermark(["id_registro"])` y `checkpoint` persistente, reconciliando **0 diferencias** frente a la capa Batch en los 7 años.
"""
    )

    add_code(
        """from pathlib import Path
import json
import csv

# Localiza la raíz del proyecto tanto si se ejecuta desde /notebooks como desde la raíz
RAIZ = Path.cwd()
if not (RAIZ / "evidencias").exists() and (RAIZ.parent / "evidencias").exists():
    RAIZ = RAIZ.parent

print("Raíz del proyecto:", RAIZ.name)
modulos = sorted(
    p.name for p in RAIZ.iterdir()
    if p.is_dir() and not p.name.startswith(".") and p.name not in {"__pycache__", "data"}
)
print("Carpetas principales del repositorio:", modulos)"""
    )

    # ------------------------------------------------------------------
    # FASE 2 — INGESTA
    # ------------------------------------------------------------------
    add_md(
        """## Fase 2 — Ingesta desde API REST (`datos.gov.co` SODA2)

Implementada en [`ingesta/`](../ingesta/):
- **`ingesta/config.py`**: configuración del endpoint SODA2 (`5wck-szir`), tamaño de página (`50.000`), reintentos con *backoff* exponencial (`429/5xx`) y soporte opcional a `SOCRATA_APP_TOKEN`.
- **`ingesta/comun.py`**: generación determinista de la llave `id_registro`, construcción del sobre JSON inmutable (`fuente`, `dataset`, `id_registro`, `fecha_ingesta`, `payload`), escritura de manifiestos `_manifiesto_*.json` y lectura/actualización de la marca de agua `_marca_agua.json`.
- **`ingesta/matricula.py`**: descarga paginada con filtro `$where` e ingesta incremental.

### Comandos ejecutados para la evidencia de marca de agua:
```bash
python3 -m ingesta descargar --hasta-anio 2020   # Corrida 1: 356,341 registros (2015-2020)
python3 -m ingesta descargar --incremental       # Corrida 2:  34,562 registros (2021-1 y 2021-2)
python3 -m ingesta descargar --incremental       # Corrida 3:       0 registros nuevos (idempotente)
```
"""
    )

    add_code(
        """muestra_dir = RAIZ / "datos_muestra" / "raw"
manifiestos = sorted(muestra_dir.glob("_manifiesto_*.json"))
total_ingestado = 0
for m in manifiestos:
    info = json.loads(m.read_text(encoding="utf-8"))
    total_ingestado += info["registros"]
    print(f"Manifiesto {m.name}:")
    print(f"  archivo    : {info['archivo']}")
    print(f"  registros  : {info['registros']:,}")
    print(f"  bytes      : {info['bytes']:,} ({info['bytes'] / (1024**2):.2f} MB)")
    print(f"  incremental: {info['parametros']['incremental']} | hasta_anio={info['parametros']['hasta_anio']}")

marca = json.loads((muestra_dir / "_marca_agua.json").read_text(encoding="utf-8"))
print(f"\\nTotal acumulado descargado : {total_ingestado:,} registros")
print("Estado final marca de agua :", json.dumps(marca, ensure_ascii=False))

# Inspección del primer sobre JSON de la muestra cruda
primera_linea = (muestra_dir / "muestra_raw_200.jsonl").read_text(encoding="utf-8").splitlines()[0]
sobre = json.loads(primera_linea)
print("\\nEstructura del sobre JSON (Fase 2):")
print("  Claves externas :", list(sobre.keys()))
print("  id_registro     :", sobre["id_registro"])
print("  fecha_ingesta   :", sobre["fecha_ingesta"])
print("  Campos en payload:", len(sobre["payload"]), "columnas ->", list(sobre["payload"].keys())[:8], "...")"""
    )

    # ------------------------------------------------------------------
    # FASE 3 — KAFKA
    # ------------------------------------------------------------------
    add_md(
        """## Fase 3 — Streaming con Apache Kafka 4.1 (KRaft)

Implementada en [`streaming/`](../streaming/):
- **Tópico:** `raw.datos_gov_co` creado explícitamente con **3 particiones**, replicación 1 y retención de 7 días.
- **Productor (`streaming/productor.py`):** envía cada sobre JSON con `key = id_registro.encode("utf-8")` para garantizar particionamiento determinista y escribe un checkpoint `.kafka_ok` por archivo procesado (evitando reenvíos duplicados).
- **Consumidor (`streaming/consumidor.py`):** lee con `group_id` configurable (`grupo-verificacion`), verificando offsets y balanceo en las 3 particiones.

```bash
docker compose --profile kafka up -d --wait
python3 -m streaming crear-topicos
python3 -m streaming producir                 # Corrida 1: 390,903 mensajes
python3 -m streaming producir                 # Corrida 2: 0 mensajes (idempotente por .kafka_ok)
python3 -m streaming info-topico
python3 -m streaming consumir --grupo grupo-verificacion --max 5
```
"""
    )

    add_code(
        """print((RAIZ / "evidencias" / "kafka_topico_y_consumidor.txt").read_text(encoding="utf-8"))"""
    )

    # ------------------------------------------------------------------
    # FASE 4a — HDFS
    # ------------------------------------------------------------------
    add_md(
        """## Fase 4a — Data Lake Distribuido en Apache Hadoop HDFS

Implementada en [`datalake/`](../datalake/):
- **Cliente WebHDFS (`datalake/webhdfs.py`):** maneja la redirección HTTP 307 NameNode → DataNode y reemplaza hostnames internos de Docker cuando se ejecuta desde el host.
- **Consumidor por lotes (`datalake/consumidor_hdfs.py`):** acumula mensajes por `(tópico, partición, fecha_ingesta)` en bloques de hasta 20.000 registros (evitando el antipatrón de archivos pequeños en HDFS), usa nombres deterministas `part-<topico>-p<particion>-o<offset_inicial>.jsonl` y realiza **commit manual de offsets solo después** de recibir confirmación HTTP 201 de HDFS.

```bash
docker compose --profile hdfs up -d --wait
python3 -m datalake consumir
docker compose exec namenode hdfs dfs -ls -R -h /datalake/raw
```
"""
    )

    add_code(
        """print((RAIZ / "evidencias" / "hdfs_ls.txt").read_text(encoding="utf-8"))"""
    )

    # ------------------------------------------------------------------
    # FASE 4b — MAPREDUCE VS PYSPARK
    # ------------------------------------------------------------------
    add_md(
        """## Fase 4b — Validación Cruzada: MapReduce en Python vs. PySpark

Implementada en [`mapreduce/`](../mapreduce/):
- **`mapper.py`** y **`reducer.py`**: implementación clásica de MapReduce con lectura/escritura por `stdin`/`stdout` (clave compuesta `anio\\tid_sector`, valor `1\\tmatriculados`).
- **`comparar_spark.py`**: ejecuta el flujo MapReduce completo en streaming sobre los 25 archivos JSONL de HDFS y lo compara grupo a grupo contra una agregación equivalente en PySpark.

```bash
docker compose run --rm spark-mapreduce
```
"""
    )

    add_code(
        """print((RAIZ / "evidencias" / "mapreduce_vs_spark.txt").read_text(encoding="utf-8"))"""
    )

    # ------------------------------------------------------------------
    # FASES 5 Y 6 — ZONA CURADA PYSPARK + CALIDAD + IDEMPOTENCIA
    # ------------------------------------------------------------------
    add_md(
        """## Fases 5 y 6 — Zona Curada en PySpark, Calidad Fail-Fast e Idempotencia

Implementadas en [`procesamiento/`](../procesamiento/):
1. **Hallazgo crítico de duplicación en la fuente oficial (`5wck-szir`):**
   - De `390.903` filas crudas, **`160.821` (41,1%) son duplicadas** en los años 2015–2019: `106.596` duplicados exactos + **`54.225` duplicados ocultos** donde solo cambia la presencia de comas en los nombres de programa o NBC (mientras la cifra de matriculados es idéntica).
   - La deduplicación por ventana sobre `id_registro` (`Window.partitionBy("id_registro")`) elimina el 100% de ambos tipos, dejando **`230.082` filas curadas únicas** (`16.773.503` matrículas acumuladas).
2. **Normalización de catálogos DANE y SNIES (`procesamiento/catalogo.py`):**
   - `90` grafías sucias de departamentos normalizadas mediante código DANE `lpad(2)` a los **33 departamentos oficiales de Colombia**.
   - `id_area = "Sin clasificar"` (192 registros) manejado con `try_cast` + `coalesce(..., 0)`.
   - Columna `matriculados_2015` renombrada a `total_matriculados` (`INT`).
3. **5 Validaciones automáticas fail-fast (`procesamiento/calidad.py`):** si alguna falla, el pipeline se detiene antes de sobrescribir la zona curada.
4. **Optimización de almacenamiento:** `401,58 MB` (JSONL raw) → **`5,66 MB`** (Parquet + Snappy particionado por `anio`) = **70,92× menos espacio (98,6% de ahorro)**.

```bash
docker compose run --rm spark
```
"""
    )

    add_code(
        """calidad = json.loads((RAIZ / "evidencias" / "reporte_calidad.json").read_text(encoding="utf-8"))
r = calidad["resumen"]
a = calidad["almacenamiento"]

print(f"Estado de calidad       : {calidad['estado']} (fallos críticos: {len(calidad['fallos_criticos'])})")
print(f"Filas raw leídas        : {r['filas_raw_leidas']:,}")
print(f"Duplicados eliminados   : {r['duplicados_eliminados']:,} ({r['duplicados_eliminados']/r['filas_raw_leidas']*100:.1f}%)")
print(f"Registros descartados   : {r['registros_descartados']:,}")
print(f"Filas curadas únicas    : {r['filas_curadas']:,}")
print(f"Almacenamiento          : {a['mb_raw_jsonl']} MB JSONL -> {a['mb_curado_parquet']} MB Parquet ({a['factor_reduccion_x']}x menos, -{a['reduccion_pct']}%)")

print("\\nValidaciones automáticas fail-fast:")
for v in calidad["validaciones"]:
    icono = "OK" if v["aprobada"] else "FALLO"
    print(f"  [{icono}] {v['nombre']:<30} | {v['detalle']}")

print("\\nAntes y después por año:")
print(f"{'Año':<6} {'RAW':>10} {'Eliminados':>12} {'Curadas':>10} {'Matriculados':>14}")
print("-" * 56)
for fila in calidad["antes_despues_por_anio"]:
    print(
        f"{fila['anio']:<6} {fila['filas_raw']:>10,} {fila['duplicados_y_descartes']:>12,} "
        f"{fila['filas_curadas']:>10,} {fila['total_matriculados_curados']:>14,}"
    )"""
    )

    add_code(
        """print((RAIZ / "evidencias" / "idempotencia_pipeline.txt").read_text(encoding="utf-8"))"""
    )

    # ------------------------------------------------------------------
    # FASE 7 — SPARK STRUCTURED STREAMING
    # ------------------------------------------------------------------
    add_md(
        """## Fase 7 — Spark Structured Streaming (Capa de Velocidad Lambda)

Implementada en [`streaming_spark/flujo_matricula.py`](../streaming_spark/flujo_matricula.py):
- Lee en streaming desde el tópico Kafka `raw.datos_gov_co` usando los conectores offline precargados en `jars/`.
- Aplica `withWatermark("event_time", "5 minutes")`, `dropDuplicatesWithinWatermark(["id_registro"])` y agregación en ventanas de `10 minutes` por `(ventana, anio, sector_nombre, departamento_oferta)`.
- Persiste el estado de offsets y deduplicación en el checkpoint `data/checkpoints/stream_matricula` y escribe el resultado consolidado en `servicio.stream_ventanas` de PostgreSQL.
- Al reiniciar el proceso (`Corrida 2`), Spark detecta que no hay offsets nuevos (`lotes_nuevos_procesados = 0`), manteniendo intactos los `230.082` registros únicos y `16.773.503` matriculados (**0 duplicados**).

```bash
docker compose --profile kafka --profile postgres up -d --wait
docker compose run --rm spark-streaming   # Corrida 1: procesa 390,903 mensajes -> 230,082 únicos
docker compose run --rm spark-streaming   # Corrida 2 (reinicio): 0 lotes nuevos, 0 duplicados
```
"""
    )

    add_code(
        """stream = json.loads((RAIZ / "evidencias" / "streaming_reinicio.json").read_text(encoding="utf-8"))
for idx, ej in enumerate(stream["ejecuciones"], 1):
    est = ej["estado_acumulado"]
    print(f"Corrida #{idx} ({ej['timestamp_utc']}) — duración: {ej['duracion_s']} s")
    print(f"  Watermark / Ventana    : {ej['watermark']} / {ej['ventana']}")
    print(f"  Lotes nuevos procesados: {ej['lotes_nuevos_procesados']}")
    print(f"  Filas ventana total    : {est['filas_ventana_total']}")
    print(f"  Registros únicos total : {est['registros_unicos_total']:,}")
    print(f"  Matriculados total     : {est['total_matriculados']:,}\\n")

# Reconciliación Lambda: Capa Batch vs Capa Velocidad por año
datos_tab = json.loads((RAIZ / "salida" / "datos_tablero.json").read_text(encoding="utf-8"))
print("Reconciliación Arquitectura Lambda (Batch Curado vs. Structured Streaming):")
print(f"{'Año':<6} {'Batch':>14} {'Streaming':>14} {'Diferencia':>12}")
print("-" * 50)
for rec in datos_tab["reconciliacion_lambda"]:
    dif = rec["batch_matriculados"] - rec["stream_matriculados"]
    print(f"{rec['anio']:<6} {rec['batch_matriculados']:>14,} {rec['stream_matriculados']:>14,} {dif:>12,}")"""
    )

    # ------------------------------------------------------------------
    # FASE 8 — ZONA DE SERVICIO POSTGRESQL + TABLERO + HALLAZGOS
    # ------------------------------------------------------------------
    add_md(
        """## Fase 8 — Zona de Servicio (PostgreSQL 17), Tablero HTML y Hallazgos Analíticos

Implementada en [`procesamiento/servir.py`](../procesamiento/servir.py), [`postgres/01_esquema.sql`](../postgres/01_esquema.sql) y [`visualizacion/`](../visualizacion/):
- Carga idempotente (vía JDBC) de 6 tablas de indicadores precalculados al esquema `servicio.*` + `servicio.stream_ventanas` desde Streaming.
- Usuario de solo lectura **`lector`** (`GRANT SELECT`) consumido por el generador del tablero HTML (`visualizacion/generar.py`).
- Tablero autocontenido con **Plotly.js** local (licencia MIT) y tipografía Inter local (OFL) publicado en `salida/dashboard.html` y `docs/index.html` (GitHub Pages).

```bash
docker compose run --rm spark-servir
python3 -m visualizacion generar --origen postgres --publicar
```
"""
    )

    add_code(
        """kpi = datos_tab["kpi"]
print("RESPUESTA PRINCIPAL:")
print(" ", datos_tab["titulo_respuesta"])
print("\\nKPIs clave (corte 2021-1):")
print(f"  Matriculados Colombia     : {kpi['matriculados_colombia']:,} ({kpi['variacion_colombia_pct']:+.1f}% vs 2015-1)")
print(f"  Matriculados Antioquia    : {kpi['matriculados_antioquia']:,} ({kpi['pct_antioquia']}% del país, {kpi['variacion_antioquia_pct']:+.1f}%)")
print(f"  Participación Virtual     : {kpi['pct_virtual']}% (crecimiento {kpi['crec_virtual_pct']:+.1f}% desde 2015-1)")
print(f"  Participación Femenina    : {kpi['pct_femenino']}% | Sector Oficial: {kpi['pct_oficial']}%")
print(f"  IES únicas / Programas    : {kpi['ies_unicas']} IES / {kpi['programas_unicos']:,} programas")

print("\\nArchivos CSV exportados en datos_muestra/zona_servicio/:")
for csv_f in sorted((RAIZ / "datos_muestra" / "zona_servicio").glob("*.csv")):
    with csv_f.open(encoding="utf-8") as fh:
        n_filas = sum(1 for _ in fh) - 1
    print(f"  - {csv_f.name:<38} ({n_filas:>5,} filas)")

dash_html = RAIZ / "docs" / "index.html"
print(f"\\nTablero HTML publicado: {dash_html.relative_to(RAIZ)} ({dash_html.stat().st_size / (1024**2):.2f} MB autocontenido)")"""
    )

    # ------------------------------------------------------------------
    # COMPONENTES OPCIONALES: HIVE (+2) Y AIRFLOW (+3)
    # ------------------------------------------------------------------
    add_md(
        """## Componentes Opcionales (+5 pts extra): Apache Hive (+2) y Apache Airflow (+3)

### 1. Apache Hive 4.0.1 (+2 pts)
- Script [`hive/01_tablas.sql`](../hive/01_tablas.sql): crea la base `matricula_db`, la tabla **externa** `matricula_curada` apuntando directamente al Parquet de HDFS (`/datalake/curated/matricula_es`) y descubre automáticamente las 7 particiones (`anio=2015..2021`) mediante `MSCK REPAIR TABLE`.
- Script [`hive/02_consultas.sql`](../hive/02_consultas.sql): ejecuta 4 consultas analíticas que responden la pregunta principal, incluida la trayectoria de la **I.U. Pascual Bravo (+88,8% de 2015-1 a 2021-1)**.

### 2. Apache Airflow 3.3.2 (+3 pts)
- DAG [`airflow/dags/matricula_pipeline_dag.py`](../airflow/dags/matricula_pipeline_dag.py): orquesta las **11 tareas** completas de la arquitectura Lambda con programación semanal (`0 6 * * 1`), reintentos automáticos (`retries=2..3`), apagado controlado de contenedores (`trigger_rule="all_done"`) y ejecución real verificada con `airflow tasks test`.
"""
    )

    add_code(
        """print((RAIZ / "evidencias" / "hive_consultas.txt").read_text(encoding="utf-8"))"""
    )

    add_code(
        """print((RAIZ / "evidencias" / "airflow_dag.txt").read_text(encoding="utf-8"))"""
    )

    # ------------------------------------------------------------------
    # VERIFICACIÓN DE PRUEBAS UNITARIAS
    # ------------------------------------------------------------------
    add_md(
        """## Verificación de Calidad del Código — Suite de Pruebas (`pytest`)

El repositorio incluye pruebas unitarias reproducibles en [`tests/`](../tests/):
- **7 pruebas offline** (`test_ingesta.py`, `test_streaming.py`, `test_datalake.py`, `test_mapreduce.py`) que verifican la llave natural, sobres JSON, marca de agua, particionamiento Kafka, nombres deterministas WebHDFS y consistencia del Mapper/Reducer.
- **2 pruebas en PySpark** (`test_procesamiento_spark.py`) que verifican deduplicación de duplicados exactos y ocultos por comas, normalización DANE, casteo seguro de `"Sin clasificar"` y disparo de las 5 validaciones fail-fast.
"""
    )

    add_code(
        """import subprocess
import sys

res = subprocess.run(
    [
        sys.executable, "-m", "pytest", "-v",
        "tests/test_ingesta.py",
        "tests/test_streaming.py",
        "tests/test_datalake.py",
        "tests/test_mapreduce.py",
    ],
    cwd=RAIZ,
    capture_output=True,
    text=True,
    check=False,
)
print(res.stdout)
if res.returncode != 0:
    print(res.stderr)
    raise RuntimeError(f"pytest falló con código {res.returncode}")"""
    )

    notebook = {
        "cells": celdas,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (.venv)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.9.6",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    DESTINO.write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Notebook generado y ejecutado con {n} celdas de código: {DESTINO}")
    return DESTINO


if __name__ == "__main__":
    construir()
