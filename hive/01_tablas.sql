-- =============================================================================
-- Componente opcional (+2 pts) — Apache Hive 4.0.1
-- Tablas EXTERNAS sobre el Parquet de la zona curada en HDFS.
--
-- Ejecución:
--   docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/ -f /proyecto/hive/01_tablas.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS matricula_db;
USE matricula_db;

-- Tabla EXTERNA: Hive guarda únicamente los metadatos; DROP TABLE no borra HDFS.
DROP TABLE IF EXISTS matricula_curada;

CREATE EXTERNAL TABLE matricula_curada (
    id_registro            STRING,
    fecha_periodo          DATE,
    semestre               INT,
    codigo_ies             INT,
    ies_padre              INT,
    nombre_ies             STRING,
    sede_tipo              STRING,
    id_sector              INT,
    sector_nombre          STRING,
    id_caracter            INT,
    caracter_nombre        STRING,
    cod_depto_ies          STRING,
    departamento_ies       STRING,
    cod_muni_ies           STRING,
    municipio_ies          STRING,
    codigo_snies           INT,
    programa_academico     STRING,
    id_nivel               INT,
    nivel_nombre           STRING,
    id_nivel_formacion     INT,
    nivel_formacion_nombre STRING,
    id_metodologia         INT,
    metodologia_nombre     STRING,
    id_area                INT,
    area_nombre            STRING,
    nbc_nombre             STRING,
    cod_depto_oferta       STRING,
    departamento_oferta    STRING,
    cod_muni_oferta        STRING,
    municipio_oferta       STRING,
    id_genero              INT,
    genero_nombre          STRING,
    total_matriculados     INT,
    fuente                 STRING,
    dataset                STRING,
    fecha_ingesta          TIMESTAMP
)
PARTITIONED BY (anio INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/datalake/curated/matricula_es';

-- Registra automáticamente las carpetas anio=2015 ... anio=2021 creadas por Spark
MSCK REPAIR TABLE matricula_curada;

-- Vista analítica reutilizable para Antioquia vs. resto del país
CREATE OR REPLACE VIEW v_matricula_antioquia_vs_colombia AS
SELECT
    anio,
    semestre,
    CASE WHEN cod_depto_oferta = '05' THEN 'Antioquia' ELSE 'Resto de Colombia' END AS region,
    sector_nombre,
    metodologia_nombre,
    area_nombre,
    genero_nombre,
    SUM(total_matriculados) AS total_matriculados,
    COUNT(DISTINCT codigo_ies) AS total_ies,
    COUNT(DISTINCT codigo_snies) AS total_programas
FROM matricula_curada
GROUP BY
    anio, semestre,
    CASE WHEN cod_depto_oferta = '05' THEN 'Antioquia' ELSE 'Resto de Colombia' END,
    sector_nombre, metodologia_nombre, area_nombre, genero_nombre;

SHOW PARTITIONS matricula_curada;
SELECT COUNT(*) AS filas_curadas_hive, SUM(total_matriculados) AS matriculados_acumulados FROM matricula_curada;

