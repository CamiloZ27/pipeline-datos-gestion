-- =============================================================================
-- Componente opcional (+2 pts) — Consultas analíticas en Apache Hive
-- Responden directamente a la pregunta principal del proyecto sobre el Parquet.
--
-- Ejecución:
--   docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/matricula_db -f /proyecto/hive/02_consultas.sql
-- =============================================================================

USE matricula_db;

-- Consulta 1: Evolución anual de la matrícula en Colombia y participación de Antioquia (Semestre 1)
SELECT
    anio,
    SUM(total_matriculados) AS total_colombia,
    SUM(CASE WHEN cod_depto_oferta = '05' THEN total_matriculados ELSE 0 END) AS total_antioquia,
    ROUND(100.0 * SUM(CASE WHEN cod_depto_oferta = '05' THEN total_matriculados ELSE 0 END) / SUM(total_matriculados), 2) AS pct_antioquia,
    ROUND(100.0 * SUM(CASE WHEN metodologia_nombre = 'Virtual' THEN total_matriculados ELSE 0 END) / SUM(total_matriculados), 2) AS pct_virtual
FROM matricula_curada
WHERE semestre = 1
GROUP BY anio
ORDER BY anio;

-- Consulta 2: Top 10 departamentos con mayor matrícula en 2021 (Semestre 1)
SELECT
    departamento_oferta,
    SUM(total_matriculados) AS matriculados_2021_1,
    ROUND(100.0 * SUM(CASE WHEN sector_nombre = 'Oficial' THEN total_matriculados ELSE 0 END) / SUM(total_matriculados), 1) AS pct_oficial,
    COUNT(DISTINCT codigo_ies) AS ies_activas
FROM matricula_curada
WHERE anio = 2021 AND semestre = 1
GROUP BY departamento_oferta
ORDER BY matriculados_2021_1 DESC
LIMIT 10;

-- Consulta 3: Brecha de género por Área de Conocimiento SNIES en 2021
SELECT
    area_nombre,
    SUM(CASE WHEN genero_nombre = 'Femenino' THEN total_matriculados ELSE 0 END) AS mujeres,
    SUM(CASE WHEN genero_nombre = 'Masculino' THEN total_matriculados ELSE 0 END) AS hombres,
    ROUND(100.0 * SUM(CASE WHEN genero_nombre = 'Femenino' THEN total_matriculados ELSE 0 END) / SUM(total_matriculados), 2) AS pct_mujeres
FROM matricula_curada
WHERE anio = 2021 AND semestre = 1
GROUP BY area_nombre
ORDER BY (mujeres + hombres) DESC;

-- Consulta 4: Evolución de la Institución Universitaria Pascual Bravo (código 3107)
SELECT
    anio,
    semestre,
    SUM(total_matriculados) AS matriculados_pascual_bravo,
    COUNT(DISTINCT codigo_snies) AS programas_activos,
    ROUND(100.0 * SUM(CASE WHEN genero_nombre = 'Femenino' THEN total_matriculados ELSE 0 END) / SUM(total_matriculados), 2) AS pct_mujeres
FROM matricula_curada
WHERE codigo_ies = 3107
GROUP BY anio, semestre
ORDER BY anio, semestre;

