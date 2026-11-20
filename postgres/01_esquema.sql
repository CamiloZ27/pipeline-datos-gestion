-- =============================================================================
-- Fase 8 — Zona de servicio en PostgreSQL (base: matricula_es)
-- Se ejecuta automáticamente al inicializar el volumen de PostgreSQL por 1.ª vez.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS servicio;
COMMENT ON SCHEMA servicio IS
  'Zona de servicio del pipeline MEN Matrícula Educación Superior (2015-2021). Tablas precalculadas para el dashboard.';

-- Usuario de solo lectura para tableros (Plotly HTML / Power BI)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lector') THEN
        CREATE ROLE lector LOGIN PASSWORD 'lector';
    END IF;
END $$;

GRANT USAGE ON SCHEMA servicio TO lector;
ALTER DEFAULT PRIVILEGES FOR ROLE pipeline IN SCHEMA servicio
    GRANT SELECT ON TABLES TO lector;

