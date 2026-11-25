-- ============================================================
-- ELECTROCASA - INGESTA BRONZE DE SUCURSALES
-- Metodo: COPY INTO
-- Fuente: CSV de referencia (40 sucursales)
-- ============================================================

CREATE TABLE IF NOT EXISTS electrocasa_dev.bronze.sucursales_bronze (
    sucursal_id STRING,
    nombre_sucursal STRING,
    distrito STRING,
    ciudad STRING,
    region STRING,
    fecha_apertura STRING
)
USING DELTA;

COPY INTO electrocasa_dev.bronze.sucursales_bronze
FROM '/Volumes/electrocasa_dev/default/vol_landing/sucursales/sucursales.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'delimiter' = ','
)
COPY_OPTIONS ('mergeSchema' = 'false');
