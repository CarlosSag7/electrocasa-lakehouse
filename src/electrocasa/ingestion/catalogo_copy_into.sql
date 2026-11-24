
-- ============================================================
-- ELECTROCASA - INGESTA BRONZE DEL CATALOGO
-- Metodo: COPY INTO
-- Formato: JSON multilínea
-- ============================================================

USE CATALOG electrocasa_dev;

-- ------------------------------------------------------------
-- 1. Tabla de preparacion: snapshot marzo
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.catalogo_marzo_staging (
    activo BOOLEAN,
    categoria STRING,
    marca STRING,
    nombre_producto STRING,
    precio_lista STRING,
    producto_id STRING,
    _rescued_data STRING
)
USING DELTA;

-- ------------------------------------------------------------
-- 2. Tabla de preparacion: snapshot agosto
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.catalogo_agosto_staging (
    activo BOOLEAN,
    categoria STRING,
    marca STRING,
    nombre_producto STRING,
    precio_lista STRING,
    producto_id STRING,
    _rescued_data STRING
)
USING DELTA;


-- ------------------------------------------------------------
-- 3. Cargar snapshot marzo 2026
-- ------------------------------------------------------------

COPY INTO bronze.catalogo_marzo_staging
FROM '/Volumes/electrocasa_dev/default/vol_landing/productos/'
FILEFORMAT = JSON
FILES = ('catalogo_productos_2026-03-01.json')
FORMAT_OPTIONS ('multiLine' = 'true');

-- ------------------------------------------------------------
-- 4. Cargar snapshot agosto 2026
-- ------------------------------------------------------------

COPY INTO bronze.catalogo_agosto_staging
FROM '/Volumes/electrocasa_dev/default/vol_landing/productos/'
FILEFORMAT = JSON
FILES = ('catalogo_productos_2026-08-01.json')
FORMAT_OPTIONS ('multiLine' = 'true');


-- ------------------------------------------------------------
-- 5. Crear tabla Bronze consolidada
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS electrocasa_dev.bronze.catalogo_bronze (
    activo BOOLEAN,
    categoria STRING,
    marca STRING,
    nombre_producto STRING,
    precio_lista STRING,
    producto_id STRING,
    _rescued_data STRING,
    fecha_snapshot DATE,
    archivo_origen STRING,
    fecha_ingesta TIMESTAMP
)
USING DELTA;

-- ------------------------------------------------------------
-- 6. Reconstruir tabla Bronze con ambos snapshots
-- ------------------------------------------------------------

DELETE FROM electrocasa_dev.bronze.catalogo_bronze;

INSERT INTO electrocasa_dev.bronze.catalogo_bronze
SELECT
    activo,
    categoria,
    marca,
    nombre_producto,
    precio_lista,
    producto_id,
    _rescued_data,
    DATE '2026-03-01',
    'catalogo_productos_2026-03-01.json',
    current_timestamp()
FROM electrocasa_dev.bronze.catalogo_marzo_staging

UNION ALL

SELECT
    activo,
    categoria,
    marca,
    nombre_producto,
    precio_lista,
    producto_id,
    _rescued_data,
    DATE '2026-08-01',
    'catalogo_productos_2026-08-01.json',
    current_timestamp()
FROM electrocasa_dev.bronze.catalogo_agosto_staging;
