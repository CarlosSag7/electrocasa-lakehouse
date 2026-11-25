@'
-- ============================================================
-- ELECTROCASA - GOBERNANZA Y SEGURIDAD
-- Unity Catalog: electrocasa_dev
-- ============================================================

-- 1. INGENIERIA: lectura y modificacion en Bronze, Silver y Gold

GRANT USE CATALOG
ON CATALOG electrocasa_dev
TO `electrocasa_ingenieria`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA electrocasa_dev.bronze
TO `electrocasa_ingenieria`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA electrocasa_dev.silver
TO `electrocasa_ingenieria`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_ingenieria`;


-- 2. ANALISTAS: solo lectura en Gold

GRANT USE CATALOG
ON CATALOG electrocasa_dev
TO `electrocasa_analistas`;

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_analistas`;

GRANT SELECT
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_analistas`;


-- 3. AUDITORIA: solo lectura en Gold

GRANT USE CATALOG
ON CATALOG electrocasa_dev
TO `electrocasa_auditoria`;

GRANT USE SCHEMA
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_auditoria`;

GRANT SELECT
ON SCHEMA electrocasa_dev.gold
TO `electrocasa_auditoria`;


-- 4. EVIDENCIA DE REVOKE
-- Se concedio temporalmente SELECT y USE SCHEMA a analistas
-- sobre Silver. Posteriormente se revocaron ambos permisos.
-- La comprobacion SHOW GRANTS confirmo su eliminacion.

-- GRANT USE SCHEMA ON SCHEMA electrocasa_dev.silver
-- TO `electrocasa_analistas`;

-- GRANT SELECT ON SCHEMA electrocasa_dev.silver
-- TO `electrocasa_analistas`;

-- REVOKE SELECT ON SCHEMA electrocasa_dev.silver
-- FROM `electrocasa_analistas`;

-- REVOKE USE SCHEMA ON SCHEMA electrocasa_dev.silver
-- FROM `electrocasa_analistas`;


-- 5. LIMITACION DE AUDITORIA EN FREE EDITION
-- Se intento conceder USE CATALOG sobre system, pero Databricks
-- devolvio UNAUTHORIZED_ACCESS: el usuario no tiene MANAGE
-- sobre el catalogo system.
-- Como alternativa se utilizo Catalog Explorer > Lineage.
-- Evidencia: evidencias/06_linaje_ventas.png

-- GRANT USE CATALOG ON CATALOG system
-- TO `electrocasa_auditoria`;


-- 6. CONSULTAS DE VERIFICACION

SHOW GRANTS ON SCHEMA electrocasa_dev.bronze;
SHOW GRANTS ON SCHEMA electrocasa_dev.silver;
SHOW GRANTS ON SCHEMA electrocasa_dev.gold;
'@ | Set-Content -Encoding UTF8 "src\electrocasa\governance\permisos.sql"