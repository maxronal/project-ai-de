-- ============================================================================
-- SCRIPT DE CALIDAD DE DATOS Y GESTIÓN DE CUARENTENA (SALES)
-- CAPA RAW (STRING) -> CAPA SILVER (DATOS VALIDADOS)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- PASO 1: CREACIÓN DE TABLAS (DDL)
-- ----------------------------------------------------------------------------

-- A. Tabla de Cuarentena / Rechazados
CREATE TABLE IF NOT EXISTS `project-ai-de-ind26.ds_stg.sales_rejected` (
	sale_id STRING,
	customer_id STRING,
	product_id STRING,
	sale_timestamp STRING,
	quantity STRING,
	unit_price STRING,
	rejection_reason STRING,
	failed_dimension STRING,
	ingested_at TIMESTAMP,
	ingestion_date DATE
)
PARTITION BY ingestion_date;

CREATE TABLE IF NOT EXISTS `project-ai-de-ind26.ds_stg.customers_rejected` (
	customer_id STRING,
	signup_date STRING,
	city STRING,
	segment STRING,
	rejection_reason STRING,
	failed_dimension STRING,
	ingested_at TIMESTAMP,
	ingestion_date DATE)
PARTITION BY ingestion_date;

CREATE TABLE IF NOT EXISTS `project-ai-de-ind26.ds_stg.products_rejected` (
	product_id STRING,
	category STRING,
	brand STRING,
	description STRING,
	price STRING,
	rejection_reason STRING,
	failed_dimension STRING,
	ingested_at TIMESTAMP,
	ingestion_date DATE)
PARTITION BY ingestion_date;


-- B. Tabla Silver Destino (Validados y Tipados)
CREATE TABLE IF NOT EXISTS `project-ai-de-ind26.ds_stg.sales` (
	sale_id STRING,
	customer_id STRING,
	product_id STRING,
	sale_timestamp TIMESTAMP,
	quantity INT64,
	unit_price NUMERIC,
	total_line NUMERIC,
	ingested_at TIMESTAMP,
	ingestion_date DATE
)
PARTITION BY ingestion_date;

CREATE TABLE IF NOT EXISTS `project-ai-de-ind26.ds_stg.customers` (
	customer_id STRING,
	signup_date TIMESTAMP,
	city STRING,
	segment STRING,
	ingested_at TIMESTAMP,
	ingestion_date DATE)
PARTITION BY ingestion_date;

CREATE TABLE IF NOT EXISTS `project-ai-de-ind26.ds_stg.products` (
	product_id STRING,
	category STRING,
	brand STRING,
	description STRING,
	price NUMERIC,
	ingested_at TIMESTAMP,
	ingestion_date DATE)
PARTITION BY ingestion_date;


-- ----------------------------------------------------------------------------
-- PASO 2: EVALUACIÓN DE REGLAS MÍNIMAS DE CALIDAD
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `project-ai-de-ind26.ds_raw_ext.tmp_sales_value`
AS
WITH base_sales AS (
  SELECT 
    s.sale_id,
    s.customer_id,
    s.product_id,
    s.sale_timestamp,
    s.quantity,
    s.unit_price,
    s.ingested_at,
    s.ingestion_date,
    -- Conteo para Unicidad
    COUNT(*) OVER(PARTITION BY s.sale_id,s.product_id) AS sale_id_count
  FROM `project-ai-de-ind26.ds_raw_ext.sales` s
),

qualified_sales AS (
  SELECT 
    b.*,
    -- Casteos seguros para validación de tipos
    SAFE_CAST(b.quantity AS INT64) AS parsed_quantity,
    SAFE_CAST(b.unit_price AS NUMERIC) AS parsed_unit_price,
    SAFE_CAST(b.sale_timestamp AS TIMESTAMP) AS parsed_timestamp,
    
    -- Evaluación de reglas de la imagen
    CASE 
      -- 1. Completitud: customer_id y product_id no nulos
      WHEN b.customer_id IS NULL OR TRIM(b.customer_id) = '' OR b.product_id IS NULL OR TRIM(b.product_id) = '' 
        THEN STRUCT(TRUE AS is_rejected, 'Completitud' AS dimension, 'customer_id o product_id son nulos/vacíos' AS reason)

      -- 2. Validez: quantity > 0, unit_price >= 0 y fechas válidas
      WHEN SAFE_CAST(b.quantity AS INT64) IS NULL OR SAFE_CAST(b.quantity AS INT64) <= 0 
        THEN STRUCT(TRUE AS is_rejected, 'Validez' AS dimension, 'quantity debe ser mayor a 0' AS reason)
      WHEN SAFE_CAST(b.unit_price AS NUMERIC) IS NULL OR SAFE_CAST(b.unit_price AS NUMERIC) < 0 
        THEN STRUCT(TRUE AS is_rejected, 'Validez' AS dimension, 'unit_price debe ser >= 0' AS reason)
      WHEN SAFE_CAST(b.sale_timestamp AS TIMESTAMP) IS NULL 
        THEN STRUCT(TRUE AS is_rejected, 'Validez' AS dimension, 'sale_timestamp no tiene un formato de fecha válido' AS reason)

      -- 3. Unicidad: sale_id único
      WHEN b.sale_id IS NULL OR TRIM(b.sale_id) = '' OR b.sale_id_count > 1 
        THEN STRUCT(TRUE AS is_rejected, 'Unicidad' AS dimension, 'sale_id es nulo o está duplicado' AS reason)

      -- 4. Integridad Referencial: customer_id y product_id deben existir en sus maestros
      WHEN c.customer_id IS NULL 
        THEN STRUCT(TRUE AS is_rejected, 'Integridad Referencial' AS dimension, 'customer_id no existe en la tabla de clientes' AS reason)
      WHEN p.product_id IS NULL 
        THEN STRUCT(TRUE AS is_rejected, 'Integridad Referencial' AS dimension, 'product_id no existe en la tabla de productos' AS reason)

      -- Registro Aprobado
      ELSE STRUCT(FALSE AS is_rejected, NULL AS dimension, NULL AS reason)
    END AS quality_flag
  FROM base_sales b
  -- Uniones para validación de Integridad Referencial
  LEFT JOIN `project-ai-de-ind26.ds_raw_ext.customers` c ON b.customer_id = c.customer_id
  LEFT JOIN `project-ai-de-ind26.ds_raw_ext.products` p ON b.product_id = p.product_id
)

SELECT * FROM qualified_sales;


-- ----------------------------------------------------------------------------
-- PASO 3: INSERCIÓN EN TABLA DE CUARENTENA / RECHAZADOS
-- ----------------------------------------------------------------------------
INSERT INTO `project-ai-de-ind26.ds_stg.sales_rejected` (
  sale_id,
  customer_id,
  product_id,
  sale_timestamp,
  quantity,
  unit_price,
  rejection_reason,
  failed_dimension,
  ingested_at,
  ingestion_date
)
SELECT 
  sale_id,
  customer_id,
  product_id,
  sale_timestamp,
  quantity,
  unit_price,
  quality_flag.reason AS rejection_reason,
  quality_flag.dimension AS failed_dimension,
  ingested_at,
  ingestion_date
FROM `project-ai-de-ind26.ds_raw_ext.tmp_sales_value`
WHERE quality_flag.is_rejected = TRUE;


-- ----------------------------------------------------------------------------
-- PASO 4: INSERCIÓN Y TRANSFORMACIÓN A LA CAPA SILVER (SOLO REGISTROS VÁLIDOS)
-- ----------------------------------------------------------------------------
INSERT INTO `project-ai-de-ind26.ds_stg.sales` (
  sale_id,
  customer_id,
  product_id,
  sale_timestamp,
  quantity,
  unit_price,
  total_line,
  ingested_at,
  ingestion_date
)
SELECT 
  sale_id,
  customer_id,
  product_id,
  parsed_timestamp AS sale_timestamp,
  parsed_quantity AS quantity,
  parsed_unit_price AS unit_price,
  -- Consistencia: Cálculo seguro del total_line = quantity * unit_price
  ROUND(parsed_quantity * parsed_unit_price, 2) AS total_line,
  -- Frescura: timestamp de ingesta y fecha de partición de negocio
  ingested_at,
  ingestion_date
FROM `project-ai-de-ind26.ds_raw_ext.tmp_sales_value`
WHERE quality_flag.is_rejected = FALSE;


INSERT INTO `project-ai-de-ind26.ds_stg.customers` (
	customer_id,
	signup_date,
	city,
	segment,
	ingested_at,
	ingestion_date
)
SELECT 
	customer_id,
	PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', signup_date) as signup_date,
	city,
	segment,
	ingested_at,
	ingestion_date
FROM `project-ai-de-ind26.ds_raw_ext.customers`;
WHERE exits (select 1 from `project-ai-de-ind26.ds_raw_ext.tmp_sales_value`
WHERE );

INSERT INTO `project-ai-de-ind26.ds_stg.products` (
	product_id,
	category,
	brand,
	description,
	price,
	ingested_at,
	ingestion_date
)
SELECT 
	product_id,
	category,
	brand,
	description,
	CAST(price AS NUMERIC) as price,
	ingested_at,
	ingestion_date
FROM `project-ai-de-ind26.ds_raw_ext.products`;
WHERE ;
  LEFT JOIN `project-ai-de-ind26.ds_raw_ext.customers` c ON b.customer_id = c.customer_id
  LEFT JOIN `project-ai-de-ind26.ds_raw_ext.products` p ON b.product_id = p.product_id

DROP TABLE `project-ai-de-ind26.ds_raw_ext.tmp_sales_value`;