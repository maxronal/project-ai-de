-- ============================================================================
-- 1. CREACIÓN DE TABLAS EXTERNAS DESDE LAS SUBCARPETAS DE FECHA EN GCS
-- ============================================================================

-- A. Ventas (Sales) desde gs://raw/sales/ingestion_date=*/*.csv
CREATE OR REPLACE EXTERNAL TABLE `tu_proyecto.raw_dataset.ext_sales`
OPTIONS (
  format = 'CSV',
  uris = ['gs://raw/sales/ingestion_date=*/*.csv'],
  skip_leading_rows = 1
);

-- B. Clientes (Customers) desde gs://raw/customers/ingestion_date=*/*.json
CREATE OR REPLACE EXTERNAL TABLE `tu_proyecto.raw_dataset.ext_customers`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://raw/customers/ingestion_date=*/*.json']
);

-- C. Productos (Products) desde gs://raw/products/ingestion_date=*/*.csv
CREATE OR REPLACE EXTERNAL TABLE `tu_proyecto.raw_dataset.ext_products`
OPTIONS (
  format = 'CSV',
  uris = ['gs://raw/products/ingestion_date=*/*.csv'],
  skip_leading_rows = 1
);


-- ============================================================================
-- 2. REGLA DE CALIDAD BLOQUEANTE (FAIL FAST)
-- ============================================================================
-- Verifica la integridad referencial y de negocio en las ventas antes de continuar.

DECLARE invalid_records INT64;

SET invalid_records = (
  SELECT COUNT(1)
  FROM `tu_proyecto.raw_dataset.ext_sales`
  WHERE customer_id IS NULL 
     OR sale_id IS NULL 
     OR product_id IS NULL
     OR quantity <= 0 
     OR unit_price < 0
);

-- Si se detecta al menos 1 registro inválido, aborta el DAG de Airflow inmediatamente
IF invalid_records > 0 THEN
  ERROR(CONCAT(
    'REGLA DE CALIDAD BLOQUEANTE VIOLADA: Se encontraron ', 
    CAST(invalid_records AS STRING), 
    ' registro(s) corruptos o nulos en las subcarpetas de ventas. Pipeline abortado.'
  ));
END IF;


-- ============================================================================
-- 3. CARGA A LA CAPA STAGING EN BIGQUERY (SI PASA LA VALIDACIÓN)
-- ============================================================================

-- A. Staging de Ventas
CREATE OR REPLACE TABLE `tu_proyecto.raw_dataset.stg_sales_raw` AS
SELECT 
  CAST(sale_id AS STRING) AS sale_id,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(product_id AS STRING) AS product_id,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) AS sale_timestamp,
  CAST(quantity AS INT64) AS quantity,
  CAST(unit_price AS NUMERIC) AS unit_price,
  CURRENT_TIMESTAMP() AS ingested_at
FROM `tu_proyecto.raw_dataset.ext_sales`;

-- B. Staging Maestro de Clientes
CREATE OR REPLACE TABLE `tu_proyecto.raw_dataset.stg_customers_raw` AS
SELECT 
  CAST(customer_id AS STRING) AS customer_id,
  PARSE_DATE('%Y-%m-%d', signup_date) AS signup_date,
  CAST(city AS STRING) AS city,
  CAST(segment AS STRING) AS segment,
  CURRENT_TIMESTAMP() AS ingested_at
FROM `tu_proyecto.raw_dataset.ext_customers`;

-- C. Staging Maestro de Productos
CREATE OR REPLACE TABLE `tu_proyecto.raw_dataset.stg_products_raw` AS
SELECT 
  CAST(product_id AS STRING) AS product_id,
  CAST(category AS STRING) AS category,
  CAST(brand AS STRING) AS brand,
  CAST(description AS STRING) AS description,
  CAST(price AS NUMERIC) AS price,
  CURRENT_TIMESTAMP() AS ingested_at
FROM `tu_proyecto.raw_dataset.ext_products`;