CREATE TABLE`project-ai-de-ind26.ds_analytics.int_customer_transactions` 
AS
  SELECT 
	  b.sale_id,
	  b.customer_id,
	  c.segment,
	  c.city,
	  b.product_id,
	  p.description,
	  p.category,
	  b.quantity AS quantity,
	  b.unit_price AS unit_price,
	  b.total_line,
	  b.ingested_at,
	  b.ingestion_date
  FROM `project-ai-de-ind26.ds_stg.sales` b
  LEFT JOIN `project-ai-de-ind26.ds_stg.customers` c ON b.customer_id = c.customer_id
  LEFT JOIN `project-ai-de-ind26.ds_stg.products` p ON b.product_id = p.product_id;