# Databricks notebook source
# DBTITLE 1,Environment Setup
import json

_conf_path = "/Workspace/Users/franco.caravello@piconsulting.com.ar/genie-databricks-demo/conf/env.json"
with open(_conf_path) as f:
    _env = json.load(f)

catalog     = _env["catalog"]
schema      = _env["schema"]

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"✓ Environment : {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Silver Layer: Clean & Type Sales Transactions
# MAGIC %md
# MAGIC # Silver Layer: Clean & Type Sales Transactions
# MAGIC
# MAGIC Reads from bronze, applies type casting, normalization, and computed columns.
# MAGIC
# MAGIC - **Source:** `genie_demo.de_demo.bronze_sales_transactions`
# MAGIC - **Target:** `genie_demo.de_demo.silver_sales_transactions`
# MAGIC
# MAGIC ### Transformations applied:
# MAGIC | Column | Transformation |
# MAGIC |--------|---------------|
# MAGIC | order_date | STRING → DATE |
# MAGIC | quantity | STRING → INT |
# MAGIC | unit_price | STRING → DECIMAL(10,2) |
# MAGIC | discount_pct | STRING → DECIMAL(5,2) |
# MAGIC | order_status | Normalized to UPPER CASE for consistency |
# MAGIC | gross_amount | Computed: quantity × unit_price |
# MAGIC | net_amount | Computed: quantity × unit_price × (1 − discount_pct / 100) |
# MAGIC | _rescued_data | Dropped (all nulls at bronze) |

# COMMAND ----------

# DBTITLE 1,Environment Setup
import json

# Load environment config from conf/env.json (differs per Git branch)
_conf_path = "/Workspace/Users/franco.caravello@piconsulting.com.ar/genie-databricks-demo/conf/env.json"
with open(_conf_path) as f:
    _env = json.load(f)

catalog     = _env["catalog"]
schema      = _env["schema"]
volume_path = _env["volume_path"]

# Set default catalog and schema for all %sql cells in this notebook
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"✓ Environment : {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Create Silver Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_sales_transactions
# MAGIC COMMENT 'Silver layer: cleaned and typed sales transactions with computed revenue columns. Sourced from bronze layer.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   CAST(order_date AS DATE) AS order_date,
# MAGIC   customer_id,
# MAGIC   TRIM(product_name) AS product_name,
# MAGIC   TRIM(category) AS category,
# MAGIC   CAST(quantity AS INT) AS quantity,
# MAGIC   CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
# MAGIC   CAST(discount_pct AS DECIMAL(5,2)) AS discount_pct,
# MAGIC   TRIM(payment_method) AS payment_method,
# MAGIC   TRIM(region) AS region,
# MAGIC   UPPER(TRIM(order_status)) AS order_status,
# MAGIC   -- Computed columns
# MAGIC   CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) AS gross_amount,
# MAGIC   ROUND(
# MAGIC     CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) * (1 - CAST(discount_pct AS DECIMAL(5,2)) / 100),
# MAGIC     2
# MAGIC   ) AS net_amount,
# MAGIC   -- Ingestion metadata (carried from bronze)
# MAGIC   _source_file_path,
# MAGIC   _ingested_at
# MAGIC FROM bronze_sales_transactions;
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN order_id COMMENT 'Unique order identifier';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN order_date COMMENT 'Order date (cast from string to DATE)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN customer_id COMMENT 'Customer identifier';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN product_name COMMENT 'Product name (trimmed)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN category COMMENT 'Product category: Electronics, Clothing, Home & Kitchen, Sports, Books';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN quantity COMMENT 'Order quantity (cast to INT)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN unit_price COMMENT 'Unit price in USD (cast to DECIMAL(10,2))';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN discount_pct COMMENT 'Discount percentage applied (0-30)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN payment_method COMMENT 'Payment method: Credit Card, Debit Card, PayPal, Bank Transfer, Cash';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN region COMMENT 'Geographic region: North, South, East, West, Central';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN order_status COMMENT 'Normalized order status (UPPER CASE): COMPLETED, CANCELLED, RETURNED, PENDING';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN gross_amount COMMENT 'Gross revenue: quantity * unit_price (before discount)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN net_amount COMMENT 'Net revenue: quantity * unit_price * (1 - discount_pct/100)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN _source_file_path COMMENT 'Ingestion metadata: source file path (carried from bronze)';
# MAGIC ALTER TABLE silver_sales_transactions ALTER COLUMN _ingested_at COMMENT 'Ingestion metadata: bronze ingestion timestamp';

# COMMAND ----------

# DBTITLE 1,Validate Row Counts
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'bronze' AS layer, count(*) AS row_count FROM bronze_sales_transactions
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'silver' AS layer, count(*) AS row_count FROM silver_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Null Check on Key Columns
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
# MAGIC   SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
# MAGIC   SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
# MAGIC   SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
# MAGIC   SUM(CASE WHEN gross_amount IS NULL THEN 1 ELSE 0 END) AS null_gross_amount,
# MAGIC   SUM(CASE WHEN net_amount IS NULL THEN 1 ELSE 0 END) AS null_net_amount
# MAGIC FROM silver_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Validate Status Values
# MAGIC %sql
# MAGIC SELECT order_status, count(*) AS row_count
# MAGIC FROM silver_sales_transactions
# MAGIC GROUP BY order_status
# MAGIC ORDER BY row_count DESC;

# COMMAND ----------

# DBTITLE 1,Validate Calculated Amounts
# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   quantity,
# MAGIC   unit_price,
# MAGIC   discount_pct,
# MAGIC   gross_amount,
# MAGIC   net_amount,
# MAGIC   -- Recalculate to verify
# MAGIC   quantity * unit_price AS expected_gross,
# MAGIC   ROUND(quantity * unit_price * (1 - discount_pct / 100), 2) AS expected_net
# MAGIC FROM silver_sales_transactions
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Check Invalid Values
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS invalid_quantity,
# MAGIC   SUM(CASE WHEN unit_price <= 0 THEN 1 ELSE 0 END) AS invalid_unit_price,
# MAGIC   SUM(CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 1 ELSE 0 END) AS invalid_discount,
# MAGIC   SUM(CASE WHEN net_amount < 0 THEN 1 ELSE 0 END) AS negative_net_amount
# MAGIC FROM silver_sales_transactions;

# COMMAND ----------


