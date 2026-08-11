# Databricks notebook source
# DBTITLE 1,Bronze Layer: Ingest Sales Transactions
# MAGIC %md
# MAGIC # Bronze Layer: Ingest Sales Transactions
# MAGIC
# MAGIC Reads raw CSV from Unity Catalog Volume and creates the bronze Delta table.
# MAGIC
# MAGIC - **Source:** `/Volumes/genie_demo/de_demo/raw_files/sales_transactions.csv`
# MAGIC - **Target:** `genie_demo.de_demo.bronze_sales_transactions`
# MAGIC - **Approach:** All columns preserved as STRING (no type casting). Ingestion metadata added.

# COMMAND ----------

# DBTITLE 1,Environment Setup
#comentario dummy
import json

# Priority: (1) Job task base_parameters, (2) conf/env.json for interactive runs
try:
    catalog     = dbutils.widgets.get("catalog")
    schema      = dbutils.widgets.get("schema")
    volume_path = dbutils.widgets.get("volume_path")
    if not catalog.strip():
        raise ValueError("Empty parameter")
except:
    _conf_path = "/Workspace/Users/franco.caravello@piconsulting.com.ar/genie-databricks-demo/conf/env.json"
    with open(_conf_path) as f:
        _env = json.load(f)
    catalog     = _env["catalog"]
    schema      = _env["schema"]
    volume_path = _env["volume_path"]

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"✓ Environment : {catalog}.{schema}")
print(f"✓ Volume path : {volume_path}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
# Note: volume_path is interpolated directly into the SQL string here instead of
# using SQL variable substitution (SET var = ... / ${var}), which fails with
# CONFIG_NOT_AVAILABLE on the current serverless compute.
bronze_sql = f"""
CREATE OR REPLACE TABLE bronze_sales_transactions
COMMENT 'Bronze layer: raw sales transactions ingested from CSV. All columns preserved as strings with ingestion metadata.'
AS
SELECT
  *,
  _metadata.file_path AS _source_file_path,
  current_timestamp() AS _ingested_at
FROM read_files(
  '{volume_path}/sales_transactions.csv',
  format => 'csv',
  header => true,
  inferColumnTypes => false
)
"""
spark.sql(bronze_sql)

# Column comments
column_comments = {
    "order_id": "Raw order identifier from source CSV",
    "order_date": "Raw order date string (YYYY-MM-DD format, not yet cast to DATE)",
    "customer_id": "Raw customer identifier from source CSV",
    "product_name": "Product name as provided in the source file",
    "category": "Product category (Electronics, Clothing, Home & Kitchen, Sports, Books)",
    "quantity": "Raw quantity string (to be cast to INT at silver layer)",
    "unit_price": "Raw unit price string (to be cast to DECIMAL at silver layer)",
    "discount_pct": "Raw discount percentage string (0-30, to be cast to DECIMAL at silver layer)",
    "payment_method": "Payment method used (Credit Card, Debit Card, PayPal, Bank Transfer, Cash)",
    "region": "Geographic sales region (North, South, East, West, Central)",
    "order_status": "Order lifecycle status (Completed, Cancelled, Returned, Pending)",
    "_source_file_path": "Ingestion metadata: full path of the source file in the UC Volume",
    "_ingested_at": "Ingestion metadata: timestamp when the row was loaded into the bronze table",
}
for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE bronze_sales_transactions ALTER COLUMN {col} COMMENT '{comment}'")

print("✓ Bronze table created")

# COMMAND ----------

# DBTITLE 1,Validate Row Count
# MAGIC %sql
# MAGIC SELECT count(*) AS row_count
# MAGIC FROM bronze_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Inspect Schema
# MAGIC %sql
# MAGIC DESCRIBE TABLE bronze_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Sample Rows
# MAGIC %sql
# MAGIC SELECT * FROM bronze_sales_transactions LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Status Distribution
# MAGIC %sql
# MAGIC SELECT order_status, count(*) AS row_count
# MAGIC FROM bronze_sales_transactions
# MAGIC GROUP BY order_status
# MAGIC ORDER BY row_count DESC;

# COMMAND ----------


