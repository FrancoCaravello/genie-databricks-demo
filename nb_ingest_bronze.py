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

# DBTITLE 1,Create Bronze Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE genie_demo.de_demo.bronze_sales_transactions
# MAGIC COMMENT 'Bronze layer: raw sales transactions ingested from CSV. All columns preserved as strings with ingestion metadata.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   _metadata.file_path AS _source_file_path,
# MAGIC   current_timestamp() AS _ingested_at
# MAGIC FROM read_files(
# MAGIC   '/Volumes/genie_demo/de_demo/raw_files/sales_transactions.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferColumnTypes => false
# MAGIC );
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN order_id COMMENT 'Raw order identifier from source CSV';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN order_date COMMENT 'Raw order date string (YYYY-MM-DD format, not yet cast to DATE)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN customer_id COMMENT 'Raw customer identifier from source CSV';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN product_name COMMENT 'Product name as provided in the source file';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN category COMMENT 'Product category (Electronics, Clothing, Home & Kitchen, Sports, Books)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN quantity COMMENT 'Raw quantity string (to be cast to INT at silver layer)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN unit_price COMMENT 'Raw unit price string (to be cast to DECIMAL at silver layer)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN discount_pct COMMENT 'Raw discount percentage string (0-30, to be cast to DECIMAL at silver layer)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN payment_method COMMENT 'Payment method used (Credit Card, Debit Card, PayPal, Bank Transfer, Cash)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN region COMMENT 'Geographic sales region (North, South, East, West, Central)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN order_status COMMENT 'Order lifecycle status (Completed, Cancelled, Returned, Pending)';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN _source_file_path COMMENT 'Ingestion metadata: full path of the source file in the UC Volume';
# MAGIC ALTER TABLE genie_demo.de_demo.bronze_sales_transactions ALTER COLUMN _ingested_at COMMENT 'Ingestion metadata: timestamp when the row was loaded into the bronze table';

# COMMAND ----------

# DBTITLE 1,Validate Row Count
# MAGIC %sql
# MAGIC SELECT count(*) AS row_count
# MAGIC FROM genie_demo.de_demo.bronze_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Inspect Schema
# MAGIC %sql
# MAGIC DESCRIBE TABLE genie_demo.de_demo.bronze_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Sample Rows
# MAGIC %sql
# MAGIC SELECT * FROM genie_demo.de_demo.bronze_sales_transactions LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Status Distribution
# MAGIC %sql
# MAGIC SELECT order_status, count(*) AS row_count
# MAGIC FROM genie_demo.de_demo.bronze_sales_transactions
# MAGIC GROUP BY order_status
# MAGIC ORDER BY row_count DESC;

# COMMAND ----------


