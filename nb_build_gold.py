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

# DBTITLE 1,Gold Layer: Sales Summary by Date, Region & Category
# MAGIC %md
# MAGIC # Gold Layer: Sales Summary by Date, Region & Category
# MAGIC
# MAGIC Aggregates completed transactions from silver into business-level metrics.
# MAGIC
# MAGIC - **Source:** `genie_demo.de_demo.silver_sales_transactions`
# MAGIC - **Target:** `genie_demo.de_demo.gold_sales_summary`
# MAGIC
# MAGIC ### Filters:
# MAGIC - Only `order_status = 'COMPLETED'` included (CANCELLED, RETURNED, PENDING excluded)
# MAGIC
# MAGIC ### Aggregation grain:
# MAGIC - `order_date` × `region` × `category`
# MAGIC
# MAGIC ### Metrics:
# MAGIC | Metric | Definition |
# MAGIC |--------|------------|
# MAGIC | total_transactions | COUNT(*) of completed orders |
# MAGIC | total_units_sold | SUM(quantity) |
# MAGIC | total_gross_revenue | SUM(gross_amount) |
# MAGIC | total_net_revenue | SUM(net_amount) |
# MAGIC | avg_transaction_value | AVG(net_amount) |

# COMMAND ----------

# DBTITLE 1,Create Gold Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_sales_summary
# MAGIC COMMENT 'Gold layer: daily sales summary by region and category. Only completed transactions included.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   category,
# MAGIC   COUNT(*) AS total_transactions,
# MAGIC   SUM(quantity) AS total_units_sold,
# MAGIC   SUM(gross_amount) AS total_gross_revenue,
# MAGIC   SUM(net_amount) AS total_net_revenue,
# MAGIC   ROUND(AVG(net_amount), 2) AS avg_transaction_value
# MAGIC FROM silver_sales_transactions
# MAGIC WHERE order_status = 'COMPLETED'
# MAGIC GROUP BY order_date, region, category;
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN order_date COMMENT 'Transaction date';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN region COMMENT 'Geographic region: North, South, East, West, Central';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN category COMMENT 'Product category: Electronics, Clothing, Home & Kitchen, Sports, Books';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN total_transactions COMMENT 'Count of completed orders for this date/region/category';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN total_units_sold COMMENT 'Total quantity of items sold';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN total_gross_revenue COMMENT 'Total gross revenue (before discounts)';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN total_net_revenue COMMENT 'Total net revenue (after discounts)';
# MAGIC ALTER TABLE gold_sales_summary ALTER COLUMN avg_transaction_value COMMENT 'Average net revenue per transaction';

# COMMAND ----------

# DBTITLE 1,Gold Row Count
# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS gold_rows,
# MAGIC   count(DISTINCT order_date) AS distinct_dates,
# MAGIC   count(DISTINCT region) AS distinct_regions,
# MAGIC   count(DISTINCT category) AS distinct_categories
# MAGIC FROM gold_sales_summary;

# COMMAND ----------

# DBTITLE 1,Revenue Reconciliation
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'silver_completed' AS source,
# MAGIC   SUM(net_amount) AS total_net_revenue,
# MAGIC   SUM(gross_amount) AS total_gross_revenue,
# MAGIC   COUNT(*) AS transaction_count
# MAGIC FROM silver_sales_transactions
# MAGIC WHERE order_status = 'COMPLETED'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'gold_aggregated' AS source,
# MAGIC   SUM(total_net_revenue) AS total_net_revenue,
# MAGIC   SUM(total_gross_revenue) AS total_gross_revenue,
# MAGIC   SUM(total_transactions) AS transaction_count
# MAGIC FROM gold_sales_summary;

# COMMAND ----------

# DBTITLE 1,Top Regions by Revenue
# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   SUM(total_net_revenue) AS net_revenue,
# MAGIC   SUM(total_transactions) AS transactions,
# MAGIC   SUM(total_units_sold) AS units_sold
# MAGIC FROM gold_sales_summary
# MAGIC GROUP BY region
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Top Categories by Revenue
# MAGIC %sql
# MAGIC SELECT
# MAGIC   category,
# MAGIC   SUM(total_net_revenue) AS net_revenue,
# MAGIC   SUM(total_transactions) AS transactions,
# MAGIC   SUM(total_units_sold) AS units_sold
# MAGIC FROM gold_sales_summary
# MAGIC GROUP BY category
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Verify Excluded Statuses
# MAGIC %sql
# MAGIC -- Confirm gold only contains COMPLETED transactions
# MAGIC -- This returns the excluded statuses and their counts from silver
# MAGIC SELECT s.order_status, COUNT(*) AS excluded_count
# MAGIC FROM silver_sales_transactions s
# MAGIC WHERE s.order_status != 'COMPLETED'
# MAGIC GROUP BY s.order_status
# MAGIC ORDER BY excluded_count DESC;

# COMMAND ----------


