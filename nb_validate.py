# Databricks notebook source
# DBTITLE 1,Pipeline Validation: Medallion End-to-End
# MAGIC %md
# MAGIC # Pipeline Validation: Medallion End-to-End
# MAGIC
# MAGIC Runs automated checks across all three layers. Fails the job if any assertion is not met.
# MAGIC
# MAGIC | Check | Description |
# MAGIC |-------|-------------|
# MAGIC | 1 | Bronze table has rows |
# MAGIC | 2 | Silver row count matches bronze |
# MAGIC | 3 | No nulls in silver key columns |
# MAGIC | 4 | Gold table has rows |
# MAGIC | 5 | Gold revenue reconciles with silver (COMPLETED only) |

# COMMAND ----------

# DBTITLE 1,Environment Setup
import json

# Load environment config from conf/env.json (differs per Git branch)
_conf_path = "/Workspace/Users/franco.caravello@piconsulting.com.ar/genie-databricks-demo/conf/env.json"
with open(_conf_path) as f:
    _env = json.load(f)

catalog     = _env["catalog"]
schema      = _env["schema"]

# Set default catalog and schema for all %sql cells in this notebook
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"✓ Environment : {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Informational: Layer Row Counts
# MAGIC %sql
# MAGIC SELECT 'bronze' AS layer, count(*) AS row_count FROM bronze_sales_transactions
# MAGIC UNION ALL
# MAGIC SELECT 'silver' AS layer, count(*) AS row_count FROM silver_sales_transactions
# MAGIC UNION ALL
# MAGIC SELECT 'gold'   AS layer, count(*) AS row_count FROM gold_sales_summary;

# COMMAND ----------

# DBTITLE 1,Informational: Silver Null Check
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
# MAGIC   SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
# MAGIC   SUM(CASE WHEN net_amount IS NULL THEN 1 ELSE 0 END) AS null_net_amount
# MAGIC FROM silver_sales_transactions;

# COMMAND ----------

# DBTITLE 1,Informational: Gold Revenue Reconciliation
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'silver_completed' AS source,
# MAGIC   SUM(net_amount) AS total_net_revenue,
# MAGIC   COUNT(*) AS transaction_count
# MAGIC FROM silver_sales_transactions
# MAGIC WHERE order_status = 'COMPLETED'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'gold_aggregated' AS source,
# MAGIC   SUM(total_net_revenue) AS total_net_revenue,
# MAGIC   SUM(total_transactions) AS transaction_count
# MAGIC FROM gold_sales_summary;

# COMMAND ----------

# DBTITLE 1,Assertions: Fail Job if Checks Do Not Pass
bronze_count = spark.sql("SELECT COUNT(*) FROM bronze_sales_transactions").collect()[0][0]
silver_count = spark.sql("SELECT COUNT(*) FROM silver_sales_transactions").collect()[0][0]
gold_count   = spark.sql("SELECT COUNT(*) FROM gold_sales_summary").collect()[0][0]

null_count = spark.sql("""
  SELECT
    SUM(CASE WHEN order_id  IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN net_amount IS NULL THEN 1 ELSE 0 END) AS total_nulls
  FROM silver_sales_transactions
""").collect()[0][0]

revenue_diff = spark.sql("""
  SELECT ABS(
    (SELECT COALESCE(SUM(net_amount), 0)        FROM silver_sales_transactions WHERE order_status = 'COMPLETED') -
    (SELECT COALESCE(SUM(total_net_revenue), 0) FROM gold_sales_summary)
  ) AS diff
""").collect()[0][0]

assert bronze_count > 0,              f"CHECK 1 FAILED: bronze_sales_transactions is empty (count={bronze_count})"
assert silver_count == bronze_count,  f"CHECK 2 FAILED: silver count ({silver_count}) != bronze count ({bronze_count})"
assert null_count == 0,               f"CHECK 3 FAILED: {null_count} null(s) found in silver key columns"
assert gold_count > 0,                f"CHECK 4 FAILED: gold_sales_summary is empty (count={gold_count})"
assert revenue_diff == 0,             f"CHECK 5 FAILED: gold/silver revenue mismatch = {revenue_diff}"

print("All validations passed.")
print(f"  bronze rows  : {bronze_count}")
print(f"  silver rows  : {silver_count}")
print(f"  gold rows    : {gold_count}")
print(f"  null count   : {null_count}")
print(f"  revenue diff : {revenue_diff}")

# COMMAND ----------


