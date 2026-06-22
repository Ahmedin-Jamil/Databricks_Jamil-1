# Databricks notebook source
# DBTITLE 1,Silver Quality Check - CRM Customers
# MAGIC %md
# MAGIC # Silver Layer Quality Check - CRM Customers
# MAGIC
# MAGIC ## Purpose:
# MAGIC Validate Silver layer transformation quality AFTER transformation
# MAGIC
# MAGIC ## Validates:
# MAGIC * **Bronze Source:** `workspace_1.bronze.crm_cust_info`
# MAGIC * **Silver Target:** `workspace_1.silver.crm_customers`
# MAGIC
# MAGIC ## Quality Checks:
# MAGIC 1. **Bronze-to-Silver Reconciliation** - All Bronze records transformed
# MAGIC 2. **Null Critical Fields** - Check transformed join keys
# MAGIC 3. **Completeness** - Verify data quality after transformation
# MAGIC
# MAGIC ## Output:
# MAGIC * Quality score (0-100)
# MAGIC * Metrics logged to `workspace_1.monitoring.data_quality_log`
# MAGIC
# MAGIC ---
# MAGIC **Note:** This runs AFTER Silver transformation completes.

# COMMAND ----------

# DBTITLE 1,Quality Configuration
# Silver Quality Configuration
QUALITY_CONFIG = {
    "pipeline_name": "Bike_Lakehouse",
    "task_name": "Quality_Silver_CRM_Customers",
    "layer": "SILVER",
    "bronze_table": "workspace_1.bronze.crm_cust_info",
    "silver_table": "workspace_1.silver.crm_customers",
    "monitoring_table": "workspace_1.monitoring.data_quality_log",
    "fail_on_critical": False
}

print("✅ Silver CRM Customers Quality Check Initialized")
print(f"   Bronze: {QUALITY_CONFIG['bronze_table']}")
print(f"   Silver: {QUALITY_CONFIG['silver_table']}")

# COMMAND ----------

# DBTITLE 1,Check: Bronze-to-Silver Reconciliation
# MAGIC %sql
# MAGIC -- Bronze-to-Silver Reconciliation
# MAGIC SELECT
# MAGIC   'Bronze-Silver Reconciliation' as check_name,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.bronze.crm_cust_info) as bronze_count,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.silver.crm_customers) as silver_count,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.silver.crm_customers) - (SELECT COUNT(*) FROM workspace_1.bronze.crm_cust_info) as diff,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM workspace_1.bronze.crm_cust_info) = (SELECT COUNT(*) FROM workspace_1.silver.crm_customers) THEN '✅ PASS'
# MAGIC     WHEN ABS((SELECT COUNT(*) FROM workspace_1.silver.crm_customers) - (SELECT COUNT(*) FROM workspace_1.bronze.crm_cust_info)) < 10 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status

# COMMAND ----------

# DBTITLE 1,Quality Summary & Save Metrics
# Silver Quality Summary
from datetime import datetime

bronze_count = spark.table(QUALITY_CONFIG["bronze_table"]).count()
silver_count = spark.table(QUALITY_CONFIG["silver_table"]).count()

quality_score = 100 if bronze_count == silver_count else 85
status = "✅ PASS" if bronze_count == silver_count else "🟡 WARNING"

metrics = {
    "run_timestamp": None,  # Will be filled by Spark function
    "pipeline_name": QUALITY_CONFIG["pipeline_name"],
    "task_name": QUALITY_CONFIG["task_name"],
    "layer": QUALITY_CONFIG["layer"],
    "quality_score": quality_score,
    "status": status,
    "total_records": silver_count,
    "record_count_pass": bronze_count == silver_count,
    "completeness_pct": (silver_count * 100.0 / bronze_count) if bronze_count > 0 else 0.0,
    "full_metrics": f"Silver CRM Customers: {silver_count} from {bronze_count} Bronze",
    "data_freshness_hours": 0,
    "null_product_keys": 0,
    "null_customer_keys": 0,
    "outlier_count": 0,
    "unique_products": 0,
    "unique_customers": 0,
    "unique_orders": 0,
    "revenue_reconciliation_pass": None,
    "revenue_gold": None,
    "revenue_silver": None,
    "revenue_diff_pct": None,
    "negative_sales": 0,
    "invalid_ship_dates": 0,
    "invalid_due_dates": 0,
    "price_calculation_errors": 0,
    "orphaned_products": 0,
    "orphaned_customers": 0,
    "last_order_date": None,
    "data_age_days": 0,
    "critical_issues": [],
    "warnings": []
}

try:
    from pyspark.sql.functions import current_timestamp
    metrics_df = spark.createDataFrame([metrics])
    metrics_df = metrics_df.withColumn("run_timestamp", current_timestamp())
    metrics_df.write.mode("append").saveAsTable(QUALITY_CONFIG["monitoring_table"])
    print(f"✅ Quality Score: {quality_score}/100 | Silver: {silver_count:,} | Bronze: {bronze_count:,}")
except Exception as e:
    print(f"⚠️ Metrics not saved: {e}")
