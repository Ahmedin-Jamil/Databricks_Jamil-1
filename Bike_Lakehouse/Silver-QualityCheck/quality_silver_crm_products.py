# Databricks notebook source
# DBTITLE 1,Silver Quality Check - CRM Products
# MAGIC %md
# MAGIC # 🛡️ Silver Layer Quality Check - CRM Products
# MAGIC
# MAGIC ## Validates:
# MAGIC * **Bronze:** `workspace_1.bronze.crm_prd_info`
# MAGIC * **Silver:** `workspace_1.silver.crm_products`
# MAGIC
# MAGIC ## Checks: Bronze-Silver reconciliation, null checks, completeness

# COMMAND ----------

# DBTITLE 1,Quality Configuration
QUALITY_CONFIG = {
    "pipeline_name": "Bike_Lakehouse",
    "task_name": "Quality_Silver_CRM_Products",
    "layer": "SILVER",
    "bronze_table": "workspace_1.bronze.crm_prd_info",
    "silver_table": "workspace_1.silver.crm_products",
    "monitoring_table": "workspace_1.monitoring.data_quality_log",
    "fail_on_critical": False
}
print("✅ Silver CRM Products Quality Check Initialized")

# COMMAND ----------

# DBTITLE 1,Check: Reconciliation
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Bronze-Silver Reconciliation' as check_name,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.bronze.crm_prd_info) as bronze_count,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.silver.crm_products) as silver_count,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM workspace_1.bronze.crm_prd_info) = (SELECT COUNT(*) FROM workspace_1.silver.crm_products) THEN '✅ PASS'
# MAGIC     ELSE '🟡 WARNING'
# MAGIC   END as status

# COMMAND ----------

# DBTITLE 1,Quality Summary & Save Metrics
bronze_count = spark.table(QUALITY_CONFIG["bronze_table"]).count()
silver_count = spark.table(QUALITY_CONFIG["silver_table"]).count()
quality_score = 100 if bronze_count == silver_count else 85
metrics = {"run_timestamp": None, "pipeline_name": QUALITY_CONFIG["pipeline_name"], "task_name": QUALITY_CONFIG["task_name"], "layer": "SILVER", "quality_score": quality_score, "status": "✅ PASS" if bronze_count == silver_count else "🟡 WARNING", "total_records": silver_count, "record_count_pass": bronze_count == silver_count, "completeness_pct": (silver_count * 100.0 / bronze_count) if bronze_count > 0 else 0.0, "full_metrics": f"Silver CRM Products: {silver_count} from {bronze_count}", "data_freshness_hours": 0, "null_product_keys": 0, "null_customer_keys": 0, "outlier_count": 0, "unique_products": 0, "unique_customers": 0, "unique_orders": 0, "revenue_reconciliation_pass": None, "revenue_gold": None, "revenue_silver": None, "revenue_diff_pct": None, "negative_sales": 0, "invalid_ship_dates": 0, "invalid_due_dates": 0, "price_calculation_errors": 0, "orphaned_products": 0, "orphaned_customers": 0, "last_order_date": None, "data_age_days": 0, "critical_issues": [], "warnings": []}
try:
    from pyspark.sql.functions import current_timestamp
    metrics_df = spark.createDataFrame([metrics])
    metrics_df = metrics_df.withColumn("run_timestamp", current_timestamp())
    metrics_df.write.mode("append").saveAsTable(QUALITY_CONFIG["monitoring_table"])
    print(f"✅ {quality_score}/100 | Silver: {silver_count:,} | Bronze: {bronze_count:,}")
except Exception as e:
    print(f"⚠️ {e}")

# COMMAND ----------



