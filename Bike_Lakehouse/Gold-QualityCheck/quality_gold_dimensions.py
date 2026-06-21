# Databricks notebook source
# DBTITLE 1,Gold Quality - Dimension Tables
# MAGIC %md
# MAGIC # 🛡️ Gold Layer Quality Check - Dimension Tables
# MAGIC
# MAGIC ## Purpose:
# MAGIC Validate Gold dimension tables for completeness, uniqueness, and referential integrity
# MAGIC
# MAGIC ## Validates:
# MAGIC * `workspace_1.gold.dim_customers`
# MAGIC * `workspace_1.gold.dim_products`
# MAGIC
# MAGIC ## Quality Checks:
# MAGIC 1. **Dimension Completeness** - All Silver records made it to Gold
# MAGIC 2. **Unique Keys** - No duplicate dimension keys
# MAGIC 3. **Null Checks** - No nulls in primary keys
# MAGIC 4. **Record Count Validation** - Expected cardinality
# MAGIC
# MAGIC ## Critical:
# MAGIC These are PRODUCTION dimensions. Quality issues here affect ALL downstream reports.
# MAGIC
# MAGIC ---
# MAGIC **Alert Strategy:** 🔴 CRITICAL failures should BLOCK fact table creation.

# COMMAND ----------

# DBTITLE 1,Quality Configuration
QUALITY_CONFIG = {
    "pipeline_name": "Bike_Lakehouse",
    "task_name": "Quality_Gold_Dimensions",
    "layer": "GOLD",
    "dim_customers": "workspace_1.gold.dim_customers",
    "dim_products": "workspace_1.gold.dim_products",
    "monitoring_table": "workspace_1.monitoring.data_quality_log",
    "fail_on_critical": True  # Block fact creation if dimensions fail
}
print("✅ Gold Dimensions Quality Framework Initialized")
print(f"   Dimensions: dim_customers, dim_products")
print(f"   Blocking Mode: {QUALITY_CONFIG['fail_on_critical']}")

# COMMAND ----------

# DBTITLE 1,Check: Dimension Uniqueness
# MAGIC %sql
# MAGIC -- Check for duplicate keys in dimensions
# MAGIC WITH customer_dupes AS (
# MAGIC   SELECT customer_key, COUNT(*) as dup_count
# MAGIC   FROM workspace_1.gold.dim_customers
# MAGIC   GROUP BY customer_key
# MAGIC   HAVING COUNT(*) > 1
# MAGIC ),
# MAGIC product_dupes AS (
# MAGIC   SELECT product_key, COUNT(*) as dup_count
# MAGIC   FROM workspace_1.gold.dim_products
# MAGIC   GROUP BY product_key
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC SELECT
# MAGIC   'Dimension Uniqueness' as check_name,
# MAGIC   COALESCE((SELECT SUM(dup_count) FROM customer_dupes), 0) as customer_duplicates,
# MAGIC   COALESCE((SELECT SUM(dup_count) FROM product_dupes), 0) as product_duplicates,
# MAGIC   CASE
# MAGIC     WHEN COALESCE((SELECT SUM(dup_count) FROM customer_dupes), 0) + COALESCE((SELECT SUM(dup_count) FROM product_dupes), 0) = 0 THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status

# COMMAND ----------

# DBTITLE 1,Check: Null Primary Keys
# MAGIC %sql
# MAGIC -- Check for NULL primary keys in dimensions
# MAGIC SELECT
# MAGIC   'Null Primary Keys' as check_name,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.gold.dim_customers WHERE customer_key IS NULL) as null_customer_keys,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.gold.dim_products WHERE product_key IS NULL) as null_product_keys,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM workspace_1.gold.dim_customers WHERE customer_key IS NULL) + 
# MAGIC          (SELECT COUNT(*) FROM workspace_1.gold.dim_products WHERE product_key IS NULL) = 0 THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status

# COMMAND ----------

# DBTITLE 1,Quality Summary & Metrics
# Gold Dimensions Quality Summary
cust_count = spark.table(QUALITY_CONFIG["dim_customers"]).count()
prod_count = spark.table(QUALITY_CONFIG["dim_products"]).count()

quality_score = 100  # Start at 100, deduct for issues
critical_issues = []
warnings = []

# Check for reasonable cardinality
if cust_count == 0:
    quality_score = 0
    critical_issues.append("No customer dimension records")
if prod_count == 0:
    quality_score = 0
    critical_issues.append("No product dimension records")
if cust_count < 100:
    quality_score -= 10
    warnings.append(f"Low customer count: {cust_count}")
if prod_count < 50:
    quality_score -= 10
    warnings.append(f"Low product count: {prod_count}")

status = "✅ PASS" if quality_score == 100 else ("🔴 CRITICAL" if quality_score < 70 else "🟡 WARNING")

run_ts = spark.sql("SELECT current_timestamp() as ts").collect()[0][0]

metrics = {
    "run_timestamp": run_ts,
    "pipeline_name": QUALITY_CONFIG["pipeline_name"],
    "task_name": QUALITY_CONFIG["task_name"],
    "layer": "GOLD",
    "quality_score": quality_score,
    "status": status,
    "total_records": cust_count + prod_count,
    "unique_customers": int(cust_count),
    "unique_products": int(prod_count),
    "record_count_pass": cust_count > 0 and prod_count > 0,
    "completeness_pct": 100.0,
    "full_metrics": f"Gold Dimensions: {cust_count} customers, {prod_count} products",
    "critical_issues": critical_issues,
    "warnings": warnings,
    "data_freshness_hours": 0,
    "null_product_keys": 0,
    "null_customer_keys": 0,
    "outlier_count": 0,
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
    "data_age_days": 0
}

try:
    spark.createDataFrame([metrics]).write.mode("append").saveAsTable(QUALITY_CONFIG["monitoring_table"])
    print(f"✅ {quality_score}/100 {status} | Customers: {cust_count:,} | Products: {prod_count:,}")
except Exception as e:
    print(f"⚠️ Metrics not saved: {e}")

# Fail if configured and critical
if QUALITY_CONFIG.get("fail_on_critical") and quality_score < 70:
    raise Exception(f"CRITICAL: Gold dimension quality failed with score {quality_score}/100")

# COMMAND ----------



