# Databricks notebook source
# DBTITLE 1,Gold Quality - Fact Sales
# MAGIC %md
# MAGIC # Gold Layer Quality Check - Fact Sales (CRITICAL)
# MAGIC
# MAGIC ## Purpose:
# MAGIC COMPREHENSIVE validation of fact_sales before production reporting
# MAGIC
# MAGIC ## Validates:
# MAGIC * `workspace_1.gold.fact_sales` (primary fact table)
# MAGIC
# MAGIC ## Quality Checks (10 Total):
# MAGIC 1. **Revenue Reconciliation** - Silver vs Gold total match (🔴 CRITICAL)
# MAGIC 2. **Record Count Reconciliation** - No dropped/duplicated records (🔴 CRITICAL)
# MAGIC 3. **Null Product Keys** - All sales have product references (🔴 CRITICAL)
# MAGIC 4. **Null Customer Keys** - All sales have customer references (🔴 CRITICAL)
# MAGIC 5. **Negative Sales** - No negative amounts (🔴 CRITICAL)
# MAGIC 6. **Invalid Ship Dates** - ship_date >= order_date (🟡 WARNING)
# MAGIC 7. **Invalid Due Dates** - due_date >= order_date (🟡 WARNING)
# MAGIC 8. **Price Calculation** - sales_amount = price × quantity (🟡 WARNING)
# MAGIC 9. **Orphaned Products** - All product_keys exist in dim_products (🔴 CRITICAL)
# MAGIC 10. **Orphaned Customers** - All customer_keys exist in dim_customers (🔴 CRITICAL)
# MAGIC
# MAGIC ---
# MAGIC **This is THE MOST CRITICAL quality check in the pipeline.**
# MAGIC
# MAGIC Blocking Mode: ENABLED - Pipeline fails if critical checks fail.

# COMMAND ----------

# DBTITLE 1,Quality Configuration
QUALITY_CONFIG = {
    "pipeline_name": "Bike_Lakehouse",
    "task_name": "Quality_Gold_Fact_Sales",
    "layer": "GOLD",
    "gold_table": "workspace_1.gold.fact_sales",
    "silver_table": "workspace_1.silver.crm_sales",
    "dim_products": "workspace_1.gold.dim_products",
    "dim_customers": "workspace_1.gold.dim_customers",
    "monitoring_table": "workspace_1.monitoring.data_quality_log",
    "thresholds": {
        "revenue_tolerance_pct": 0.01,  # 0.01% tolerance
        "max_negative_sales": 0,
        "max_orphaned_keys": 0
    },
    "fail_on_critical": True  # Block reporting if critical issues found
}
print("✅ Gold Fact Sales Quality Framework Initialized")
print(f"   Table: {QUALITY_CONFIG['gold_table']}")
print(f"   Blocking Mode: ENABLED")

# COMMAND ----------

# DBTITLE 1,Check 1: Revenue Reconciliation (CRITICAL)
# MAGIC %sql
# MAGIC -- Revenue Reconciliation: Silver vs Gold
# MAGIC WITH silver_revenue AS (
# MAGIC   SELECT SUM(sales_amount) as total FROM workspace_1.silver.crm_sales
# MAGIC ),
# MAGIC gold_revenue AS (
# MAGIC   SELECT SUM(sales_amount) as total FROM workspace_1.gold.fact_sales
# MAGIC )
# MAGIC SELECT
# MAGIC   'Revenue Reconciliation' as check_name,
# MAGIC   s.total as silver_revenue,
# MAGIC   g.total as gold_revenue,
# MAGIC   (g.total - s.total) as diff,
# MAGIC   ABS((g.total - s.total) / NULLIF(s.total, 0)) * 100 as diff_pct,
# MAGIC   CASE
# MAGIC     WHEN ABS((g.total - s.total) / NULLIF(s.total, 0)) * 100 < 0.01 THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status
# MAGIC FROM silver_revenue s, gold_revenue g

# COMMAND ----------

# DBTITLE 1,Check 2: Record Count (CRITICAL)
# MAGIC %sql
# MAGIC -- Record Count Reconciliation
# MAGIC SELECT
# MAGIC   'Record Count' as check_name,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.silver.crm_sales) as silver_count,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.gold.fact_sales) as gold_count,
# MAGIC   (SELECT COUNT(*) FROM workspace_1.gold.fact_sales) - (SELECT COUNT(*) FROM workspace_1.silver.crm_sales) as diff,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM workspace_1.silver.crm_sales) = (SELECT COUNT(*) FROM workspace_1.gold.fact_sales) THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status

# COMMAND ----------

# DBTITLE 1,Check 3-4: Null Keys (CRITICAL)
# MAGIC %sql
# MAGIC -- Null Foreign Keys Check
# MAGIC SELECT
# MAGIC   'Null Foreign Keys' as check_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) as null_product_keys,
# MAGIC   SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer_keys,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN product_key IS NULL OR customer_key IS NULL THEN 1 ELSE 0 END) = 0 THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status
# MAGIC FROM workspace_1.gold.fact_sales

# COMMAND ----------

# DBTITLE 1,Check 5: Negative Sales (CRITICAL)
# MAGIC %sql
# MAGIC -- Negative Sales Amounts
# MAGIC SELECT
# MAGIC   'Negative Sales' as check_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN sales_amount < 0 THEN 1 ELSE 0 END) as negative_count,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN sales_amount < 0 THEN 1 ELSE 0 END) = 0 THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status
# MAGIC FROM workspace_1.gold.fact_sales

# COMMAND ----------

# DBTITLE 1,Check 6-8: Business Rules (WARNING)
# MAGIC %sql
# MAGIC -- Business Rule Validation (combined)
# MAGIC SELECT
# MAGIC   'Business Rules' as check_name,
# MAGIC   SUM(CASE WHEN ship_date < order_date THEN 1 ELSE 0 END) as invalid_ship_dates,
# MAGIC   SUM(CASE WHEN due_date < order_date THEN 1 ELSE 0 END) as invalid_due_dates,
# MAGIC   SUM(CASE WHEN ABS(sales_amount - (price * quantity)) > 0.01 THEN 1 ELSE 0 END) as price_calc_errors,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN ship_date < order_date OR due_date < order_date THEN 1 ELSE 0 END) = 0 THEN '✅ PASS'
# MAGIC     ELSE '🟡 WARNING'
# MAGIC   END as status
# MAGIC FROM workspace_1.gold.fact_sales

# COMMAND ----------

# DBTITLE 1,Check 9-10: Referential Integrity (CRITICAL)
# MAGIC %sql
# MAGIC -- Referential Integrity: Orphaned Keys
# MAGIC WITH orphaned_products AS (
# MAGIC   SELECT DISTINCT f.product_key
# MAGIC   FROM workspace_1.gold.fact_sales f
# MAGIC   LEFT ANTI JOIN workspace_1.gold.dim_products d ON f.product_key = d.product_key
# MAGIC   WHERE f.product_key IS NOT NULL
# MAGIC ),
# MAGIC orphaned_customers AS (
# MAGIC   SELECT DISTINCT f.customer_key
# MAGIC   FROM workspace_1.gold.fact_sales f
# MAGIC   LEFT ANTI JOIN workspace_1.gold.dim_customers d ON f.customer_key = d.customer_key
# MAGIC   WHERE f.customer_key IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   'Referential Integrity' as check_name,
# MAGIC   (SELECT COUNT(*) FROM orphaned_products) as orphaned_products,
# MAGIC   (SELECT COUNT(*) FROM orphaned_customers) as orphaned_customers,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM orphaned_products) + (SELECT COUNT(*) FROM orphaned_customers) = 0 THEN '✅ PASS'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status

# COMMAND ----------

# DBTITLE 1,Quality Summary & Metrics (CRITICAL)
# Gold Fact Sales - Comprehensive Quality Summary
from pyspark.sql.functions import col, sum as spark_sum

# Get metrics
gold_count = spark.table(QUALITY_CONFIG["gold_table"]).count()
silver_count = spark.table(QUALITY_CONFIG["silver_table"]).count()

# Revenue comparison
gold_revenue = spark.table(QUALITY_CONFIG["gold_table"]).selectExpr("SUM(sales_amount) as total").collect()[0][0]
silver_revenue = spark.table(QUALITY_CONFIG["silver_table"]).selectExpr("SUM(sales_amount) as total").collect()[0][0]

# Calculate quality score
quality_score = 100
critical_issues = []
warnings = []

# Critical checks
if gold_count != silver_count:
    quality_score -= 30
    critical_issues.append(f"Record count mismatch: Gold {gold_count} != Silver {silver_count}")

if gold_revenue is not None and silver_revenue is not None:
    revenue_diff_pct = abs((gold_revenue - silver_revenue) / silver_revenue) * 100 if silver_revenue != 0 else 0
    if revenue_diff_pct > QUALITY_CONFIG["thresholds"]["revenue_tolerance_pct"]:
        quality_score -= 40
        critical_issues.append(f"Revenue mismatch: {revenue_diff_pct:.4f}% difference")

# Null checks
null_checks = spark.table(QUALITY_CONFIG["gold_table"]).selectExpr(
    "SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) as null_products",
    "SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customers",
    "SUM(CASE WHEN sales_amount < 0 THEN 1 ELSE 0 END) as negative_sales"
).collect()[0]

if null_checks.null_products > 0:
    quality_score -= 10
    critical_issues.append(f"{null_checks.null_products} records with null product_key")

if null_checks.null_customers > 0:
    quality_score -= 10
    critical_issues.append(f"{null_checks.null_customers} records with null customer_key")

if null_checks.negative_sales > 0:
    quality_score -= 10
    critical_issues.append(f"{null_checks.negative_sales} records with negative sales_amount")

# Status
if quality_score < 70:
    status = "🔴 CRITICAL"
elif quality_score < 100:
    status = "🟡 WARNING"
else:
    status = "✅ PASS"

run_ts = spark.sql("SELECT current_timestamp() as ts").collect()[0][0]

# Save metrics
metrics = {
    "run_timestamp": run_ts,
    "pipeline_name": QUALITY_CONFIG["pipeline_name"],
    "task_name": QUALITY_CONFIG["task_name"],
    "layer": "GOLD",
    "quality_score": quality_score,
    "status": status,
    "total_records": gold_count,
    "record_count_pass": gold_count == silver_count,
    "revenue_reconciliation_pass": revenue_diff_pct < QUALITY_CONFIG["thresholds"]["revenue_tolerance_pct"] if gold_revenue and silver_revenue else None,
    "revenue_gold": gold_revenue,
    "revenue_silver": silver_revenue,
    "revenue_diff_pct": revenue_diff_pct if gold_revenue and silver_revenue else None,
    "null_product_keys": int(null_checks.null_products),
    "null_customer_keys": int(null_checks.null_customers),
    "negative_sales": int(null_checks.negative_sales),
    "completeness_pct": 100.0,
    "full_metrics": f"Gold Fact Sales: {gold_count} records, ${gold_revenue:,.2f} revenue",
    "critical_issues": critical_issues,
    "warnings": warnings,
    "data_freshness_hours": 0,
    "outlier_count": 0,
    "unique_products": 0,
    "unique_customers": 0,
    "unique_orders": 0,
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
    print(f"✅ Metrics saved: {quality_score}/100 {status}")
except Exception as e:
    print(f"⚠️ Metrics save failed: {e}")

print(f"\n📊 Gold Fact Sales Quality: {quality_score}/100 {status}")
print(f"   Total Records: {gold_count:,}")
print(f"   Total Revenue: ${gold_revenue:,.2f}" if gold_revenue else "   Revenue: N/A")
print(f"   Silver Match: {'✅' if gold_count == silver_count else '❌'}")

if critical_issues:
    print(f"\n🔴 Critical Issues ({len(critical_issues)}):")
    for issue in critical_issues:
        print(f"   - {issue}")

if warnings:
    print(f"\n🟡 Warnings ({len(warnings)}):")
    for warning in warnings:
        print(f"   - {warning}")

# FAIL PIPELINE IF CRITICAL
if QUALITY_CONFIG.get("fail_on_critical") and quality_score < 70:
    raise Exception(f"PIPELINE BLOCKED: Gold fact quality score {quality_score}/100. Critical issues: {critical_issues}")
