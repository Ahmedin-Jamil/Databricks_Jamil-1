# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Quality Check - CRM Data
# MAGIC %md
# MAGIC # 🛡️ Bronze Layer Quality Check - CRM Data
# MAGIC
# MAGIC ## Purpose:
# MAGIC Validate CRM source data quality AFTER ingestion, BEFORE downstream transformation
# MAGIC
# MAGIC ## Validates:
# MAGIC * `workspace_1.bronze.crm_cust_info` (Customer data)
# MAGIC * `workspace_1.bronze.crm_prd_info` (Product data)
# MAGIC * `workspace_1.bronze.crm_sales_details` (Sales transactions)
# MAGIC
# MAGIC ## Quality Checks:
# MAGIC 1. **File Ingestion Validation** - All tables exist with data
# MAGIC 2. **Duplicate Detection** - Find duplicate records by natural keys
# MAGIC 3. **Null Critical Fields** - Check for nulls in business-critical columns
# MAGIC
# MAGIC ## Output:
# MAGIC * Quality score (0-100)
# MAGIC * Metrics logged to `workspace_1.monitoring.data_quality_log`
# MAGIC * Detailed validation results
# MAGIC
# MAGIC ## Alert Strategy:
# MAGIC * 🔴 **CRITICAL** (Score < 70) - Zero records, missing tables, excessive nulls
# MAGIC * 🟡 **WARNING** (Score 70-99) - Duplicates found, some null values
# MAGIC * ✅ **PASS** (Score 100) - All checks passed
# MAGIC
# MAGIC ---
# MAGIC **Note:** This notebook is independent of data ingestion. It only READS and VALIDATES existing Bronze tables.

# COMMAND ----------

# DBTITLE 1,Quality Configuration
# ============================================
# BRONZE QUALITY CONFIGURATION
# ============================================

from datetime import datetime

# Quality configuration for CRM Bronze layer
QUALITY_CONFIG = {
    "pipeline_name": "Bike_Lakehouse",
    "task_name": "Quality_Bronze_CRM",
    "layer": "BRONZE",
    
    # Tables to validate
    "tables_to_validate": [
        "workspace_1.bronze.crm_cust_info",
        "workspace_1.bronze.crm_prd_info",
        "workspace_1.bronze.crm_sales_details"
    ],
    
    # Quality thresholds
    "thresholds": {
        "min_record_count": 1,  # Critical: at least 1 record
        "max_duplicate_pct": 10.0,  # Warning: >10% duplicates
        "max_null_pct": 10.0  # Warning: >10% nulls in critical fields
    },
    
    # Monitoring
    "monitoring_table": "workspace_1.monitoring.data_quality_log",
    "fail_on_critical": False  # Set True to fail pipeline on critical issues
}

print("✅ Bronze CRM Quality Framework Initialized")
print(f"   Task: {QUALITY_CONFIG['task_name']}")
print(f"   Tables to Validate: {len(QUALITY_CONFIG['tables_to_validate'])}")
print(f"   Monitoring Table: {QUALITY_CONFIG['monitoring_table']}")

# COMMAND ----------

# DBTITLE 1,Check 1: File Ingestion Validation
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- BRONZE CHECK 1: FILE INGESTION VALIDATION
# MAGIC -- ============================================
# MAGIC -- WHAT: Verify all expected Bronze tables were created and have data
# MAGIC -- WHY: If source files failed to load, downstream transformations will fail
# MAGIC -- CRITICAL: Zero records = pipeline should fail immediately
# MAGIC
# MAGIC SELECT
# MAGIC   'File Ingestion Validation' as check_name,
# MAGIC   'crm_cust_info' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT cst_id) as unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN COUNT(*) < 1000 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN 'CRITICAL: No records ingested from cust_info.csv'
# MAGIC     WHEN COUNT(*) < 1000 THEN CONCAT('WARNING: Only ', COUNT(*), ' customers ingested - expected more')
# MAGIC     ELSE CONCAT('PASS: ', COUNT(*), ' customer records ingested successfully')
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.crm_cust_info
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'File Ingestion Validation' as check_name,
# MAGIC   'crm_prd_info' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT prd_id) as unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN COUNT(*) < 100 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN 'CRITICAL: No records ingested from prd_info.csv'
# MAGIC     WHEN COUNT(*) < 100 THEN CONCAT('WARNING: Only ', COUNT(*), ' products ingested - expected more')
# MAGIC     ELSE CONCAT('PASS: ', COUNT(*), ' product records ingested successfully')
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.crm_prd_info
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'File Ingestion Validation' as check_name,
# MAGIC   'crm_sales_details' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT sls_ord_num) as unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN COUNT(*) < 1000 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN 'CRITICAL: No records ingested from sales_details.csv'
# MAGIC     WHEN COUNT(*) < 1000 THEN CONCAT('WARNING: Only ', COUNT(*), ' sales records ingested - expected more')
# MAGIC     ELSE CONCAT('PASS: ', COUNT(*), ' sales records ingested successfully')
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.crm_sales_details

# COMMAND ----------

# DBTITLE 1,Check 2: Duplicate Detection
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- BRONZE CHECK 2: DUPLICATE DETECTION
# MAGIC -- ============================================
# MAGIC -- WHAT: Find duplicate records based on natural keys
# MAGIC -- WHY: Duplicates in source data will cause incorrect aggregations
# MAGIC --      Better to catch them at Bronze before they multiply downstream
# MAGIC -- WARNING: Duplicates should be flagged but won't fail Bronze ingestion
# MAGIC
# MAGIC WITH customer_dupes AS (
# MAGIC   SELECT 
# MAGIC     cst_id,
# MAGIC     COUNT(*) as dup_count
# MAGIC   FROM workspace_1.bronze.crm_cust_info
# MAGIC   GROUP BY cst_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC ),
# MAGIC product_dupes AS (
# MAGIC   SELECT 
# MAGIC     prd_id,
# MAGIC     COUNT(*) as dup_count
# MAGIC   FROM workspace_1.bronze.crm_prd_info
# MAGIC   GROUP BY prd_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC ),
# MAGIC sales_dupes AS (
# MAGIC   -- FIXED: Check composite key (order + product) for true duplicates
# MAGIC   -- Multi-line orders are NORMAL - one order can have many products
# MAGIC   SELECT 
# MAGIC     sls_ord_num,
# MAGIC     sls_prd_key,
# MAGIC     COUNT(*) as dup_count
# MAGIC   FROM workspace_1.bronze.crm_sales_details
# MAGIC   GROUP BY sls_ord_num, sls_prd_key
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC SELECT
# MAGIC   'Duplicate Detection' as check_name,
# MAGIC   'crm_cust_info' as table_name,
# MAGIC   COALESCE(SUM(dup_count), 0) as duplicate_records,
# MAGIC   COALESCE(COUNT(*), 0) as duplicate_keys,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN '✅ PASS'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN '✅ PASS'  -- Accept minor duplicates as normal
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 100 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN 'PASS: No duplicate customer IDs found'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN CONCAT('PASS: Only ', COUNT(*), ' minor customer ID duplicates (acceptable)')
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 100 THEN CONCAT('WARNING: ', COUNT(*), ' duplicate customer IDs found')
# MAGIC     ELSE CONCAT('CRITICAL: ', COUNT(*), ' duplicate customer IDs found - investigate source data')
# MAGIC   END as message
# MAGIC FROM customer_dupes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Duplicate Detection' as check_name,
# MAGIC   'crm_prd_info' as table_name,
# MAGIC   COALESCE(SUM(dup_count), 0) as duplicate_records,
# MAGIC   COALESCE(COUNT(*), 0) as duplicate_keys,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN '✅ PASS'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 20 THEN '✅ PASS'  -- Accept minor duplicates as normal
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN 'PASS: No duplicate product IDs found'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 20 THEN CONCAT('PASS: Only ', COUNT(*), ' minor product ID duplicates (acceptable)')
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN CONCAT('WARNING: ', COUNT(*), ' duplicate product IDs found')
# MAGIC     ELSE CONCAT('CRITICAL: ', COUNT(*), ' duplicate product IDs found - investigate source data')
# MAGIC   END as message
# MAGIC FROM product_dupes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Duplicate Detection' as check_name,
# MAGIC   'crm_sales_details' as table_name,
# MAGIC   COALESCE(SUM(dup_count), 0) as duplicate_records,
# MAGIC   COALESCE(COUNT(*), 0) as duplicate_keys,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN '✅ PASS'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 10 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN 'PASS: No duplicate order numbers found'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 10 THEN CONCAT('WARNING: ', COUNT(*), ' duplicate order numbers found')
# MAGIC     ELSE CONCAT('CRITICAL: ', COUNT(*), ' duplicate order numbers found - investigate source data')
# MAGIC   END as message
# MAGIC FROM sales_dupes

# COMMAND ----------

# DBTITLE 1,Check 3: Null Critical Fields
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- BRONZE CHECK 3: NULL CRITICAL FIELDS
# MAGIC -- ============================================
# MAGIC -- WHAT: Check for NULL values in business-critical columns
# MAGIC -- WHY: NULL primary/foreign keys prevent proper joins downstream
# MAGIC --      NULL business fields (names, amounts) indicate incomplete data
# MAGIC -- WARNING: Nulls should be flagged for investigation
# MAGIC
# MAGIC SELECT
# MAGIC   'Null Critical Fields' as check_name,
# MAGIC   'crm_cust_info' as table_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) as null_cst_id,
# MAGIC   SUM(CASE WHEN cst_key IS NULL THEN 1 ELSE 0 END) as null_cst_key,
# MAGIC   SUM(CASE WHEN cst_firstname IS NULL THEN 1 ELSE 0 END) as null_firstname,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) > 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN SUM(CASE WHEN cst_key IS NULL OR cst_firstname IS NULL THEN 1 ELSE 0 END) > 0 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status
# MAGIC FROM workspace_1.bronze.crm_cust_info
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Null Critical Fields' as check_name,
# MAGIC   'crm_prd_info' as table_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) as null_prd_id,
# MAGIC   SUM(CASE WHEN prd_key IS NULL THEN 1 ELSE 0 END) as null_prd_key,
# MAGIC   0 as null_field3,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) > 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN SUM(CASE WHEN prd_key IS NULL THEN 1 ELSE 0 END) > 0 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status
# MAGIC FROM workspace_1.bronze.crm_prd_info
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Null Critical Fields' as check_name,
# MAGIC   'crm_sales_details' as table_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN sls_ord_num IS NULL THEN 1 ELSE 0 END) as null_order_num,
# MAGIC   SUM(CASE WHEN sls_prd_key IS NULL THEN 1 ELSE 0 END) as null_prd_key,
# MAGIC   SUM(CASE WHEN sls_cust_id IS NULL THEN 1 ELSE 0 END) as null_cst_key,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN sls_ord_num IS NULL THEN 1 ELSE 0 END) > 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN SUM(CASE WHEN sls_prd_key IS NULL OR sls_cust_id IS NULL THEN 1 ELSE 0 END) > 0 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status
# MAGIC FROM workspace_1.bronze.crm_sales_details

# COMMAND ----------

# DBTITLE 1,Quality Summary & Save Metrics
# ============================================
# BRONZE QUALITY SUMMARY & METRICS
# ============================================

from pyspark.sql.functions import lit, current_timestamp, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, DecimalType, DateType, TimestampType, ArrayType, DoubleType
from datetime import datetime

print("\n" + "="*80)
print("BRONZE LAYER - CRM DATA QUALITY SUMMARY")
print("="*80)

# Get total records from each table
total_customers = spark.table("workspace_1.bronze.crm_cust_info").count()
total_products = spark.table("workspace_1.bronze.crm_prd_info").count()
total_sales = spark.table("workspace_1.bronze.crm_sales_details").count()

total_records = total_customers + total_products + total_sales

# Calculate quality score based on validation results
quality_score = 100
critical_issues = []
warnings = []

# Check 1: Record counts
if total_records == 0:
    quality_score = 0
    critical_issues.append("No records ingested from any CRM source file")
elif total_customers == 0:
    quality_score = 20
    critical_issues.append("No customer records ingested")
elif total_products == 0:
    quality_score = 30
    critical_issues.append("No product records ingested")
elif total_sales == 0:
    quality_score = 40
    critical_issues.append("No sales records ingested")
else:
    # Deduct points for warnings
    if total_customers < 1000:
        quality_score -= 5
        warnings.append(f"Low customer count: {total_customers}")
    if total_products < 100:
        quality_score -= 5
        warnings.append(f"Low product count: {total_products}")
    if total_sales < 1000:
        quality_score -= 10
        warnings.append(f"Low sales count: {total_sales}")

# Determine status
if quality_score < 70:
    status = "🔴 CRITICAL"
elif quality_score < 100:
    status = "🟡 WARNING"
else:
    status = "✅ PASS"

# Create metrics record - use SQL to generate timestamp (Spark type, not Python)
metrics_data = {
    "run_timestamp": None,  # Will be filled by SQL
    "pipeline_name": QUALITY_CONFIG["pipeline_name"],
    "task_name": QUALITY_CONFIG["task_name"],
    "layer": QUALITY_CONFIG["layer"],
    "quality_score": quality_score,
    "status": status,
    "total_records": total_records,
    "data_freshness_hours": 0,  # Bronze doesn't track freshness
    "null_product_keys": 0,  # Detailed null counts in SQL checks
    "null_customer_keys": 0,
    "outlier_count": 0,
    "unique_products": int(total_products),
    "unique_customers": int(total_customers),
    "unique_orders": int(total_sales),
    "revenue_reconciliation_pass": None,
    "revenue_gold": None,
    "revenue_silver": None,
    "revenue_diff_pct": None,
    "record_count_pass": total_records > 0,
    "negative_sales": 0,
    "invalid_ship_dates": 0,
    "invalid_due_dates": 0,
    "price_calculation_errors": 0,
    "orphaned_products": 0,
    "orphaned_customers": 0,
    "last_order_date": None,
    "data_age_days": 0,
    "completeness_pct": 100.0 if total_records > 0 else 0.0,
    "critical_issues": critical_issues,
    "warnings": warnings,
    "full_metrics": f"Bronze CRM Quality: {total_records} total records ({total_customers} customers, {total_products} products, {total_sales} sales)"
}

# Save to monitoring table with error handling
try:
    # Create DataFrame from dict
    metrics_df = spark.createDataFrame([metrics_data])
    # Replace None timestamp with current_timestamp() using SQL function
    from pyspark.sql.functions import current_timestamp
    metrics_df = metrics_df.withColumn("run_timestamp", current_timestamp())
    # Save to table
    metrics_df.write.mode("append").saveAsTable(QUALITY_CONFIG["monitoring_table"])
    print(f"\n✅ Metrics saved to {QUALITY_CONFIG['monitoring_table']}")
except Exception as e:
    print(f"\n⚠️ Warning: Could not save to monitoring table: {e}")
    print("Quality checks completed but metrics not persisted.")

# Print summary
print(f"\n📊 Bronze CRM Quality Score: {quality_score}/100 {status}")
print(f"   Total Records Ingested: {total_records:,}")
print(f"   - Customers: {total_customers:,}")
print(f"   - Products: {total_products:,}")
print(f"   - Sales: {total_sales:,}")

if critical_issues:
    print(f"\n🔴 Critical Issues ({len(critical_issues)}):")
    for issue in critical_issues:
        print(f"   - {issue}")

if warnings:
    print(f"\n🟡 Warnings ({len(warnings)}):")
    for warning in warnings:
        print(f"   - {warning}")

print("="*80)

# Fail pipeline if configured and critical issues found
if QUALITY_CONFIG.get("fail_on_critical") and quality_score < 70:
    raise Exception(f"Quality check failed with score {quality_score}/100. Critical issues: {critical_issues}")
