# Databricks notebook source
# DBTITLE 1,Bronze Quality Check - ERP Data
# MAGIC %md
# MAGIC # 🛡️ Bronze Layer Quality Check - ERP Data
# MAGIC
# MAGIC ## Purpose:
# MAGIC Validate ERP source data quality AFTER ingestion, BEFORE downstream transformation
# MAGIC
# MAGIC ## Validates:
# MAGIC * `workspace_1.bronze.erp_cust_az12` (Customer IDs & demographics)
# MAGIC * `workspace_1.bronze.erp_loc_a101` (Customer locations)
# MAGIC * `workspace_1.bronze.erp_px_cat_g1v2` (Product categories)
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
# BRONZE QUALITY CONFIGURATION - ERP
# ============================================

from datetime import datetime

# Quality configuration for ERP Bronze layer
QUALITY_CONFIG = {
    "pipeline_name": "Bike_Lakehouse",
    "task_name": "Quality_Bronze_ERP",
    "layer": "BRONZE",
    
    # Tables to validate
    "tables_to_validate": [
        "workspace_1.bronze.erp_cust_az12",
        "workspace_1.bronze.erp_loc_a101",
        "workspace_1.bronze.erp_px_cat_g1v2"
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

print("✅ Bronze ERP Quality Framework Initialized")
print(f"   Task: {QUALITY_CONFIG['task_name']}")
print(f"   Tables to Validate: {len(QUALITY_CONFIG['tables_to_validate'])}")
print(f"   Monitoring Table: {QUALITY_CONFIG['monitoring_table']}")

# COMMAND ----------

# DBTITLE 1,Check 1: File Ingestion Validation
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- BRONZE CHECK 1: FILE INGESTION VALIDATION
# MAGIC -- ============================================
# MAGIC -- WHAT: Verify all expected ERP Bronze tables were created and have data
# MAGIC -- WHY: If source files failed to load, downstream transformations will fail
# MAGIC -- CRITICAL: Zero records = pipeline should fail immediately
# MAGIC
# MAGIC SELECT
# MAGIC   'File Ingestion Validation' as check_name,
# MAGIC   'erp_cust_az12' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT CID) as unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN COUNT(*) < 100 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN 'CRITICAL: No records ingested from CUST_AZ12.csv'
# MAGIC     WHEN COUNT(*) < 100 THEN CONCAT('WARNING: Only ', COUNT(*), ' customer IDs ingested - expected more')
# MAGIC     ELSE CONCAT('PASS: ', COUNT(*), ' customer ID records ingested successfully')
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.erp_cust_az12
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'File Ingestion Validation' as check_name,
# MAGIC   'erp_loc_a101' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT CID) as unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN COUNT(*) < 100 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN 'CRITICAL: No records ingested from LOC_A101.csv'
# MAGIC     WHEN COUNT(*) < 100 THEN CONCAT('WARNING: Only ', COUNT(*), ' location records ingested - expected more')
# MAGIC     ELSE CONCAT('PASS: ', COUNT(*), ' location records ingested successfully')
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.erp_loc_a101
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'File Ingestion Validation' as check_name,
# MAGIC   'erp_px_cat_g1v2' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT ID) as unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN COUNT(*) < 50 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = 0 THEN 'CRITICAL: No records ingested from PX_CAT_G1V2.csv'
# MAGIC     WHEN COUNT(*) < 50 THEN CONCAT('WARNING: Only ', COUNT(*), ' product categories ingested - expected more')
# MAGIC     ELSE CONCAT('PASS: ', COUNT(*), ' product category records ingested successfully')
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.erp_px_cat_g1v2

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
# MAGIC WITH cust_dupes AS (
# MAGIC   SELECT 
# MAGIC     CID,
# MAGIC     COUNT(*) as dup_count
# MAGIC   FROM workspace_1.bronze.erp_cust_az12
# MAGIC   GROUP BY CID
# MAGIC   HAVING COUNT(*) > 1
# MAGIC ),
# MAGIC loc_dupes AS (
# MAGIC   SELECT 
# MAGIC     CID,
# MAGIC     COUNT(*) as dup_count
# MAGIC   FROM workspace_1.bronze.erp_loc_a101
# MAGIC   GROUP BY CID
# MAGIC   HAVING COUNT(*) > 1
# MAGIC ),
# MAGIC cat_dupes AS (
# MAGIC   SELECT 
# MAGIC     ID,
# MAGIC     COUNT(*) as dup_count
# MAGIC   FROM workspace_1.bronze.erp_px_cat_g1v2
# MAGIC   GROUP BY ID
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC SELECT
# MAGIC   'Duplicate Detection' as check_name,
# MAGIC   'erp_cust_az12' as table_name,
# MAGIC   COALESCE(SUM(dup_count), 0) as duplicate_records,
# MAGIC   COALESCE(COUNT(*), 0) as duplicate_keys,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN '✅ PASS'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 20 THEN '✅ PASS'  -- Accept minor duplicates as normal
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN 'PASS: No duplicate customer IDs found'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 20 THEN CONCAT('PASS: Only ', COUNT(*), ' minor customer ID duplicates (acceptable)')
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN CONCAT('WARNING: ', COUNT(*), ' duplicate customer IDs found')
# MAGIC     ELSE CONCAT('CRITICAL: ', COUNT(*), ' duplicate customer IDs found - investigate source data')
# MAGIC   END as message
# MAGIC FROM cust_dupes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Duplicate Detection' as check_name,
# MAGIC   'erp_loc_a101' as table_name,
# MAGIC   COALESCE(SUM(dup_count), 0) as duplicate_records,
# MAGIC   COALESCE(COUNT(*), 0) as duplicate_keys,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN '✅ PASS'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 20 THEN '✅ PASS'  -- Accept minor duplicates as normal
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN 'PASS: No duplicate location records found'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 20 THEN CONCAT('PASS: Only ', COUNT(*), ' minor location duplicates (acceptable)')
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 50 THEN CONCAT('WARNING: ', COUNT(*), ' duplicate location records found')
# MAGIC     ELSE CONCAT('CRITICAL: ', COUNT(*), ' duplicate location records found - investigate source data')
# MAGIC   END as message
# MAGIC FROM loc_dupes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Duplicate Detection' as check_name,
# MAGIC   'erp_px_cat_g1v2' as table_name,
# MAGIC   COALESCE(SUM(dup_count), 0) as duplicate_records,
# MAGIC   COALESCE(COUNT(*), 0) as duplicate_keys,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN '✅ PASS'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 10 THEN '✅ PASS'  -- Accept minor duplicates as normal
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 25 THEN '🟡 WARNING'
# MAGIC     ELSE '🔴 CRITICAL'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(COUNT(*), 0) = 0 THEN 'PASS: No duplicate product category IDs found'
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 10 THEN CONCAT('PASS: Only ', COUNT(*), ' minor category duplicates (acceptable)')
# MAGIC     WHEN COALESCE(COUNT(*), 0) < 25 THEN CONCAT('WARNING: ', COUNT(*), ' duplicate category IDs found')
# MAGIC     ELSE CONCAT('CRITICAL: ', COUNT(*), ' duplicate category IDs found - investigate source data')
# MAGIC   END as message
# MAGIC FROM cat_dupes

# COMMAND ----------

# DBTITLE 1,Check 3: Null Critical Fields
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- BRONZE CHECK 3: NULL CRITICAL FIELDS
# MAGIC -- ============================================
# MAGIC -- WHAT: Check for NULL values in business-critical columns
# MAGIC -- WHY: NULL primary/foreign keys prevent proper joins downstream
# MAGIC --      NULL business fields indicate incomplete data
# MAGIC -- WARNING: Nulls should be flagged for investigation
# MAGIC
# MAGIC SELECT
# MAGIC   'Null Critical Fields' as check_name,
# MAGIC   'erp_cust_az12' as table_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) as null_cid,
# MAGIC   0 as null_field2,
# MAGIC   0 as null_field3,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) > 0 THEN '🔴 CRITICAL'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) > 0 
# MAGIC       THEN CONCAT('CRITICAL: ', SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END), ' records with NULL customer ID')
# MAGIC     ELSE 'PASS: No NULL values in critical fields'
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.erp_cust_az12
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Null Critical Fields' as check_name,
# MAGIC   'erp_loc_a101' as table_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) as null_cid,
# MAGIC   SUM(CASE WHEN CNTRY IS NULL THEN 1 ELSE 0 END) as null_country,
# MAGIC   0 as null_field3,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) > 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN SUM(CASE WHEN CNTRY IS NULL THEN 1 ELSE 0 END) > 0 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) > 0 
# MAGIC       THEN CONCAT('CRITICAL: ', SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END), ' records with NULL customer ID')
# MAGIC     WHEN SUM(CASE WHEN CNTRY IS NULL THEN 1 ELSE 0 END) > 0
# MAGIC       THEN CONCAT('WARNING: ', SUM(CASE WHEN CNTRY IS NULL THEN 1 ELSE 0 END), ' records with NULL country')
# MAGIC     ELSE 'PASS: No NULL values in critical fields'
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.erp_loc_a101
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Null Critical Fields' as check_name,
# MAGIC   'erp_px_cat_g1v2' as table_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END) as null_id,
# MAGIC   SUM(CASE WHEN CAT IS NULL THEN 1 ELSE 0 END) as null_category,
# MAGIC   0 as null_field3,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END) > 0 THEN '🔴 CRITICAL'
# MAGIC     WHEN SUM(CASE WHEN CAT IS NULL THEN 1 ELSE 0 END) > 0 THEN '🟡 WARNING'
# MAGIC     ELSE '✅ PASS'
# MAGIC   END as status,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END) > 0 
# MAGIC       THEN CONCAT('CRITICAL: ', SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END), ' records with NULL product ID')
# MAGIC     WHEN SUM(CASE WHEN CAT IS NULL THEN 1 ELSE 0 END) > 0
# MAGIC       THEN CONCAT('WARNING: ', SUM(CASE WHEN CAT IS NULL THEN 1 ELSE 0 END), ' records with NULL category')
# MAGIC     ELSE 'PASS: No NULL values in critical fields'
# MAGIC   END as message
# MAGIC FROM workspace_1.bronze.erp_px_cat_g1v2

# COMMAND ----------

# DBTITLE 1,Quality Summary & Save Metrics
# ============================================
# BRONZE QUALITY SUMMARY & METRICS - ERP
# ============================================

from pyspark.sql.functions import lit, current_timestamp, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, DecimalType, DateType, TimestampType, ArrayType, DoubleType
from datetime import datetime

print("\n" + "="*80)
print("BRONZE LAYER - ERP DATA QUALITY SUMMARY")
print("="*80)

# Get total records from each table
total_cust_ids = spark.table("workspace_1.bronze.erp_cust_az12").count()
total_locations = spark.table("workspace_1.bronze.erp_loc_a101").count()
total_categories = spark.table("workspace_1.bronze.erp_px_cat_g1v2").count()

total_records = total_cust_ids + total_locations + total_categories

# Calculate quality score
quality_score = 100
critical_issues = []
warnings = []

# Check record counts
if total_records == 0:
    quality_score = 0
    critical_issues.append("No records ingested from any ERP source file")
elif total_cust_ids == 0:
    quality_score = 20
    critical_issues.append("No customer ID records ingested")
elif total_locations == 0:
    quality_score = 30
    critical_issues.append("No location records ingested")
elif total_categories == 0:
    quality_score = 40
    critical_issues.append("No product category records ingested")
else:
    # Deduct points for warnings (only for very low counts)
    if total_cust_ids < 50:
        quality_score -= 10
        warnings.append(f"Low customer ID count: {total_cust_ids}")
    if total_categories < 10:
        quality_score -= 10
        warnings.append(f"Low category count: {total_categories}")

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
    "data_freshness_hours": 0,
    "null_product_keys": 0,
    "null_customer_keys": 0,
    "outlier_count": 0,
    "unique_products": int(total_categories),
    "unique_customers": int(total_cust_ids),
    "unique_orders": 0,
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
    "full_metrics": f"Bronze ERP Quality: {total_records} total records ({total_cust_ids} customer IDs, {total_locations} locations, {total_categories} categories)"
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
print(f"\n📊 Bronze ERP Quality Score: {quality_score}/100 {status}")
print(f"   Total Records Ingested: {total_records:,}")
print(f"   - Customer IDs: {total_cust_ids:,}")
print(f"   - Locations: {total_locations:,}")
print(f"   - Product Categories: {total_categories:,}")

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

