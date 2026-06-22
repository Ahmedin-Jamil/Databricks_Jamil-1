# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Initialize Lakehouse Infrastructure
# MAGIC
# MAGIC ## Purpose
# MAGIC Sets up the foundational infrastructure for the Bike Lakehouse project:
# MAGIC - Unity Catalog workspace catalog (if needed)
# MAGIC - Bronze, Silver, Gold schemas
# MAGIC - Volumes for raw source files
# MAGIC
# MAGIC **Run this notebook ONCE before running any data pipelines.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use workspace_1 catalog
# MAGIC USE CATALOG workspace_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Bronze layer: raw ingested data';
# MAGIC
# MAGIC -- Create Silver schema  
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Silver layer: cleaned and transformed data';
# MAGIC
# MAGIC -- Create Gold schema
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Gold layer: business-ready data';
# MAGIC
# MAGIC -- Create Monitoring schema
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring
# MAGIC COMMENT 'Monitoring: data quality logs and metrics';

# COMMAND ----------

# DBTITLE 1,Create Volume
# MAGIC %sql
# MAGIC -- Create volume for raw source files (if using Volumes)
# MAGIC CREATE VOLUME IF NOT EXISTS workspace_1.bronze.raw_sources
# MAGIC COMMENT 'Volume for raw source files (CSV)';

# COMMAND ----------

# DBTITLE 1,Verify Setup
# Verify schemas were created
schemas = spark.sql("SHOW SCHEMAS IN workspace_1").collect()
schema_names = [row.databaseName for row in schemas]

print("✅ Lakehouse Infrastructure Setup Complete\n")
print("Created Schemas:")
for schema in ['bronze', 'silver', 'gold', 'monitoring']:
    if schema in schema_names:
        print(f"   ✓ workspace_1.{schema}")
    else:
        print(f"   ✗ workspace_1.{schema} - NOT FOUND")

# Check if volume exists
try:
    volumes = spark.sql("SHOW VOLUMES IN workspace_1.bronze").collect()
    print(f"\n✓ Volume: workspace_1.bronze.raw_sources")
except:
    print(f"\n✗ Volume creation may have failed - check permissions")
