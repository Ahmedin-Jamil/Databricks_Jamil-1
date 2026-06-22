# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 🔄 Gold Layer Orchestration
# MAGIC
# MAGIC ## Orchestration Logic
# MAGIC
# MAGIC This notebook programmatically runs all Gold notebooks in sequence.
# MAGIC It becomes the **single entry point** for the Gold layer in Databricks Jobs.
# MAGIC
# MAGIC **Gold Transformations:**
# MAGIC - Dimension tables (Customers, Products)
# MAGIC - Fact table (Sales)

# COMMAND ----------

# Gold transformation notebooks to run in sequence
notebooks = [
    "./Gold_Dim_Customers",
    "./Gold_dim_products",
    "./Gold_fact_sales"
]

for nb in notebooks:
    print(f"Running {nb}")
    dbutils.notebook.run(nb, timeout_seconds=0)
    
print("\n✅ Gold Layer Orchestration Complete")
