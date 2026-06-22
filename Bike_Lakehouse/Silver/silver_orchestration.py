# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 🔄 Silver Layer Orchestration
# MAGIC
# MAGIC ## Orchestration Logic
# MAGIC
# MAGIC This notebook programmatically runs all Silver transformation notebooks in sequence.
# MAGIC It becomes the **single entry point** for the Silver layer in Databricks Jobs.
# MAGIC
# MAGIC **Silver Transformations:**
# MAGIC - **ERP**: Customer IDs, Location data, Product categories  
# MAGIC - **CRM**: Customer info, Product info, Sales details

# COMMAND ----------

# Silver transformation notebooks to run in sequence
notebooks = [
    "./Silver_crm_customers_info",
    "./Silver_crm_products_info",
    "./Silver_crm_sales_details",
    "./silver_erp_cust_az12",
    "./silver_erp_loc_a101",
    "./silver_erp_px_cat_g1v2"
]

for nb in notebooks:
    print(f"Running {nb}")
    dbutils.notebook.run(nb, timeout_seconds=0)
    
print("\n✅ Silver Layer Orchestration Complete")
