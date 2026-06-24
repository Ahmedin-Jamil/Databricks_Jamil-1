# Bike Lakehouse - Azure Databricks Project

## About This Project

This is a personal project I built to practice real-world pipeline development using Azure Databricks.

The goal was to take raw bicycle sales data from two different source systems, clean and integrate the data across multiple transformation layers, and produce a final data model that is ready for reporting and analysis.

---

## Project Context & Data Source

**Data Source:** This project uses [TPC-H benchmark data](http://www.tpc.org/tpch/), an industry-standard dataset for demonstrating database and analytics capabilities.

**Business Scenario:** The data is framed as a fictional B2B wholesale distribution company with:
- 150,000+ customers across 5 global regions
- 200,000+ products
- Millions of orders spanning
- Two source systems: ERP (customer master, products, locations) and CRM (orders, line items)

**Purpose:** This is a technical demonstration project built to showcase:
End-to-end lakehouse architecture implementation using the medallion architecture and data quality capabilities.

---

## Architecture Overview

```text
CSV Files (ERP + CRM)
        |
   BRONZE LAYER       <- Raw ingestion, no transformation
        |
   SILVER LAYER       <- Cleaning, standardization, validation
        |
    GOLD LAYER        <- Star schema, analytics-ready
        |
MONITORING LAYER      <- Data quality log, trend tracking
```

---

## Key Features & Capabilities

### Data Ingestion
- **Source Systems**: Simulated ERP (customer master, products, locations) and CRM (orders, line items)
- **Ingestion Pattern**: Batch ingestion from CSV files into Delta Lake Bronze tables
- **Metadata Tracking**: All ingested records timestamped with `_ingest_ts` and `_source_table` for lineage

### Data Quality Framework
A comprehensive **multi-layer data quality validation system** that runs at each transformation stage:

**Bronze Layer Quality Checks:**
- File ingestion validation (all expected tables loaded)
- Record count thresholds
- Duplicate detection by natural keys
- Null value detection in critical fields

**Silver Layer Quality Checks:**
- Data type validation and casting
- Referential integrity checks (foreign key relationships)
- Business rule validation (e.g., valid dates, positive amounts)
- Standardization verification

**Gold Layer Quality Checks:**
- Dimensional model integrity
- Aggregate value reconciliation
- Metric consistency validation

**Quality Metrics:**
- Each validation assigns a quality score (0-100)
- Results logged to `workspace.monitoring.data_quality_log`
- Alert thresholds: 🔴 CRITICAL (<70), 🟡 WARNING (70-99), ✅ PASS (100)

### Job Orchestration
- **Databricks Workflows**: End-to-end pipeline orchestration using Databricks Jobs
- **Layer Orchestration**: Silver and Gold layers use orchestration notebooks that programmatically call individual transformation notebooks via `dbutils.notebook.run()`
- **Single Entry Points**: `silver_orchestration.ipynb` and `gold_orchestration.ipynb` serve as single task entry points, simplifying job configuration
- **Task Dependencies**: Bronze → Silver → Gold with proper dependency management
- **Quality Gates**: Each layer includes quality validation tasks before proceeding
- **Error Handling**: Failed quality checks logged but don't block downstream (configurable)
- **Infrastructure Setup**: `init_lakehouse.ipynb` initializes Unity Catalog schemas and volumes before first run

### Analytics & BI
- **Gold Layer Star Schema**: Fact tables (orders) + Dimension tables (customers, products, regions)
- **Business Metrics**: Customer lifetime value, revenue by region, top products, customer segmentation
- **Unity Catalog Metric Views**: Pre-aggregated datasets for dashboard performance
- **Lakeview Dashboard**: 8-widget sales analytics dashboard with KPIs, trends, and segmentation

### Technology Stack
- **Platform**: Azure Databricks (Lakehouse)
- **Storage**: Delta Lake (ACID transactions, time travel)
- **Catalog**: Unity Catalog (governance, lineage, access control)
- **Processing**: Apache Spark (PySpark & Spark SQL)
- **Orchestration**: Databricks Jobs/Workflows
- **Languages**: Python, SQL

---

## Project Structure

```
Bike_Lakehouse/
├── init_lakehouse.ipynb        # Setup: Creates schemas, volumes, catalog
├── Bronze/                     # Raw data ingestion notebooks
│   ├── ingest_erp_data
│   └── ingest_crm_data
├── Bronze-QualityCheck/        # Bronze validation notebooks
│   ├── quality_bronze_erp
│   └── quality_bronze_crm
├── Silver/                     # Data cleaning & transformation
│   ├── Silver_crm_customers_info
│   ├── Silver_crm_products_info
│   ├── Silver_crm_sales_details
│   ├── silver_erp_cust_az12
│   ├── silver_erp_loc_a101
│   └── silver_erp_px_cat_g1v2
├── silver_orchestration.ipynb  # Run all Silver notebooks in sequence
├── Silver-QualityCheck/        # Silver validation notebooks
│   ├── quality_silver_erp_customers
│   ├── quality_silver_erp_location
│   ├── quality_silver_erp_products
│   ├── quality_silver_crm_products
│   └── quality_silver_crm_sales
├── Gold/                       # Analytics ready data models
│   ├── Gold_Dim_Customers
│   ├── Gold_dim_products
│   └── Gold_fact_sales
├── gold_orchestration.ipynb    # Run all Gold notebooks in sequence
└── Gold-QualityCheck/          # Gold validation notebooks
    └── quality_gold_analytics
```

---

## Future Enhancements (Roadmap)

- [ ] Implement streaming ingestion with Auto Loader
- [ ] Add incremental processing with Delta Lake Change Data Feed
- [ ] Expand dashboard with drill-down capabilities and filters
- [ ] Implement data lineage visualization
- [ ] Add CI/CD pipeline with Databricks Asset Bundles (DABs)
- [ ] Create alerting system for quality threshold violations
- [ ] Add ML model for customer churn prediction

---

**Author**: Jamil Al-Amin  
**Focus**: Data Engineering
**Platform**: Azure Databricks  
