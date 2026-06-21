# Bike Lakehouse - Databricks Data Engineering Project

## About This Project

This is a personal data engineering project I built to practice real-world pipeline development using Azure Databricks.

The goal was to take raw bicycle sales data from two different source systems, clean and integrate the data across multiple transformation layers, and produce a final data model that is ready for reporting and analysis.

The project follows the **Medallion Architecture** (Bronze, Silver, Gold), which is a standard pattern used by data engineering teams in the industry.

---

## Project Context & Data Source

**Data Source:** This project uses [TPC-H benchmark data](http://www.tpc.org/tpch/), an industry-standard dataset for demonstrating database and analytics capabilities.

**Business Scenario:** The data is framed as a fictional B2B wholesale distribution company with:
- 150,000+ customers across 5 global regions
- 200,000+ products
- Millions of orders spanning 1992-1998
- Two source systems: ERP (customer master, products, locations) and CRM (orders, line items)

**Purpose:** This is a technical demonstration project built to showcase:
- End-to-end lakehouse architecture implementation
- Data engineering best practices (medallion architecture, data quality, orchestration)
- Databricks platform expertise (Spark, Delta Lake, Unity Catalog, Jobs, BI)
- ERP/CRM integration patterns from my SAP background

The focus is on **demonstrating technical skills** using recognized sample data, rather than analyzing proprietary business data.

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
        |
  BI DASHBOARDS       <- Sales analytics visualization
```

---

## Key Features & Capabilities

### 🔄 Data Ingestion
- **Source Systems**: Simulated ERP (customer master, products, locations) and CRM (orders, line items)
- **Ingestion Pattern**: Batch ingestion from CSV files into Delta Lake Bronze tables
- **Metadata Tracking**: All ingested records timestamped with `_ingest_ts` and `_source_table` for lineage

### 🛡️ Data Quality Framework
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

### 🔁 Job Orchestration
- **Databricks Workflows**: End-to-end pipeline orchestration using Databricks Jobs
- **Task Dependencies**: Bronze → Silver → Gold with proper dependency management
- **Quality Gates**: Each layer includes quality validation tasks before proceeding
- **Error Handling**: Failed quality checks logged but don't block downstream (configurable)

### 📊 Analytics & BI
- **Gold Layer Star Schema**: Fact tables (orders) + Dimension tables (customers, products, regions)
- **Business Metrics**: Customer lifetime value, revenue by region, top products, customer segmentation
- **Unity Catalog Metric Views**: Pre-aggregated datasets for dashboard performance
- **Lakeview Dashboard**: 8-widget sales analytics dashboard with KPIs, trends, and segmentation

### 🏗️ Technology Stack
- **Platform**: Azure Databricks (Lakehouse)
- **Storage**: Delta Lake (ACID transactions, time travel)
- **Catalog**: Unity Catalog (governance, lineage, access control)
- **Processing**: Apache Spark (PySpark & Spark SQL)
- **Orchestration**: Databricks Jobs/Workflows
- **Visualization**: Databricks Lakeview Dashboards
- **Languages**: Python, SQL

---

## Project Structure

```
Bike_Lakehouse/
├── Bronze/                     # Raw data ingestion notebooks
│   ├── ingest_erp_data
│   └── ingest_crm_data
├── Bronze-QualityCheck/        # Bronze validation notebooks
│   ├── quality_bronze_erp
│   └── quality_bronze_crm
├── Silver/                     # Data cleaning & transformation
│   ├── transform_customers
│   ├── transform_locations
│   ├── transform_products
│   └── transform_orders
├── Silver-QualityCheck/        # Silver validation notebooks
│   ├── quality_silver_erp_customers
│   ├── quality_silver_erp_location
│   ├── quality_silver_erp_products
│   └── quality_silver_crm_orders
├── Gold/                       # Analytics-ready data models
│   ├── gold_customer_ltv
│   ├── gold_revenue_analysis
│   └── gold_product_performance
└── Gold-QualityCheck/          # Gold validation notebooks
    └── quality_gold_analytics
```

---

## What This Project Demonstrates

### For Data Engineering Roles:
✅ **Medallion Architecture Implementation** - Industry-standard Bronze/Silver/Gold pattern  
✅ **Data Quality Engineering** - Multi-layer validation framework with scoring and alerting  
✅ **Pipeline Orchestration** - Dependency management, error handling, job scheduling  
✅ **Delta Lake Expertise** - ACID transactions, schema evolution, time travel  
✅ **Unity Catalog** - Table management, governance, and lineage  
✅ **Performance Optimization** - Metric views for dashboard performance  

### For Analytics Engineering Roles:
✅ **Dimensional Modeling** - Star schema design for analytics  
✅ **Business Metrics** - Customer segmentation, lifetime value, revenue analysis  
✅ **BI Development** - End-to-end dashboard creation with Lakeview  
✅ **Data Governance** - Proper cataloging, documentation, and access patterns  

### ERP/SAP Background Connection:
✅ **Multi-System Integration** - Simulated ERP + CRM data consolidation (mirrors SAP landscapes)  
✅ **Master Data Management** - Customer and product master data handling  
✅ **Transactional Data Processing** - Order/line-item processing patterns  
✅ **Data Quality Patterns** - Similar to SAP Data Services/Information Steward approaches  

---

## How to Use This Portfolio Project

When discussing this project in interviews:

1. **Start with the business context** - "I built a lakehouse demonstration using TPC-H benchmark data..."
2. **Highlight the data quality framework** - "I implemented comprehensive validation at each layer with automated scoring..."
3. **Emphasize end-to-end ownership** - "I designed the architecture, built the pipelines, implemented quality checks, orchestrated the jobs, AND delivered the analytics dashboard"
4. **Connect to your SAP background** - "This mirrors real-world ERP integration patterns I've seen in SAP environments..."
5. **Show technical depth** - "I used Delta Lake for ACID transactions, Unity Catalog for governance, metric views for performance..."

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
**Focus**: Data Engineering | Analytics Engineering | SAP Integration  
**Platform**: Azure Databricks  
