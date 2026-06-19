# Bike Lakehouse - Databricks Data Engineering Project

## About This Project

This is a personal data engineering project I built to practice real-world pipeline development using Azure Databricks.

The goal was to take raw bicycle sales data from two different source systems, clean and integrate the data across multiple transformation layers, and produce a final data model that is ready for reporting and analysis.

The project follows the **Medallion Architecture** (Bronze, Silver, Gold), which is a standard pattern used by data engineering teams in the industry.

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
