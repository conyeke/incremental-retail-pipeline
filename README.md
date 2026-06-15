# Incremental Online Retail Data Pipeline (Medallion Architecture)

This repository contains an end-to-end, production-ready data engineering pipeline built on **Databricks** using **PySpark**, **Delta Lake**, and **Structured Streaming**. It processes the UCI Online Retail dataset through a multi-hop (Medallion) architecture.

## 🏗️ Architecture Overview
- **Source**: UCI Online Retail Dataset (simulated as monthly batch drop arrivals via Auto Loader).
- **Bronze Layer**: Raw ingestion of streaming CSVs with schema evolution and metadata tracking.
- **Silver Layer**: Data cleaning, deduplication using Spark Window partition functions, and incremental upserts (`MERGE`) handled per micro-batch.
- **Gold Layer**: Aggregated business metrics (weekly revenue, top products, premium customers) leveraging streaming watermarks for system stability.

## 🚀 How to Run
1. Ensure you have access to a Databricks Workspace with Unity Catalog enabled.
2. Run the notebooks in chronological order:
   - `01_data_partitioning.py` to split dataset by month and year.
   - `02_hourly_file_ingestion.py` to move one file every hour.
   - `03_bronze_layer_ingestion.py` to append only newly ingested file.
   - `04_silver_layer_cleaning.py` to clean and merge the data.
   - `05_gold_layer_aggregations.py` to generate the reporting views.

## 🛠️ Tech Stack
- **Languages:** Python, PySpark SQL
- **Storage & Compute:** Databricks, Unity Catalog Volumes, Delta Lake
- **Streaming:** Spark Structured Streaming, Auto Loader (`cloudFiles`)

## 📂 Project Structure
```bash
online-retail-databricks-pipeline/
│
├── notebooks/
│   ├── 01_data_partitioning.py
│   ├── 02_hourly_file_ingestion.py
│   ├── 03_bronze_layer_ingestion.py
│   ├── 04_silver_layer_cleaning.py
│   └── 05_gold_layer_aggregations.py
│
├── screenshots/
│   ├── data_workflow.png
│   ├── first_run_dashboard.png
│   ├── last_run_dashboard.png
│   └── pipeline_runs.png
│
├── .gitignore
├── README.md
└── requirements.txt
```
