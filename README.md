# IKEA India Product Data Pipeline Using SQL and Databricks

## 1. Project Overview

This project implements a SQL-based ETL pipeline for IKEA India product data using Databricks SQL. It follows Medallion Architecture with Bronze, Silver, and Gold layers to organize source data, clean and validate product records, and generate business-ready analytical tables.

The pipeline processed 10,414 product records in a previously verified run.

## 2. Project Objectives

- Build a SQL-based ETL pipeline in Databricks.
- Implement Bronze, Silver, and Gold data layers.
- Clean product attributes and convert price, rating, and discount values into suitable data types.
- Apply data quality checks.
- Create analytical tables for pricing, ratings, categories, discounts, and badges.
- Orchestrate pipeline execution through Databricks Jobs and Apache Airflow.

## 3. Technology Stack

- **SQL:** Data transformation and analysis
- **Databricks:** Notebook execution and table management
- **Medallion Architecture:** Data organization
- **Apache Airflow:** Workflow orchestration
- **Git and GitHub:** Version control and project sharing

## 4. Architecture

IKEA Product Data  
↓  
Databricks Source Table  
↓  
Bronze Layer — Raw Data  
↓  
Silver Layer — Cleaned and Validated Data  
↓  
Gold Layer — Business Analytics Tables  
↓  
Business Insights

Apache Airflow can trigger the configured Databricks job to execute the SQL pipeline notebook.

## 5. Pipeline Layers

### Bronze Layer

**Table:** `ikea_medallion.sql_bronze.ikea_products`

The Bronze layer loads the source product records into a structured table.

**Purpose:** Provide a consistent starting point for downstream transformations.

### Silver Layer

**Table:** `ikea_medallion.sql_silver.ikea_products`

The Silver layer prepares fields for analysis, including product IDs, names, URLs, descriptions, prices, offer prices, discounts, ratings, rating counts, data quality flags, rating categories, and price categories.

Missing descriptions are filled with the product name when available. This is a fallback label, not a genuine product description.

**Purpose:** Improve consistency and make product attributes suitable for reliable analysis.

### Gold Layer

Tables are created under the `ikea_medallion.sql_gold` schema.

- `high_price_products`
- `highest_price_products`
- `lowest_price_products`
- `high_rating_products`
- `category_business_insights`
- `rating_business_insights`
- `price_business_insights`
- `discount_business_insights`
- `badge_business_insights`
- `price_rating_insights`

**Purpose:** Store business-ready datasets for common analytical questions.

## 6. Data Quality and Verification

Results from a previously verified run:

| Metric | Result |
|---|---:|
| Bronze records | 10,414 |
| Silver records | 10,414 |
| High-price products | 7,616 |
| High-rating products | 4,870 |
| Category insight rows | 2,981 |
| Rating insight rows | 5 |
| Price insight rows | 5 |
| Discount insight rows | 2 |
| Badge insight rows | 4 |
| Price-rating insight rows | 25 |

These values describe a previous successful run and may change if the input data or transformation rules change.

## 7. Orchestration

Apache Airflow uses `DatabricksRunNowOperator` to trigger the configured SQL Databricks job.

Execution sequence:

1. Trigger the Airflow DAG.
2. Airflow calls the SQL Databricks job.
3. Databricks executes the SQL ETL notebook.
4. Bronze, Silver, and Gold tables are created or updated.
5. Airflow waits for the job result.

## 8. Repository Contents

- `IKEA_SQL_Medallion_ETL.py` — exported Databricks notebook source.
- `README.md` — project overview.
- `PROJECT_DOCUMENTATION.md` — detailed technical documentation.
- `architecture.png` — pipeline architecture diagram.

## 9. Execution

The exported Python file contains Databricks notebook source and may use Databricks-specific utilities. Run it in a compatible Databricks environment with the required source table, catalog, schemas, and permissions configured.

The SQL pipeline's main transformations are SQL statements executed from the notebook. A Python file extension does not mean the transformations are implemented only in Python.

The Airflow DAG requires a configured Databricks job and an Airflow connection named `databricks_default`.

Never publish access tokens, passwords, or other secrets in a public repository.

## 10. Learning Outcomes

- Writing SQL transformations in Databricks.
- Implementing Bronze, Silver, and Gold data layers.
- Handling missing and inconsistent product attributes.
- Applying data quality checks.
- Creating analytical tables for business insights.
- Orchestrating ETL execution using Airflow and Databricks Jobs.

## 11. Disclaimer

This is an educational portfolio project using IKEA product data. It is not affiliated with or endorsed by IKEA.
