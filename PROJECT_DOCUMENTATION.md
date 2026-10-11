# IKEA India Product Data Pipeline Using SQL — Technical Documentation

## 1. Introduction

This project implements an SQL-based data engineering pipeline using Databricks. It processes IKEA India product data through Bronze, Silver, and Gold layers and creates analytical tables for product pricing, ratings, categories, discounts, and badges.

Apache Airflow is used to orchestrate execution of the Databricks job.

## 2. Problem Statement

Product data needs to be organized and prepared before it can be used for business analysis. Values such as prices, discounts, ratings, and descriptions may require cleaning or conversion into consistent data types.

This project uses SQL transformations to prepare the data and generate reusable analytical tables.

## 3. Objectives

1. Load source data into a Bronze table.
2. Clean and transform product attributes in Silver.
3. Apply data quality checks and categorization rules.
4. Create Gold tables for business analysis.
5. Verify record counts and analytical outputs.
6. Orchestrate pipeline execution using Apache Airflow and Databricks Jobs.

## 4. Tools and Technologies

| Technology | Purpose |
|---|---|
| SQL | Data cleaning, transformations, and analytical queries |
| Databricks | SQL execution and table management |
| Medallion Architecture | Separating raw, cleaned, and analytical data |
| Apache Airflow | Workflow orchestration |
| Git and GitHub | Version control and sharing |

## 5. Source Data

The source table is:

`workspace.default.bronze_IKEA`

It contained 10,414 records in the previously verified run.

The dataset includes product information such as IDs, names, prices, ratings, URLs, image URLs, discounts, badges, categories, and descriptions, depending on the available source fields.

## 6. Architecture

IKEA Product Data  
↓  
Databricks Source Table  
↓  
Bronze Layer — Raw Data  
↓  
Silver Layer — Cleaned and Validated Data  
↓  
Gold Layer — Business Analytics  
↓  
Business Insights

Orchestration:

Apache Airflow DAG  
↓  
DatabricksRunNowOperator  
↓  
SQL Databricks Job  
↓  
SQL ETL Notebook  
↓  
Bronze → Silver → Gold

## 7. ETL Process

### Step 1: Load Bronze

Table:

`ikea_medallion.sql_bronze.ikea_products`

The Bronze table is created from the configured source table:

```sql
CREATE OR REPLACE TABLE ikea_medallion.sql_bronze.ikea_products AS
SELECT *
FROM workspace.default.bronze_IKEA;
```

**Why:** Bronze provides a structured starting point for downstream transformations while retaining the source fields.

### Step 2: Transform Silver

Table:

`ikea_medallion.sql_silver.ikea_products`

The Silver layer prepares product fields for analysis.

The implemented transformations include:

- Extracting numeric values from price strings.
- Converting price and discount values into suitable numeric types.
- Cleaning product IDs, names, URLs, image URLs, and descriptions.
- Creating data quality flags.
- Categorizing ratings.
- Categorizing product prices into bands.

**Why:** Consistent data types and normalized fields support more reliable SQL queries and comparisons.

### Handling Missing Descriptions

When a source description is null or blank, the pipeline uses the product name as a fallback value.

Conceptually:

```sql
COALESCE(
    NULLIF(TRIM(CAST(description AS STRING)), ''),
    NULLIF(TRIM(CAST(product_name AS STRING)), '')
)
```

This ensures that the cleaned description field can be populated when the product name is available. The fallback is a product name, not a full product description.

### Step 3: Create Gold Tables

Gold tables are created under:

`ikea_medallion.sql_gold`

| Table | Business purpose |
|---|---|
| `high_price_products` | Analyze products priced above ₹1,000 |
| `highest_price_products` | Identify the highest-priced products |
| `lowest_price_products` | Identify the lowest-priced products |
| `high_rating_products` | Find products rated 4.5 or higher |
| `category_business_insights` | Analyze product categories |
| `rating_business_insights` | Summarize rating patterns |
| `price_business_insights` | Analyze pricing patterns |
| `discount_business_insights` | Analyze discounts |
| `badge_business_insights` | Analyze product badges |
| `price_rating_insights` | Compare prices and ratings |

**Why:** Gold tables organize the transformed data around business questions and make it easier to query.

## 8. Data Quality and Results

Results from a previously verified pipeline run:

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

These counts describe the earlier run. Re-run the verification queries if the source data or transformation logic changes.

## 9. Verification Queries

### Bronze count

```sql
SELECT COUNT(*) AS bronze_count
FROM ikea_medallion.sql_bronze.ikea_products;
```

### Silver count

```sql
SELECT COUNT(*) AS silver_count
FROM ikea_medallion.sql_silver.ikea_products;
```

### Gold high-price count

```sql
SELECT COUNT(*) AS gold_high_price_count
FROM ikea_medallion.sql_gold.high_price_products;
```

Previously verified results were 10,414, 10,414, and 7,616 respectively.

## 10. Apache Airflow Orchestration

The Airflow DAG is named:

`ikea_sql_medallion_pipeline`

It uses `DatabricksRunNowOperator` to trigger the configured SQL Databricks job.

Execution sequence:

1. Start the Airflow scheduler and webserver.
2. Open the Airflow web interface.
3. Trigger the SQL pipeline DAG.
4. Airflow calls the Databricks job.
5. Databricks executes the SQL ETL notebook.
6. The notebook creates or updates the Bronze, Silver, and Gold tables.
7. Airflow waits for the job result and displays the task status.

**Why:** Airflow provides repeatable execution and centralized monitoring of the pipeline.

## 11. Project Deliverables

- `IKEA_SQL_Medallion_ETL.py` — exported Databricks notebook source.
- `README.md` — project overview and instructions.
- `PROJECT_DOCUMENTATION.md` — detailed technical documentation.
- `architecture.png` — architecture diagram to be added.

## 12. Security

Do not upload Databricks tokens, passwords, private connection details, or other secrets to GitHub.

Use Airflow Connections or an appropriate secret-management mechanism for credentials. Public documentation should use placeholders rather than real credentials.

## 13. Conclusion

This project demonstrates SQL-based data transformation in Databricks, the Medallion Architecture, data quality checks, Gold-layer analytical tables, and Airflow orchestration.

It provides practical experience with SQL ETL development, data preparation, validation, and workflow automation.

This is an educational portfolio project and is not affiliated with or endorsed by IKEA.
