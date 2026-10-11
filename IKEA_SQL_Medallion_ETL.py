# Databricks notebook source
# ============================================================
# IKEA SQL MEDALLION ETL PIPELINE
# BRONZE -> SILVER -> GOLD
# ============================================================

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS ikea_medallion.sql_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS ikea_medallion.sql_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS ikea_medallion.sql_gold")

# ------------------------------------------------------------
# STEP 1: BRONZE LAYER - RAW DATA
# ------------------------------------------------------------

print("\nSTEP 1: BRONZE LAYER")

spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_bronze.ikea_products
COMMENT 'SQL Bronze layer: raw IKEA product data'
AS
SELECT *
FROM workspace.default.bronze_IKEA
""")

display(spark.sql("""
SELECT COUNT(*) AS bronze_total_products
FROM ikea_medallion.sql_bronze.ikea_products
"""))


# COMMAND ----------

# ------------------------------------------------------------
# STEP 2: SILVER LAYER - CLEAN AND TRANSFORM
# ------------------------------------------------------------

print("\nSTEP 2: SILVER LAYER")

spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_silver.ikea_products
COMMENT 'SQL Silver layer: cleaned and validated IKEA products'
AS
WITH cleaned AS (
    SELECT
        b.*,

        NULLIF(TRIM(CAST(b.productID AS STRING)), '')
            AS clean_productID,

        NULLIF(TRIM(CAST(b.product_name AS STRING)), '')
            AS clean_product_name,

        NULLIF(TRIM(CAST(b.product_url AS STRING)), '')
            AS clean_product_url,

        NULLIF(TRIM(CAST(b.image_urls AS STRING)), '')
            AS clean_image_urls,

        COALESCE(
            NULLIF(TRIM(CAST(b.description AS STRING)), ''),
            NULLIF(TRIM(CAST(b.product_name AS STRING)), '')
        ) AS clean_description,

        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(
                    CAST(b.price AS STRING),
                    r'([0-9][0-9,]*(?:\\.[0-9]+)?)',
                    1
                ),
                ',',
                ''
            ) AS DECIMAL(18,2)
        ) AS price_numeric,

        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(
                    CAST(b.offer_price AS STRING),
                    r'([0-9][0-9,]*(?:\\.[0-9]+)?)',
                    1
                ),
                ',',
                ''
            ) AS DECIMAL(18,2)
        ) AS offer_price_numeric,

        TRY_CAST(
            REGEXP_EXTRACT(
                CAST(b.discount AS STRING),
                r'([0-9]+(?:\\.[0-9]+)?)',
                1
            ) AS DECIMAL(18,2)
        ) AS discount_numeric,

        TRY_CAST(b.rating AS DECIMAL(4,2))
            AS rating_numeric,

        TRY_CAST(b.rating_count AS BIGINT)
            AS rating_count_numeric

    FROM ikea_medallion.sql_bronze.ikea_products b
),

classified AS (
    SELECT
        *,

        CASE
            WHEN clean_productID IS NULL
              OR clean_product_name IS NULL
              OR clean_product_url IS NULL
            THEN 'INVALID'
            ELSE 'VALID'
        END AS data_quality_flag,

        CASE
            WHEN rating_numeric IS NULL THEN 'NO RATING'
            WHEN rating_numeric >= 4.5 THEN 'EXCELLENT'
            WHEN rating_numeric >= 4.0 THEN 'GOOD'
            WHEN rating_numeric >= 3.0 THEN 'AVERAGE'
            ELSE 'LOW'
        END AS rating_category,

        CASE
            WHEN price_numeric IS NULL OR price_numeric < 0
                THEN 'NO PRICE'
            WHEN price_numeric > 1000
                THEN 'ABOVE_RS_1000'
            ELSE 'RS_1000_OR_BELOW'
        END AS price_category,

        CASE
            WHEN price_numeric IS NULL OR price_numeric < 0
                THEN 'INVALID_PRICE'
            WHEN price_numeric > 5000 THEN 'ABOVE_5000'
            WHEN price_numeric > 2000 THEN '2001_5000'
            WHEN price_numeric > 1000 THEN '1001_2000'
            WHEN price_numeric > 500 THEN '501_1000'
            ELSE '0_500'
        END AS price_band

    FROM cleaned
)

SELECT *
FROM classified
""")

print("Silver table created.")

display(spark.sql("""
SELECT
    COUNT(*) AS total_products,
    COUNT(price_numeric) AS products_with_numeric_price,
    MIN(price_numeric) AS minimum_price_rs,
    MAX(price_numeric) AS maximum_price_rs,
    SUM(CASE WHEN price_numeric > 1000 THEN 1 ELSE 0 END)
        AS products_above_1000,
    SUM(CASE WHEN discount_numeric IS NOT NULL THEN 1 ELSE 0 END)
        AS products_with_numeric_discount,
    SUM(CASE WHEN data_quality_flag = 'VALID' THEN 1 ELSE 0 END)
        AS valid_products,
    SUM(CASE WHEN data_quality_flag = 'INVALID' THEN 1 ELSE 0 END)
        AS invalid_products,
    SUM(CASE WHEN clean_description IS NULL THEN 1 ELSE 0 END)
        AS missing_descriptions
FROM ikea_medallion.sql_silver.ikea_products
"""))



# COMMAND ----------

# ------------------------------------------------------------
# STEP 3: GOLD LAYER - ANALYTICS AND BUSINESS INSIGHTS
# ------------------------------------------------------------

print("\nSTEP 3: GOLD LAYER")

# 1. High-price products
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.high_price_products
AS
SELECT *
FROM ikea_medallion.sql_silver.ikea_products
WHERE price_numeric > 1000
  AND data_quality_flag = 'VALID'
""")

# 2. Highest-price products
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.highest_price_products
AS
SELECT *
FROM ikea_medallion.sql_silver.ikea_products
WHERE price_numeric = (
    SELECT MAX(price_numeric)
    FROM ikea_medallion.sql_silver.ikea_products
)
""")

# 3. Lowest-price products
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.lowest_price_products
AS
SELECT *
FROM ikea_medallion.sql_silver.ikea_products
WHERE price_numeric = (
    SELECT MIN(price_numeric)
    FROM ikea_medallion.sql_silver.ikea_products
    WHERE price_numeric >= 0
)
""")

# 4. High-rating products
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.high_rating_products
AS
SELECT *
FROM ikea_medallion.sql_silver.ikea_products
WHERE rating_numeric >= 4.5
  AND data_quality_flag = 'VALID'
""")

# 5. Category business insights
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.category_business_insights
AS
SELECT
    category,
    COUNT(*) AS total_products,
    ROUND(AVG(price_numeric), 2) AS average_price_rs,
    MIN(price_numeric) AS minimum_price_rs,
    MAX(price_numeric) AS maximum_price_rs,
    ROUND(AVG(rating_numeric), 2) AS average_rating,
    SUM(CASE WHEN rating_numeric >= 4.5 THEN 1 ELSE 0 END)
        AS high_rating_products,
    SUM(CASE WHEN discount_numeric > 0 THEN 1 ELSE 0 END)
        AS discounted_products
FROM ikea_medallion.sql_silver.ikea_products
WHERE data_quality_flag = 'VALID'
GROUP BY category
""")

# 6. Rating business insights
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.rating_business_insights
AS
SELECT
    rating_category,
    COUNT(*) AS total_products,
    ROUND(AVG(rating_numeric), 2) AS average_rating,
    ROUND(AVG(price_numeric), 2) AS average_price_rs
FROM ikea_medallion.sql_silver.ikea_products
GROUP BY rating_category
""")

# 7. Price business insights
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.price_business_insights
AS
SELECT
    price_band,
    COUNT(*) AS total_products,
    ROUND(AVG(price_numeric), 2) AS average_price_rs,
    ROUND(AVG(rating_numeric), 2) AS average_rating
FROM ikea_medallion.sql_silver.ikea_products
GROUP BY price_band
""")

# 8. Discount business insights
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.discount_business_insights
AS
SELECT
    CASE
        WHEN discount_numeric IS NULL OR discount_numeric <= 0
            THEN 'NO DISCOUNT'
        ELSE 'DISCOUNTED'
    END AS discount_status,
    COUNT(*) AS total_products,
    ROUND(AVG(discount_numeric), 2) AS average_discount_percent,
    MAX(discount_numeric) AS maximum_discount_percent
FROM ikea_medallion.sql_silver.ikea_products
GROUP BY
    CASE
        WHEN discount_numeric IS NULL OR discount_numeric <= 0
            THEN 'NO DISCOUNT'
        ELSE 'DISCOUNTED'
    END
""")

# 9. Badge business insights
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.badge_business_insights
AS
SELECT
    COALESCE(NULLIF(TRIM(CAST(badge AS STRING)), ''), 'NO BADGE')
        AS badge_category,
    COUNT(*) AS total_products,
    ROUND(AVG(price_numeric), 2) AS average_price_rs,
    ROUND(AVG(rating_numeric), 2) AS average_rating
FROM ikea_medallion.sql_silver.ikea_products
GROUP BY
    COALESCE(NULLIF(TRIM(CAST(badge AS STRING)), ''), 'NO BADGE')
""")

# 10. Price-rating insights
spark.sql("""
CREATE OR REPLACE TABLE ikea_medallion.sql_gold.price_rating_insights
AS
SELECT
    price_band,
    rating_category,
    COUNT(*) AS total_products,
    ROUND(AVG(price_numeric), 2) AS average_price_rs,
    ROUND(AVG(rating_numeric), 2) AS average_rating
FROM ikea_medallion.sql_silver.ikea_products
GROUP BY price_band, rating_category
""")

print("Gold tables created.")



# COMMAND ----------

# ------------------------------------------------------------
# FINAL DATA QUALITY CHECKS
# ------------------------------------------------------------

print("\nFINAL DATA QUALITY CHECKS")

display(spark.sql("""
SELECT
    COUNT(*) AS total_products,
    SUM(CASE WHEN data_quality_flag = 'VALID' THEN 1 ELSE 0 END)
        AS valid_products,
    SUM(CASE WHEN data_quality_flag = 'INVALID' THEN 1 ELSE 0 END)
        AS invalid_products,
    SUM(CASE WHEN clean_productID IS NULL THEN 1 ELSE 0 END)
        AS missing_product_ids,
    SUM(CASE WHEN clean_product_name IS NULL THEN 1 ELSE 0 END)
        AS missing_product_names,
    SUM(CASE WHEN price_numeric IS NULL OR price_numeric < 0 THEN 1 ELSE 0 END)
        AS invalid_or_missing_prices,
    SUM(CASE WHEN rating_numeric IS NULL THEN 1 ELSE 0 END)
        AS missing_ratings,
    SUM(CASE WHEN clean_product_url IS NULL THEN 1 ELSE 0 END)
        AS missing_product_urls,
    SUM(CASE WHEN clean_image_urls IS NULL THEN 1 ELSE 0 END)
        AS missing_image_urls,
    SUM(CASE WHEN clean_description IS NULL THEN 1 ELSE 0 END)
        AS missing_descriptions
FROM ikea_medallion.sql_silver.ikea_products
"""))

# Duplicate product ID check
display(spark.sql("""
SELECT
    clean_productID,
    COUNT(*) AS duplicate_count
FROM ikea_medallion.sql_silver.ikea_products
GROUP BY clean_productID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20
"""))


# COMMAND ----------

# ------------------------------------------------------------
# DISPLAY GOLD INSIGHTS
# ------------------------------------------------------------

print("\nGOLD: HIGH-PRICE PRODUCTS")
display(spark.sql("""
SELECT productID, product_name, price_numeric, rating_numeric
FROM ikea_medallion.sql_gold.high_price_products
ORDER BY price_numeric DESC
LIMIT 20
"""))

print("\nGOLD: HIGHEST-PRICE PRODUCTS")
display(spark.sql("""
SELECT productID, product_name, price_numeric
FROM ikea_medallion.sql_gold.highest_price_products
"""))

print("\nGOLD: LOWEST-PRICE PRODUCTS")
display(spark.sql("""
SELECT productID, product_name, price_numeric
FROM ikea_medallion.sql_gold.lowest_price_products
"""))

print("\nGOLD: HIGH-RATING PRODUCTS")
display(spark.sql("""
SELECT productID, product_name, rating_numeric, price_numeric
FROM ikea_medallion.sql_gold.high_rating_products
ORDER BY rating_numeric DESC, rating_count_numeric DESC
LIMIT 20
"""))

print("\nGOLD: CATEGORY BUSINESS INSIGHTS")
display(spark.sql("""
SELECT *
FROM ikea_medallion.sql_gold.category_business_insights
ORDER BY total_products DESC
LIMIT 30
"""))

print("\nGOLD: RATING BUSINESS INSIGHTS")
display(spark.sql("""
SELECT *
FROM ikea_medallion.sql_gold.rating_business_insights
ORDER BY total_products DESC
"""))

print("\nGOLD: PRICE BUSINESS INSIGHTS")
display(spark.sql("""
SELECT *
FROM ikea_medallion.sql_gold.price_business_insights
ORDER BY total_products DESC
"""))

print("\nGOLD: DISCOUNT BUSINESS INSIGHTS")
display(spark.sql("""
SELECT *
FROM ikea_medallion.sql_gold.discount_business_insights
ORDER BY total_products DESC
"""))

print("\nGOLD: BADGE BUSINESS INSIGHTS")
display(spark.sql("""
SELECT *
FROM ikea_medallion.sql_gold.badge_business_insights
ORDER BY total_products DESC
"""))

print("\nGOLD: PRICE-RATING INSIGHTS")
display(spark.sql("""
SELECT *
FROM ikea_medallion.sql_gold.price_rating_insights
ORDER BY total_products DESC
LIMIT 25
"""))



# COMMAND ----------

# ------------------------------------------------------------
# FINAL TABLE COUNTS
# ------------------------------------------------------------

print("\nFINAL TABLE COUNTS")

table_names = [
    "ikea_medallion.sql_bronze.ikea_products",
    "ikea_medallion.sql_silver.ikea_products",
    "ikea_medallion.sql_gold.high_price_products",
    "ikea_medallion.sql_gold.highest_price_products",
    "ikea_medallion.sql_gold.lowest_price_products",
    "ikea_medallion.sql_gold.high_rating_products",
    "ikea_medallion.sql_gold.category_business_insights",
    "ikea_medallion.sql_gold.rating_business_insights",
    "ikea_medallion.sql_gold.price_business_insights",
    "ikea_medallion.sql_gold.discount_business_insights",
    "ikea_medallion.sql_gold.badge_business_insights",
    "ikea_medallion.sql_gold.price_rating_insights",
]

for table_name in table_names:
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count}")

print("\nIKEA SQL MEDALLION ETL PIPELINE COMPLETED")