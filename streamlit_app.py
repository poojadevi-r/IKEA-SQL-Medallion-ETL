
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(
    page_title="IKEA Medallion ETL Explorer",
    page_icon="🛋️",
    layout="wide"
)

st.title("IKEA SQL Medallion ETL Explorer")
st.caption("An interactive overview of an IKEA product data pipeline")

st.markdown("""
This dashboard provides an overview of the Bronze, Silver and Gold
layers of a Medallion Architecture pipeline.
""")

# Pipeline architecture
st.subheader("Medallion Architecture")

col1, col2, col3 = st.columns(3)

with col1:
    st.info("🥉 BRONZE")
    st.write("Raw data ingestion")
    st.write("Source data preservation")

with col2:
    st.info("🥈 SILVER")
    st.write("Data cleaning")
    st.write("Transformation and validation")

with col3:
    st.info("🥇 GOLD")
    st.write("Analytics-ready data")
    st.write("Business-level outputs")

st.divider()

# Project information
st.subheader("Project Overview")

a, b, c = st.columns(3)
a.metric("Architecture", "Medallion")
b.metric("Processing", "ETL")
c.metric("Platform", "Databricks SQL")

st.divider()

# Dataset explorer
st.subheader("Dataset Explorer")

uploaded_file = st.file_uploader(
    "Upload a CSV dataset to explore",
    type=["csv"]
)

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    st.success(f"Dataset loaded: {len(df):,} rows")

    x, y, z = st.columns(3)
    x.metric("Rows", f"{len(df):,}")
    y.metric("Columns", len(df.columns))
    z.metric("Missing values", int(df.isna().sum().sum()))

    st.dataframe(df.head(100), use_container_width=True)

    st.subheader("Data Quality Summary")
    quality = pd.DataFrame({
        "Column": df.columns,
        "Missing Values": df.isna().sum().values,
        "Unique Values": df.nunique().values
    })
    st.dataframe(quality, use_container_width=True)

    numeric_columns = df.select_dtypes(include="number").columns.tolist()

    if numeric_columns:
        st.subheader("Numeric Data Analysis")
        selected_column = st.selectbox(
            "Choose a numeric column",
            numeric_columns
        )
        st.bar_chart(df[selected_column].value_counts().head(20))

else:
    st.info("Upload a CSV file to explore dataset records and data quality.")

st.divider()

# Architecture documentation
st.subheader("Project Architecture")

architecture_image = Path("architecture.png")

if architecture_image.exists():
    st.image(str(architecture_image), use_container_width=True)
else:
    st.caption("Architecture image is not available in the repository.")

st.markdown("""
---
**Project:** IKEA SQL Medallion ETL  
**Technologies:** Databricks SQL, Apache Airflow, Python, Medallion Architecture  
""")
