import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data, get_mysql_connection
from io import BytesIO

st.set_page_config(page_title="Data Pipeline Dashboard", layout="wide")
st.title("📊 Complete Data Pipeline Dashboard")

# --- Step 1: Extract ---
st.header("1️⃣ Extract Data")
try:
    data = extract_data()
    st.dataframe(data.head())
except Exception as e:
    st.error(f"Data extraction failed: {e}")
    st.stop()

# --- Step 2: Transform ---
st.header("2️⃣ Transform Data")
transformed_data = transform_data(data)
st.dataframe(transformed_data.head())

# --- Step 3: Load ---
st.header("3️⃣ Load to MySQL")
conn = get_mysql_connection()
load_data(transformed_data, conn)
st.success("✅ Data loaded into MySQL successfully!")

# --- Step 4: Automated Reporting ---
st.header("4️⃣ Automated Reporting")

numeric_cols = transformed_data.select_dtypes(include=np.number).columns
if len(numeric_cols) > 0:
    fig, ax = plt.subplots()
    sns.histplot(transformed_data[numeric_cols[0]], kde=True, ax=ax)
    st.pyplot(fig)

    fig2 = px.histogram(transformed_data, x=numeric_cols[0])
    st.plotly_chart(fig2)
else:
    st.warning("No numeric columns available for plotting.")

# --- Step 5: Export Report ---
excel_file = BytesIO()
transformed_data.to_excel(excel_file, index=False)
st.download_button(label="⬇️ Download Excel Report", data=excel_file.getvalue(), file_name="report.xlsx")
