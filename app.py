import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from io import BytesIO
from etl.transform import transform_data
from etl.load import load_data, get_mysql_connection
import requests
from urllib.parse import urlparse

st.set_page_config(page_title="Data Pipeline Dashboard", layout="wide")
st.title("📊 Data Pipeline Dashboard with File/Link Upload")

# --- Step 1: Upload or provide link ---
st.header("1️⃣ Upload File or Enter URL")

uploaded_file = st.file_uploader("Upload CSV, Excel or TXT", type=["csv", "xlsx", "txt"])
data = None

url_input = st.text_input("Or enter a link (CSV/Excel)")

if uploaded_file:
    try:
        if uploaded_file.name.endswith(".csv") or uploaded_file.name.endswith(".txt"):
            data = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(".xlsx"):
            data = pd.read_excel(uploaded_file, engine="openpyxl")
        st.success(f"✅ Successfully uploaded `{uploaded_file.name}`")
    except Exception as e:
        st.error(f"Error reading file: {e}")

elif url_input:
    try:
        response = requests.get(url_input)
        response.raise_for_status()
        filename = urlparse(url_input).path.split("/")[-1]
        if filename.endswith(".csv") or filename.endswith(".txt"):
            data = pd.read_csv(BytesIO(response.content))
        elif filename.endswith(".xlsx"):
            data = pd.read_excel(BytesIO(response.content), engine="openpyxl")
        st.success(f"✅ Successfully loaded from link: {url_input}")
    except Exception as e:
        st.error(f"Error fetching data from URL: {e}")

if data is not None:
    st.dataframe(data.head())

    # --- Step 2: Transform ---
    st.header("2️⃣ Transform Data")
    transformed_data = transform_data(data)
    st.dataframe(transformed_data.head())

    # --- Step 3: Load to MySQL ---
    st.header("3️⃣ Load to MySQL")
    conn = get_mysql_connection()
    load_data(transformed_data, conn)
    st.success("✅ Data loaded into MySQL successfully!")

    # --- Step 4: Visualization ---
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

    # --- Step 5: Download report ---
    excel_file = BytesIO()
    transformed_data.to_excel(excel_file, index=False)
    st.download_button(label="⬇️ Download Excel Report", data=excel_file.getvalue(), file_name="report.xlsx")
