import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import requests
from io import BytesIO
from urllib.parse import urlparse
from etl.transform import transform_data
from etl.load import load_data, get_mysql_connection

st.set_page_config(page_title="Data Collector", layout="wide")
st.title("📦 Unified Data Collector (Upload / URL / Process / Store)")
st.markdown("Upload any **CSV / Excel / TXT** or provide a **link**, and this app will clean, process, and store it in MySQL.")
uploaded_file = st.file_uploader("📁 Upload File", type=["csv", "xlsx", "txt"])
url_input = st.text_input("🔗 Or enter a link (direct to CSV/Excel)")
data = None
try:
    if uploaded_file:
        if uploaded_file.name.endswith(".csv") or uploaded_file.name.endswith(".txt"):
            data = pd.read_csv(uploaded_file, on_bad_lines="skip")
        elif uploaded_file.name.endswith(".xlsx"):
            data = pd.read_excel(uploaded_file, engine="openpyxl")
        st.success(f"✅ Successfully uploaded `{uploaded_file.name}`")
    elif url_input:
        response = requests.get(url_input)
        response.raise_for_status()
        filename = urlparse(url_input).path.split("/")[-1]
        if filename.endswith(".csv") or filename.endswith(".txt"):
            data = pd.read_csv(BytesIO(response.content), on_bad_lines="skip")
        elif filename.endswith(".xlsx"):
            data = pd.read_excel(BytesIO(response.content), engine="openpyxl")
        st.success(f"✅ Successfully loaded data from URL")
except Exception as e:
    st.error(f"Error reading data: {e}")
if data is not None:
    st.dataframe(data.head())
    st.header("2️⃣ Transform Data")
    transformed_data = transform_data(data)
    st.dataframe(transformed_data.head())
    st.header("3️⃣ Load into MySQL")
    try:
        conn = get_mysql_connection()
        load_data(transformed_data, conn)
        st.success("✅ Data successfully loaded into MySQL!")
    except Exception as e:
        st.error(f"MySQL Error: {e}")
    st.header("4️⃣ Quick Data Insights")
    num_cols = transformed_data.select_dtypes(include=np.number).columns
    if len(num_cols) > 0:
        fig, ax = plt.subplots()
        sns.histplot(transformed_data[num_cols[0]], kde=True, ax=ax)
        st.pyplot(fig)
        st.plotly_chart(px.histogram(transformed_data, x=num_cols[0]))
    else:
        st.warning("No numeric columns available for plotting.")
    st.header("5️⃣ Download Processed Data")
    excel_file = BytesIO()
    transformed_data.to_excel(excel_file, index=False)
    st.download_button(
        label="⬇️ Download Excel Report",
        data=excel_file.getvalue(),
        file_name="processed_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
