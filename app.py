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
st.header("1️⃣ Data Extraction")
data = extract_data()
st.dataframe(data.head())
st.success("✅ Data extracted successfully!")
st.header("2️⃣ Data Transformation")
transformed_data = transform_data(data)
st.dataframe(transformed_data.head())
st.success("✅ Data transformed successfully!")
st.header("3️⃣ Load to MySQL")
conn = get_mysql_connection()
load_data(transformed_data, conn)
st.success("✅ Data loaded into MySQL successfully!")
st.header("4️⃣ Automated Reporting")
fig, ax = plt.subplots()
sns.histplot(transformed_data.select_dtypes(include=np.number).iloc[:,0], kde=True, ax=ax)
st.pyplot(fig)
fig2 = px.histogram(transformed_data, x=transformed_data.select_dtypes(include=np.number).columns[0])
st.plotly_chart(fig2)
excel_file = BytesIO()
transformed_data.to_excel(excel_file, index=False)
st.download_button(label="Download Excel", data=excel_file.getvalue(), file_name="report.xlsx")
st.success("✅ Report generated successfully!")