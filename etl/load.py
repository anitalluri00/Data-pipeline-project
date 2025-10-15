import re
import mysql.connector
import pandas as pd

def sanitize_column(col_name: str) -> str:
    col = col_name.strip()
    col = re.sub(r"[^\w]", "_", col)
    col = col.lower()
    if len(col) > 60:  # limit to avoid MySQL 64-char max
        col = col[:60]
    if re.match(r"^\d", col):
        col = f"col_{col}"
    return col
def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql-db",      # or 'localhost' if local
        user="root",
        password="rootpassword",
        database="data_pipeline"
    )
def load_data(df: pd.DataFrame, conn):
    cursor = conn.cursor()

    # Generate a unique table name per upload
    table_name = "uploaded_data"

    # Sanitize and truncate long column names
    safe_columns = [sanitize_column(c) for c in df.columns]
    df.columns = safe_columns

    # Drop and recreate the table to match schema exactly
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    cols = ", ".join([f"`{col}` TEXT" for col in safe_columns])
    cursor.execute(f"CREATE TABLE `{table_name}` ({cols})")

    # Insert data
    for _, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join([f"`{c}`" for c in safe_columns])
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.close()
