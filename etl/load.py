import pandas as pd
import mysql.connector
import re

def sanitize_column(col_name: str) -> str:
    """
    Make column names safe for MySQL:
    - Replace spaces with underscores
    - Remove brackets, special chars
    - Ensure it doesn't start with a number
    """
    col = col_name.strip()
    col = re.sub(r"[^\w]", "_", col)  # Keep only letters, numbers, underscore
    if re.match(r"^\d", col):
        col = f"col_{col}"  # prepend if starts with number
    return col
def get_mysql_connection():
    conn = mysql.connector.connect(
        host="mysql-db",
        user="root",
        password="rootpassword",
        database="data_pipeline"
    )
    return conn
def load_data(df: pd.DataFrame, conn):
    cursor = conn.cursor()
    table_name = "gdp_data"
    safe_columns = [sanitize_column(c) for c in df.columns]
    df.columns = safe_columns
    cols = ", ".join([f"`{col}` TEXT" for col in safe_columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols})")
    conn.commit()
    for _, row in df.iterrows():
        sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{c}`' for c in safe_columns])}) VALUES ({', '.join(['%s']*len(row))})"
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.close()
