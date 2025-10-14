import pandas as pd
import mysql.connector

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
    cols = ", ".join([f"{col} TEXT" for col in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})")
    conn.commit()
    for i,row in df.iterrows():
        sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(row))})"
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.close()