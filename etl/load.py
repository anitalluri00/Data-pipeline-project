import mysql.connector
import pandas as pd
import re

def sanitize_column_name(col):
    """Sanitize column name to be MySQL-compatible (max 64 chars)"""
    col = re.sub(r'\W+', '_', col)  # replace non-alphanumeric with _
    return col[:64]  # truncate to 64 chars

def load_data(df, conn, table_name="my_table"):
    cursor = conn.cursor()
    
    # Sanitize column names
    df.columns = [sanitize_column_name(c) for c in df.columns]

    # Build CREATE TABLE statement
    cols = ', '.join([f"`{c}` TEXT" for c in df.columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols})"
    cursor.execute(create_table_sql)

    # Build INSERT statement
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO `{table_name}` ({', '.join('`'+c+'`' for c in df.columns)}) VALUES ({placeholders})"

    # Insert rows
    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_sql, row)

    conn.commit()
    cursor.close()
    print(f"Loaded {len(df)} rows into `{table_name}`")

if __name__ == "__main__":
    # Example usage
    try:
        # Use encoding and error handling to prevent CSV parsing issues
        df = pd.read_csv("your_file.csv", encoding='utf-8', on_bad_lines='skip')
        
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="yourpassword",
            database="yourdb"
        )

        load_data(df, conn, table_name="my_table")
        conn.close()
    except Exception as e:
        print(f"Error loading data: {e}")
