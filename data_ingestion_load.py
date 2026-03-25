import pyodbc

DRIVER_PATH = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME = 'project'
SCHEMA_NAME = "ingestion"
USER = 'postgres'  
PASSWORD = '563634851' 
PORT = '1234'


conn_str = (
    f"DRIVER={{{DRIVER_PATH}}};"
    f"SERVER=localhost;"
    f"DATABASE={DB_NAME};"
    f"UID={USER};"
    f"PWD={PASSWORD};"
    f"PORT={PORT};"
)

def load_data():
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()

    data_map = {
        "users_data": "/tmp/datasets/users_data.csv",
        "cards_data": "/tmp/datasets/cards_data.csv",
        "mcc_data": "/tmp/datasets/mcc_data.csv",
        "transactions_data": "/tmp/datasets/transactions_data.csv"
    }

    for table, path in data_map.items():
        try:
            sql = f"""
                COPY {SCHEMA_NAME}.{table}
                FROM '{path}'
                WITH (FORMAT CSV, HEADER true, DELIMITER ',');
            """
            cursor.execute(sql)
            
            cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table}")
            row_count = cursor.fetchone()[0]
            
            if row_count > 0:
                print(f"{table} save successfully， {row_count} rows")
            else:
                print(f"empty please check again")

        except Exception as e:
            print(f"{table} fail leading: {e}")

    conn.close()
    print(" All data load attempts finished!")


if __name__ == "__main__":
    load_data()
