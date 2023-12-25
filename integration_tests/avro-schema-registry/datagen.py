import psycopg2
import sys

# PostgreSQL Connection Details
DB_NAME = 'dev'
DB_USER = 'root'
DB_HOST = 'localhost'
DB_PORT = '4566'


def execute_sql_script(script_path):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    try:
        with open(script_path, 'r') as sql_file:
            cursor.execute(sql_file.read())
            conn.commit()
            print("Script executed successfully!")
    except Exception as e:
        conn.rollback()
        print(f"Failed to execute the script: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    sql_script_path = sys.argv[1]
    execute_sql_script(sql_script_path)
