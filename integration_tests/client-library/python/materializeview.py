import psycopg2
from client import Client
from crud import SampleTableCrud


# Represents the materialized view `average_salary_view_py`.
class MaterializeView:
    def __init__(self, host, port, database, user, password):
        self.client = Client(host, port, database, user, password)
        self.crud = SampleTableCrud(host, port, database, user, password)

    def create_mv(self):
        self.crud.create_table()
        self.crud.insert_data("John", 25, 10000)
        self.crud.insert_data("Shaun", 25, 11000)
        self.crud.insert_data("Caul", 25, 14000)
        self.crud.insert_data("Mantis", 28, 18000)
        self.crud.insert_data("Tony", 28, 19000)
        mv_query = """
                    CREATE MATERIALIZED VIEW average_salary_view_py AS
                    SELECT age, AVG(salary) AS average_salary
                    FROM sample_table_py
                    GROUP BY age;
                """
        try:
            cursor = self.client.connect()
            cursor.execute(mv_query)
            self.client.connection.commit()
            print("MV created successfully.")
        except psycopg2.Error as e:
            print("MV creation failed: ", str(e))

    def drop_mv(self):
        mv_drop_query = "DROP materialized view average_salary_view_py;"
        try:
            cursor = self.client.connect()
            cursor.execute(mv_drop_query)
            self.client.connection.commit()
            print("MV dropped successfully.")
        except psycopg2.Error as e:
            print("MV drop failed: ", str(e))
