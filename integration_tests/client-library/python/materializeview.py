import psycopg2
from client import client
from crud import crud

class MaterializeView:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.port=port

    def create_mv(self):
        crud_ins = crud(self.host, self.port, self.database, self.user, self.password)
        crud_ins.create_table()
        crud_ins.insert_data("John",25,10000)
        crud_ins.insert_data("Shaun",25,11000)
        crud_ins.insert_data("Caul",25,14000)
        crud_ins.insert_data("Mantis",28,18000)
        crud_ins.insert_data("Tony",28,19000)
        mv_query="""
                    CREATE MATERIALIZED VIEW average_salary_view_py AS
                    SELECT age, AVG(salary) AS average_salary
                    FROM sample_table_py
                    GROUP BY age;
                """
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(mv_query)
            databaseconnection.connection.commit()
            print("MV created successfully.")
        except psycopg2.Error as e:
            print("MV creation failed: ", str(e))

    def drop_mv(self):
        mv_drop_query = "DROP materialized view average_salary_view_py;"
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(mv_drop_query)
            databaseconnection.connection.commit()
            print("MV dropped successfully.")
        except psycopg2.Error as e:
            print("MV drop failed: ", str(e))
