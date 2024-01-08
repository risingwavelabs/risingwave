import psycopg2
from client import client

class crud:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.port=port

    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS sample_table_py (
                name VARCHAR,
                age INTEGER,
                salary BIGINT
            );
        """
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(create_table_query)
            databaseconnection.connection.commit()
            print("Table created successfully.")
        except psycopg2.Error as e:
            print("Table creation failed: ", str(e))

    def insert_data(self, name, age, salary):
        insert_data_query = """
            INSERT INTO sample_table_py (name, age, salary)
            VALUES (%s, %s,%s);
        """
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(insert_data_query, (name, age, salary))
            databaseconnection.connection.commit()
            print("Data inserted successfully.")
        except psycopg2.Error as e:
            print("Data insertion failed: ", str(e))

    def update_data(self, name, salary):
        update_data_query = """
            UPDATE sample_table_py
            SET salary=%s
            WHERE name=%s;
        """
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(update_data_query, (salary, name))
            databaseconnection.connection.commit()
            print("Data updated successfully.")
        except psycopg2.Error as e:
            print("Data updation failed: ", str(e))

    def delete_data(self, name):
        insert_data_query = """
            DELETE FROM sample_table_py WHERE name='%s';
        """
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(insert_data_query, (name,))
            databaseconnection.connection.commit()
            print("Data deletion successfully.")
        except psycopg2.Error as e:
            print("Data deletion failed: ", str(e))

    def table_drop(self):
        reset_query = """
            DROP TABLE sample_table_py;
        """
        try:
            databaseconnection = client(self.host, self.port,self.database, self.user, self.password)
            cursor=databaseconnection.connect()
            cursor.execute(reset_query)
            databaseconnection.connection.commit()
            print("Table Dropped successfully")
        except psycopg2.Error as e:
            print("Table Drop Failed: ", str(e))

crud_ins=crud(host="risingwave-standalone",
        port="4566",
        database="dev",
        user="root",
        password="")
crud_ins.create_table()
