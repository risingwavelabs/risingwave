import psycopg2
from client import Client


class SampleTableCrud:
    # Represents the table `sample_table_py`.

    def __init__(self, host, port, database, user, password):
        self.client = Client(host, port, database, user, password)

    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS sample_table_py (
                name VARCHAR,
                age INTEGER,
                salary BIGINT
            );
        """
        try:
            cursor = self.client.connect()
            cursor.execute(create_table_query)
            self.client.connection.commit()
            print("Table created successfully.")
        except psycopg2.Error as e:
            print("Table creation failed: ", str(e))

    def insert_data(self, name, age, salary):
        insert_data_query = """
            INSERT INTO sample_table_py (name, age, salary)
            VALUES (%s, %s,%s);
        """
        try:
            cursor = self.client.connect()
            cursor.execute(insert_data_query, (name, age, salary))
            self.client.connection.commit()
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
            cursor = self.client.connect()
            cursor.execute(update_data_query, (salary, name))
            self.client.connection.commit()
            print("Data updated successfully.")
        except psycopg2.Error as e:
            print("Data updation failed: ", str(e))

    def delete_data(self, name):
        insert_data_query = """
            DELETE FROM sample_table_py WHERE name='%s';
        """
        try:
            cursor = self.client.connect()
            cursor.execute(insert_data_query, (name,))
            self.client.connection.commit()
            print("Data deletion successfully.")
        except psycopg2.Error as e:
            print("Data deletion failed: ", str(e))

    def table_drop(self):
        reset_query = """
            DROP TABLE sample_table_py;
        """
        try:
            cursor = self.client.connect()
            cursor.execute(reset_query)
            self.client.connection.commit()
            print("Table Dropped successfully")
        except psycopg2.Error as e:
            print("Table Drop Failed: ", str(e))
