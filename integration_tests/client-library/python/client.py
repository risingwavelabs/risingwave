import psycopg2

class client:
    def __init__(self, host, port,database, user, password):
        self.host = host
        self.port=port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = self.connection.cursor()  # Create a cursor object
            return cursor  # Return the cursor object
        except psycopg2.Error as e:
            print(e)
            return e

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
