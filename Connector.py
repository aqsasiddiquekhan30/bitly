from sqlalchemy import create_engine

# class to connect to database

class MySQLConnector:
    def __init__(self, host, port=3306, user=None, password=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.engine = None

    def create_connection(self):
        try:
            connection_string = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
            self.engine = create_engine(connection_string)
            print("Connection to the MySQL database was successful.")
            return self.engine
        except Exception as e:
            raise Exception(f'Error occurred connecting to database: {e}')

    def close_connection(self):
        """Close the database connection if it exists."""
        if self.engine:
            self.engine.dispose()
            print("MySQL database connection closed.")
        else:
            print("No active MySQL database connection to close.")
