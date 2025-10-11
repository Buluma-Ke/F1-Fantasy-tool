import pandas as pd
import numpy as np
from sqlalchemy import create_engine    # pip3 install  sqlalchemy ,   pip3 install  psycopg2
from sqlalchemy.exc import SQLAlchemyError




class PostgresLoader:
    def __init__(self):
        self.user = None
        self.password = None
        self.host = None
        self.port = None
        self.dbname = None
        self.table_name = None
        self.engine = None

    def  get_connection_details(self):
        self.user = input("Enter PostgreSQL username: ")
        self.password = input("Enter Password: ")
        self.host = input("Enter PostgreSQL host (default 'localhost'): ") or "localhost"
        self.port = input("Enter PostgreSQL port (default '5432')") or "5432"
        self.dbname = input("Enter database name: ")
        self.table_name = input("Enter the target table name: ")

    def create_engine(self):
        try:
            conn_str = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}'
            self.engine = create_engine(conn_str)
            # Try to connect to test the connection
            with self.engine.connect() as conn:
                #conn.execute("SELECT 1")
                print(f"Connected to database '{self.dbname}' on {self.host}:{self.port} as user '{self.user}'.")
        except SQLAlchemyError as e:
            print("Failed to connect to the database.")
            print("Error:", e)
            self.engine = None

    def insert_dataframe(self, df: pd.DataFrame):

        """Transform the column to lower case
            and fill the spaces with '_' for
            easier querying"""
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(" ", '_')

        if self.engine is None:
            raise Exception("Engine not created. Call create_engine() first.")
        print(f"Inserting DataFrame into table '{self.table_name}'...")
        df.to_sql(self.table_name, self.engine, if_exists='append', index=False)
        print("Insert completed.")




if __name__ == "__main__":
    loader = PostgresLoader()
    loader.get_connection_details()
    loader.create_engine()




# You can load your own DataFrame here
# Example: load a sample CSV for testing

# try:
#     df = pd.read_csv("ncr_ride_bookings.csv")  # Replace with actual file
#     loader.insert_dataframe(df)
# except FileNotFoundError:
#     print("CSV file not found. Make sure 'your_data.csv' exists in the same folder.")



# Example usage(import):
# inserter = PostgresLoader()
# inserter.get_connection_details()
# inserter.create_engine()
# df = pd.read_csv('your_data.csv')  # or however you get your DataFrame
# inserter.insert_dataframe(df)
