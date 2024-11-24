
import pandas as pd
import psycopg2
from psycopg2 import Error

# Connect to 
connnection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Guvi8754241299"
)
# Create a cursor object

mycursor = connnection.cursor ()
connnection.autocommit = True
cur = connnection.cursor()

# Create a new database if it doesn't exist
database_name = "redbus_data"
cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
cur.execute(f"CREATE DATABASE {database_name}")
print(f"Database '{database_name}' has been overwritten.")

# creating table    

def create_table():
    try:
        # Establish the connection
        connection = psycopg2.connect(
            dbname="redbus_data",
            user="postgres",
            password="Guvi8754241299",
            host="localhost",
            port="5432"
        )
        
        # Create a cursor object using the connection
        with connection.cursor() as cur:
            # Define the create table query
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS bus_routes (
                "Route_id" SERIAL PRIMARY KEY,
                "Route_name" TEXT,
                "Route_link" TEXT,
                "Bus_name" TEXT,
                "Bus_type" TEXT,
                "Departing_time" TIME,
                "Duration" TEXT,
                "Reaching_time" TIME,
                "Star_rating" FLOAT,
                "Price" DECIMAL(10, 2), 
                "Seats_available" TEXT
            )'''
            
            # Execute the create table query
            cur.execute(create_table_query)
            connection.commit()
            print("Table created successfully")

    except (Exception, Error) as e:
        print(f"Error: {e}")

    finally:
        connection.close()  # Close the connection here

# Call the create_table function to create the table
create_table()

