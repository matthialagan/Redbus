
import psycopg2
from psycopg2 import sql, OperationalError
import pandas as pd 
# Connect to MySQL server
try :
    
    connection = psycopg2.connect (
    dbname = "redbus_data",
    user="postgres",
    password="Guvi8754241299",
    host="localhost",
    port="5432"
)
    print ("connection successful")
except OperationalError as e:
        print(f"The error '{e}' occurred")  

# Create a cursor object
cursor = connection.cursor()
csv_path = r"E:\project_class\govtredbusdata.csv"
df = pd.read_csv(csv_path) 

###checking the columns in table and csv file 

table_name = 'bus_routes'
columns_to_check = ['Route_name', 'Route_link', 'Bus_name', 'Bus_type', 'Departing_time', 'Duration', 'Reaching_time', 'Star_rating', 'Price', 'Seats_available'] 
# List of column names to check 
cursor.execute(f""" SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' """) 


existing_columns = [row[0] for row in cursor.fetchall()] 
for column in columns_to_check:
    if column not in existing_columns:
        print(f"Column '{column}' exists in table '{table_name}'.")
    else:
        print(f"Column '{column}' does not exist in table '{table_name}'.")

# Print the column names
print(df.columns.tolist())

table_name = 'bus_routes'

# Query to get column names
query = f"""
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = '{table_name}';
"""

# Execute the query
cursor.execute(query)

# Fetch all results
column_names = cursor.fetchall()

# Print column names
for column in column_names:
    print(column[0])

        

###inserting the data in the table

with connection.cursor() as cursor:
    # Specify the columns to insert directly
    columns_to_insert = [
    'Route_name', 'Route_link', 'Bus_name', 'Bus_type', 'Departing_time', 'Duration', 'Reaching_time', 'Star_rating', 'Price', 'Seats_available'
]

# Create placeholders for the SQL query
placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns_to_insert))

# Construct the SQL INSERT query
query = sql.SQL('INSERT INTO {table} ({fields}) VALUES ({values})').format(
    table=sql.Identifier('bus_routes'),  # Safe table name identifier
    fields=sql.SQL(', ').join(sql.Identifier(col) for col in columns_to_insert),
    values=placeholders
)

# Prepare data for insertion using direct column references
with connection.cursor() as cursor:
    data_to_insert = [
    tuple(getattr(row,col.replace(' ', '_')) for col in columns_to_insert)
    for index, row in df.iterrows()
]
     
 # Insert data into the table using executemany for batch insertion
cursor.executemany(query, data_to_insert)

# Commit the transaction
connection.commit()

# Close the connection
connection.close()
