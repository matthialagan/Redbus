import streamlit as st
import psycopg2
from psycopg2 import OperationalError 
import pandas as pd 


# Connect to MySQL server

try :
    
    connection = psycopg2.connect(
    dbname = "redbusdata",
    user="postgres",
    password="Guvi8754241299",
    host="localhost",
    port="5432"
)
    print ("connection successful")
except OperationalError as e:
        print(f"The error '{e}' occurred")
# Create a cursor object
mycursor = connection.cursor()

# SQL query
sql_query = "SELECT * FROM redbusroutes"
# Execute SQL query
mycursor.execute(sql_query)
print()

mycursor.execute("SELECT Route_name, Route_link FROM rebusroutes")
routes = mycursor.fetchall()

# Create a DataFrame from the fetched routes
routes_df = pd.DataFrame(routes, columns=["Route_name", "Route_link"])

# Streamlit form to select a route
with st.form("route_form"):
    selected_route = st.selectbox("Select Route", routes_df["Route_name"].unique())
    route_link = routes_df[routes_df["Route_name"] == selected_route]["Route_link"].values[0]
    submitted_route = st.form_submit_button("Submit")

if submitted_route:
    st.session_state['selected_route'] = selected_route
    st.session_state['route_link'] = route_link

