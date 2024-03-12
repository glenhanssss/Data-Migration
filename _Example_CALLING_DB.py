# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Calling database from SQL Server
from _DB_CONNECTOR import create_connection_sql_server, read_data_sql_server 

# create connection variable
connection_sql_server = create_connection_sql_server()

# read data
query_sql_server = "SELECT * FROM ____"
df_sql_server = read_data_sql_server(connection_sql_server, query_sql_server)

# close connection
connection_sql_server.close()

# show dataframe
print(df_sql_server)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Calling database from mongoDB
from _DB_CONNECTOR import create_connection_mongo_db, get_collection_mongo_db, execute_query_mongo_db
import pandas as pd
from datetime import datetime

# Using the create_mongo_connection function without providing a value for the variable
mongo_client = create_connection_mongo_db()

# Get a collection from the MongoDB database
default_database_name = "____"
collection_name = "____"
mongo_collection = get_collection_mongo_db(mongo_client, default_database_name, collection_name)

# condition query (like WHERE in SQL)
query = {
    "requestType": {"$in": ["overtime","assignment"]}, # like sql.. where the requestType is overtime and assignment
    "status": "FINISHED", # Only take status that are FINISHED
    "requestDate": {"$gte": datetime(2022, 1, 1)} # take a requestDate that is later than January 1, 2022
}

# column display
projection = {
    # you can fill this for column that you want to display, if its blank, you will call all a column in the dataframe
}

# Get query results
result_data = execute_query_mongo_db(mongo_collection, query, projection)

# Convert query results into DataFrame
df_mongo_db = pd.DataFrame(result_data)

# close connection
mongo_client.close()

# show DataFrame
print(df_mongo_db)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  

# Calling database from SQL server 1
from _DB_CONNECTOR import create_connection_sql_server_1, read_data_sql_server_1

# create connection variable
connection_sql_server_1 = create_connection_sql_server_1()

# read data
query_sql_server_1 = "SELECT * FROM ____"
df_sql_server_1 = read_data_sql_server_1(connection_sql_server_1, query_sql_server_1)

# close connection
connection_sql_server_1.close()

# show dataframe
print(df_sql_server_1)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# example UPDATE table using get_database_sql_server_1_cursor function
from _DB_CONNECTOR import get_database_sql_server_1_cursor

# Get the cursor from the db_connector module
cursor = get_database_sql_server_1_cursor()

# Customize your queries
query_column1 = "UPDATE table1 SET column1 = 0x WHERE column1 IS NULL"
query_column2 = "UPDATE table1 SET column2 = 0x WHERE column2 IS NULL"

# Execute the queries
cursor.execute(query_column1)
cursor.execute(query_column2)

# Commit the changes
cursor.connection.commit()

# Close the connection
cursor.connection.close()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Calling SQLserver.scheme1 database
from _DB_CONNECTOR import create_connection_sql_server_scheme1, read_data_sql_server_scheme1 

# create connection variable
connection_sql_server_scheme1 = create_connection_sql_server_scheme1()

# read data
query_sql_server_scheme1 = "SELECT * FROM ____"
df = read_data_sql_server_scheme1(connection_sql_server_scheme1, query_sql_server_scheme1)

# close connection
connection_sql_server_scheme1.close()

# show dataframe
print(df)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
