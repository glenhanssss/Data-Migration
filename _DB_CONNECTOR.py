import pandas as pd
import pyodbc                           # for SQL Server DB (just reading data)
from sqlalchemy import create_engine    # for SQL Server DB (create engine)
from six.moves import urllib            # for SQL Server DB (create engine)
import pymongo                          # for mongoDB (just reading data)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# create connection function SQL Server DB
def create_connection_sql_server():
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=____;'
        'DATABASE=____;'
        'UID=' + '____' + ';'
        'PWD=' + '____' + ''
    )
    return connection

# # create execute function (ex for insert into) for SQL Server DB
# def execute_query_sql_server(connection, query):
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#         connection.commit()
#     except Exception as e:
#         print(f"Error executing query: {e}")

# create read data function to read table for SQL Server DB
def read_data_sql_server(connection, query):
    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error reading data: {e}")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# make a variable for mongoDB connection
default_username = "____"
default_password = "____"
default_host = "____"
default_port = "____"
default_database_name = "____"

# create connection function for mongoDB
def create_connection_mongo_db(
    username = default_username,
    password = default_password,
    host = default_host,
    port = default_port,
    database_name = default_database_name
):
    connection_string = f"mongodb://{username}:{password}@{host}:{port}/{database_name}"
    return pymongo.MongoClient(connection_string)

# create get function for mongoDB
def get_collection_mongo_db(client, database_name, collection_name):
    db = client[database_name]
    return db[collection_name]

def execute_query_mongo_db(collection, query, projection=None):
    cursor = collection.find(query, projection)
    data = list(cursor)
    return data

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# create connection function for SQL Server DB including ENGINE for migrating Data
def create_connection_sql_server_1():
    params = urllib.parse.quote_plus(
        'DRIVER=' + '{ODBC Driver 17 for SQL Server}' + ';'
        'SERVER=' + '____' + ';'
        'DATABASE=' + '____' + ';'
        'UID=' + '____' + ';'
        'PWD=' + '____' + ''
    )
    connection = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params) 
    connection = connection.connect()
    return connection

# # create execute function (ex for insert into) for SQL Server DB
# def execute_query_sql_server_1(connection, query):
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#         connection.commit()
#     except Exception as e:
#         print(f"Error executing query: {e}")

# create read data function to read table for SQL Server DB
def read_data_sql_server_1(connection, query):
    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error reading data: {e}")
        
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# create function for updating table in SQL Server DB 1
def get_database_sql_server_1_cursor():
    connection_string = (
        'DRIVER=' + '{ODBC Driver 17 for SQL Server}' + ';'
        'SERVER=' + '____' + ';'
        'DATABASE=' + '____' + ';'
        'UID=' + '____' + ';'
        'PWD=' + '____' + ''
    )
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    return cursor

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# create connection function for SQL Server DB scheme 1
def create_connection_sql_server_scheme1():
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=____;'
        'DATABASE=____;'
        'UID=' + '____' + ';'
        'PWD=' + '____' + ''
    )
    return connection

# # create execute function (ex for insert into) for SQL Server DB scheme 1
# def execute_query_sql_server_scheme1(connection, query):
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#         connection.commit()
#     except Exception as e:
#         print(f"Error executing query: {e}")

# create read data function to read table for SQL Server DB scheme 1
def read_data_sql_server_scheme1(connection, query):
    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error reading data: {e}")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 