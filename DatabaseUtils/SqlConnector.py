import mysql.connector
from mysql.connector import Error


# Function to establish a connection to the MySQL database
def connect():
    """Establish and return a connection to the MySQL database using mysql-connector."""
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',  
            port=3306,  
            user='root',  
            password='powerdata',  
            database='powerdata'  
        )
        if connection.is_connected():
            print("Successfully connected to the MySQL database 'powerdata'.")
            return connection
    except Error as e:
        print(f"Error connecting to the MySQL database: {e}")
        return None


# Function to select a database
def execute_query_from_file(connection, sql_file_path, parameters=None):
    """Executes a query from a given SQL file with optional parameters."""
    try:
        # Read the query from the .sql file
        with open(sql_file_path, 'r') as file:
            query = file.read()
        
        cursor = connection.cursor()
        if parameters:
            cursor.execute(query, parameters)  # If parameters are provided, use them in the query
        else:
            cursor.execute(query)  # Execute the query without parameters if not provided
        connection.commit()  # Commit the transaction
        cursor.close()
        print(f"Query executed successfully from {sql_file_path}")
    except Error as e:
        print(f"Error executing query from {sql_file_path}: {e}")
