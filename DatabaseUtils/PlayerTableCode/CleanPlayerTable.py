import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from DatabaseUtils.SqlConnector import connect
from mysql.connector import Error

# Function to clean (truncate) the static_player_info table
def clean_static_player_info_table():
    try:
        # Connect to the database
        connection = connect()

        if connection:
            cursor = connection.cursor()

            # SQL query to truncate the static_player_info table
            query = "TRUNCATE TABLE static_player_info"

            # Execute the query
            cursor.execute(query)
            connection.commit()

            print("Table 'static_player_info' has been successfully cleaned.")
        
        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

    except Error as e:
        print(f"Error occurred: {e}")

# Run the function to clean the table
if __name__ == "__main__":
    clean_static_player_info_table()
