import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from DatabaseUtils.SqlConnector import connect
from mysql.connector import Error

# Function to create the static_player_info table
def create_static_player_info_table():
    try:
        # Connect to the database
        connection = connect()

        if connection:
            cursor = connection.cursor()

            # SQL query to create the new table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS static_player_info (
                playerId BIGINT PRIMARY KEY,
                firstname VARCHAR(255),
                surname VARCHAR(255),
                displayName VARCHAR(255),
                shortDisplayName VARCHAR(255),
                squadName VARCHAR(255),
                squadId INT,
                sportId INT,
                uniqueSquadId VARCHAR(255),
                uniquePlayerId VARCHAR(255)
            );
            """

            # Execute the query
            cursor.execute(create_table_query)
            connection.commit()

            print("Table 'static_player_info' has been successfully created.")
        
        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

    except Error as e:
        print(f"Error occurred while creating the table: {e}")

# Run the function to create the table
if __name__ == "__main__":
    create_static_player_info_table()
