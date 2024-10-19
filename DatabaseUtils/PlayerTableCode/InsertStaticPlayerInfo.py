import sys
import os

# Add the parent directory (Scraper 3.0) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import json
from DatabaseUtils.SqlConnector import connect
from mysql.connector import Error

# Correct path to the player_info.json file
JSON_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'Assets', 'Jsons', 'player_info.json'))

# Function to insert data from NDJSON into the static_player_info table
def insert_data_from_json_into_static_player_info():
    try:
        # Connect to the database
        connection = connect()

        if connection:
            cursor = connection.cursor()

            # SQL query to insert data
            insert_query = """
            INSERT INTO static_player_info (playerId, firstname, surname, displayName, shortDisplayName, squadName, squadId, sportId, uniqueSquadId, uniquePlayerId)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                firstname=VALUES(firstname), 
                surname=VALUES(surname), 
                displayName=VALUES(displayName),
                shortDisplayName=VALUES(shortDisplayName),
                squadName=VALUES(squadName),
                squadId=VALUES(squadId),
                sportId=VALUES(sportId),
                uniqueSquadId=VALUES(uniqueSquadId),
                uniquePlayerId=VALUES(uniquePlayerId);
            """

            # Load the JSON data from the file (NDJSON format, read line by line)
            with open(JSON_FILE_PATH, 'r') as file:
                for line in file:
                    player = json.loads(line.strip())  # Read each line as a JSON object
                    
                    # Prepare the data tuple
                    data = (
                        player.get('playerId'),
                        player.get('firstname'),
                        player.get('surname'),
                        player.get('displayName'),
                        player.get('shortDisplayName'),
                        player.get('squadName'),
                        player.get('squadId'),
                        player.get('sportId'),
                        player.get('uniqueSquadId'),
                        player.get('uniquePlayerId')
                    )
                    
                    # Execute the insert query for each player
                    cursor.execute(insert_query, data)

            # Commit the transaction
            connection.commit()
            print("Data has been successfully inserted into 'static_player_info'.")

        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

    except Error as e:
        print(f"Error occurred: {e}")
    except json.JSONDecodeError as je:
        print(f"Error decoding JSON: {je}")

# Run the function to insert data from JSON into the static_player_info table
if __name__ == "__main__":
    insert_data_from_json_into_static_player_info()
