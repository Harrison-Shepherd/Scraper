import sys
import os

# Add the parent directory (Scraper 3.0) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
from DatabaseUtils.SqlConnector import connect 



"""
Exports existing player data from the database to a JSON file for testing purposes. 
This might end up being a one-time operation, but it's useful to have the code here for reference.
"""

# Function to fetch player data and save as a JSON file
def export_player_data_to_json():
    try:
        # Establish a connection using your existing connector
        connection = connect()

        if connection:
            # Define the query to fetch all data from the table
            query = "SELECT * FROM powerdata.player_info"

            # Fetch the data using pandas and store it in a DataFrame
            df = pd.read_sql(query, connection)

            # Save the DataFrame as a JSON file
            df.to_json('player_info.json', orient='records', lines=False)  # Comma-separated JSON array
            print("Data successfully exported to 'player_info.json'.")
        
        # Close the connection after the operation
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function to export player data to JSON
if __name__ == "__main__":
    export_player_data_to_json()
