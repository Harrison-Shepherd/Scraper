import re
import os
import pandas as pd
import requests
from Utils.sanitize_filename import sanitize_filename  

class League:
    league_info = {}
    
    @classmethod
    def fetch_leagues(cls):
        url = 'http://mc.championdata.com/data/competitions.json'
        response = requests.get(url)
    
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code}")
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            leagues = response.json()
        except ValueError:
            print("Error: Failed to parse JSON response from API.")
            return pd.DataFrame(), pd.DataFrame()
        
        # Check if expected keys exist in the response
        if 'competitionDetails' not in leagues or 'competition' not in leagues['competitionDetails']:
            print("Error: Unexpected response structure. 'competitionDetails' or 'competition' key missing.")
            return pd.DataFrame(), pd.DataFrame()

        # Normalize the competition details into a DataFrame
        leagues_df = pd.json_normalize(leagues['competitionDetails']['competition'])
        
        # Clean the league names by removing the year and sanitizing the file name
        leagues_df['cleaned_name'] = leagues_df['name'].apply(lambda x: sanitize_filename(re.sub(r'\b\d{4}\b', '', x).strip()))
        leagues_df['league_season'] = leagues_df['cleaned_name'] + ' (' + leagues_df['season'].astype(str) + ')'
        
        # Store the league info in a class-level dictionary
        cls.league_info = leagues_df.set_index('id')['league_season'].to_dict()
    
        # Return the full DataFrame and a simplified one with only relevant columns
        return leagues_df, leagues_df[['id', 'league_season', 'season']].drop_duplicates()
    
    @classmethod
    def get_league_name_and_season(cls, league_id):
        # Safely get the league name and season based on league_id, defaulting to 'Unknown League' if not found
        return cls.league_info.get(league_id, 'Unknown League')
