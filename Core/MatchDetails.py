import requests
import pandas as pd
import numpy as np
import logging
from Utils.sanitize_filename import sanitize_filename
from Core.LeaguesList import League

class Match:
    def __init__(self, league_id, match_id, fixture_id, sport_id, fixture_year):
        self.league_id = league_id
        self.match_id = match_id
        self.fixture_id = fixture_id
        self.sport_id = sport_id
        self.data = pd.DataFrame()
        self.fixture_year = fixture_year

    def fetch_data(self):
        league_name_and_season = League.get_league_name_and_season(self.league_id)
        league_name_and_season = sanitize_filename(league_name_and_season)
    
        url = f'https://mc.championdata.com/data/{self.league_id}/{self.match_id}.json'
        response = requests.get(url)
        
        if response.status_code != 200:
            logging.error(f"Failed to retrieve data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            print(f"Failed to retrieve data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            return
    
        data = response.json()
        
        # Check if the data contains player stats
        if ('matchStats' in data and isinstance(data['matchStats'], dict) and
            'playerStats' in data['matchStats'] and isinstance(data['matchStats']['playerStats'], dict) and
            'player' in data['matchStats']['playerStats']):
            
            # Create DataFrames for player stats, team info, and player info
            box = pd.DataFrame(data['matchStats']['playerStats']['player'])
            teams = pd.DataFrame(data['matchStats']['teamInfo']['team'])
            players = pd.DataFrame(data['matchStats']['playerInfo']['player'])
    
            # Merge player stats with player info based on 'playerId' to include 'firstname' and 'surname'
            box = pd.merge(box, players[['playerId', 'firstname', 'surname', 'displayName', 'shortDisplayName']], how='left', on='playerId')
            # Merge with team info based on 'squadId'
            box = pd.merge(box, teams[['squadId', 'squadName']], how='left', on='squadId')
    
            # Extract home and away team information
            home_id = data['matchStats']['matchInfo']['homeSquadId']
            away_id = data['matchStats']['matchInfo']['awaySquadId']
            home = teams.loc[teams['squadId'] == home_id, 'squadName'].iloc[0] if not teams.empty else "Unknown Home Team"
            away = teams.loc[teams['squadId'] == away_id, 'squadName'].iloc[0] if not teams.empty else "Unknown Away Team"
    
            # Add additional match-specific columns to the DataFrame
            box['homeId'] = home_id
            box['awayId'] = away_id
            box['opponent'] = np.where(box['squadId'] == home_id, away, home)
            box['round'] = data['matchStats']['matchInfo']['roundNumber']
            box['fixtureId'] = self.fixture_id
            box['sportId'] = self.sport_id
            box['matchId'] = self.match_id
            box['fixtureYear'] = self.fixture_year

            # Ensure playerId is populated
            if box['playerId'].isnull().any():
                logging.error(f"Missing playerId for some rows in match {self.match_id}.")
                print(f"Missing playerId for some rows in match {self.match_id}. Continuing anyway.")

            # Generate Unique Player ID
            box['uniquePlayerId'] = box.apply(lambda row: f"{row['playerId']}-{row['squadId']}" if pd.notnull(row['playerId']) and pd.notnull(row['squadId']) else 'Unknown', axis=1)

            # Generate Unique Match ID (Composite Key)
            box['uniqueMatchId'] = box.apply(lambda row: f"{row['matchId']}-{row['playerId']}" if pd.notnull(row['matchId']) and pd.notnull(row['playerId']) else 'Unknown', axis=1)
            
            # Remove unwanted columns if necessary
            box = box.drop(columns=['squadNickname', 'squadCode'], errors='ignore')
    
            # Ensure 'firstname' and 'surname' are present
            if 'firstname' not in box.columns or 'surname' not in box.columns:
                logging.error(f"'firstname' or 'surname' not found in match data for matchId: {self.match_id}.")
                print(f"'firstname' or 'surname' not found in match data for matchId: {self.match_id}.")
                # Continue but be aware that 'firstname' and 'surname' will not be available
    
            print(f"Match data inserted for ID:  {self.match_id}")
    
            # Store processed data
            self.data = box
        else:
            logging.error(f"Player stats not found or incomplete for match {self.match_id} in league {self.league_id}.")
            print(f"Player stats not found or incomplete for match {self.match_id} in league {self.league_id}. Skipping this match.")
