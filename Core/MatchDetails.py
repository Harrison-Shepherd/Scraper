import requests
import pandas as pd
import numpy as np
import logging
from Utils.sanitize_filename import sanitize_filename  
from Core.LeaguesList import League

class Match:
    # Initialize the Match object with the league_id, match_id, fixture_id, and sport_id
    def __init__(self, league_id, match_id, fixture_id, sport_id):
        self.league_id = league_id
        self.match_id = match_id
        self.fixture_id = fixture_id
        self.sport_id = sport_id
        self.data = pd.DataFrame()

    # Fetch data for the match
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
    
            # Merge player and team info based on playerId and squadId
            box = pd.merge(box, players, how='outer', on='playerId')
            box = pd.merge(box, teams, how='outer', on='squadId')
    
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

            # Ensure playerId is populated
            if box['playerId'].isnull().any():
                logging.warning(f"Missing playerId for some rows in match {self.match_id}.")
                print(f"Missing playerId for some rows in match {self.match_id}. Continuing anyway.")

            # Generate Unique Player ID
            box['uniquePlayerId'] = box.apply(lambda row: f"{row['playerId']}-{row['squadId']}" if pd.notnull(row['playerId']) and pd.notnull(row['squadId']) else 'Unknown', axis=1)

            # Generate Unique Match ID (Composite Key)
            box['uniqueMatchId'] = box.apply(lambda row: f"{row['matchId']}-{row['playerId']}" if pd.notnull(row['matchId']) and pd.notnull(row['playerId']) else 'Unknown', axis=1)
            
            # Remove unwanted columns if necessary
            box = box.drop(columns=['squadNickname', 'squadCode'], errors='ignore')
    
            print(f"Match data inserted for ID:  {self.match_id}")
    
            # Store processed data
            self.data = box
        else:
            logging.warning(f"Player stats not found or incomplete for match {self.match_id} in league {self.league_id}.")
            print(f"Player stats not found or incomplete for match {self.match_id} in league {self.league_id}. Skipping this match.")

# Define the PeriodData class to fetch period stats for a match
class PeriodData:
    def __init__(self, league_id, match_id):
        self.league_id = league_id
        self.match_id = str(match_id)  
        self.data = pd.DataFrame()
    
    # Fetch period stats for the match
    def fetch_data(self):
        logging.info(f"Fetching period stats for match {self.match_id} in league {self.league_id}")
    
        url = f'https://mc.championdata.com/data/{self.league_id}/{self.match_id}.json'
        response = requests.get(url)
    
        if response.status_code != 200:
            logging.error(f"Failed to retrieve data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            return
    
        json_data = response.json()
    
        # Check if the data contains player period stats
        if ('matchStats' in json_data and
            'playerPeriodStats' in json_data['matchStats'] and
            'player' in json_data['matchStats']['playerPeriodStats']):
    
            player_period_stats = json_data['matchStats']['playerPeriodStats']['player']
            df = pd.json_normalize(player_period_stats)

            # Ensure periodId and playerId exist, and generate uniquePeriodId
            df['uniquePeriodId'] = df.apply(
                lambda row: f"{row['periodId']}-{row['playerId']}" if pd.notnull(row.get('periodId')) and pd.notnull(row.get('playerId')) else 'Unknown', axis=1
            )

            self.data = df
        else:
            logging.error(f"Player period stats not found or incomplete for match {self.match_id} in league {self.league_id}.")  


# Define the ScoreFlow class to fetch score flow data for a match
class ScoreFlow:
    def __init__(self, league_id, match_id):
        self.league_id = league_id
        self.match_id = match_id
        self.data = pd.DataFrame()
    
    # Fetch score flow data for the match
    def fetch_data(self):
        url = f'https://mc.championdata.com/data/{self.league_id}/{self.match_id}.json'
        response = requests.get(url)
    
        if response.status_code != 200:
            logging.error(f"Failed to retrieve score flow data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            print(f"Failed to retrieve data: {response.status_code}")
            return
    
        # Extract score flow data
        match_data = response.json()
        score_flow = match_data.get('matchStats', {}).get('scoreFlow', {}).get('score', [])

        # Check if score flow data exists
        if not score_flow:
            logging.warning(f"No score flow data found for match {self.match_id} in league {self.league_id}.")
            print(f"No score flow data found for match {self.match_id} in league {self.league_id}.")
            return
    
        # Normalize the JSON data into a DataFrame
        df = pd.json_normalize(score_flow)
        df['matchId'] = self.match_id
        df['scoreFlowId'] = df['matchId'].astype(str) + "_1"  # Just an example for generating scoreFlowId
    
        self.data = df  
