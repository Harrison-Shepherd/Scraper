import requests
import pandas as pd
import logging
from Utils.logger import setup_logging

class ScoreFlow:
    def __init__(self, league_id, match_id):
        self.league_id = league_id
        self.match_id = match_id
        self.data = pd.DataFrame()
    
    def fetch_data(self):
        url = f'https://mc.championdata.com/data/{self.league_id}/{self.match_id}.json'
        response = requests.get(url)
    
        if response.status_code != 200:
            logging.error(f"Failed to retrieve score flow data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            print(f"Failed to retrieve data: {response.status_code}")
            return
    
        match_data = response.json()
        score_flow = match_data.get('matchStats', {}).get('scoreFlow', {}).get('score', [])
    
        if not score_flow:
            logging.warning(f"No score flow data found for match {self.match_id} in league {self.league_id}.")
            print(f"No score flow data found for match {self.match_id} in league {self.league_id}.")
            return
    
        df = pd.json_normalize(score_flow)
        df['matchId'] = self.match_id
        df['scoreFlowId'] = df['matchId'].astype(str) + "_1"  
    
        self.data = df
