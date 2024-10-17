import requests
import pandas as pd
import logging
from Utils.logger import setup_logging

class PeriodData:
    def __init__(self, league_id, match_id):
        self.league_id = league_id
        self.match_id = str(match_id)  
        self.data = pd.DataFrame()
    
    def fetch_data(self):
        logging.info(f"Fetching period stats for match {self.match_id} in league {self.league_id}")
    
        url = f'https://mc.championdata.com/data/{self.league_id}/{self.match_id}.json'
        response = requests.get(url)
    
        if response.status_code != 200:
            logging.error(f"Failed to retrieve data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            return
    
        json_data = response.json()
    
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
