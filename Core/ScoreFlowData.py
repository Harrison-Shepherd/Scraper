import requests
import pandas as pd
import logging

class ScoreFlow:
    def __init__(self, league_id, match_id):
        self.league_id = league_id
        self.match_id = match_id
        self.data = pd.DataFrame()

    def fetch_data(self):
        logging.info(f"Fetching score flow data for match {self.match_id} in league {self.league_id}")

        url = f'https://mc.championdata.com/data/{self.league_id}/{self.match_id}.json'
        response = requests.get(url)

        if response.status_code != 200:
            logging.error(f"Failed to retrieve score flow data for match {self.match_id} in league {self.league_id}: {response.status_code}")
            print(f"Failed to retrieve data: {response.status_code}")
            return

        json_data = response.json()

        # Access score flow data
        match_stats = json_data.get('matchStats', {})
        score_flow = match_stats.get('scoreFlow', {})

        scores = score_flow.get('score', [])

        if not scores:
            logging.warning(f"No score flow data found for match {self.match_id} in league {self.league_id}.")
            print(f"No score flow data found for match {self.match_id} in league {self.league_id}.")
            return

        df = pd.DataFrame(scores)
        df['matchId'] = self.match_id

        # Merge with player info if available
        player_info = match_stats.get('playerInfo', {}).get('player', [])
        if player_info:
            players_df = pd.DataFrame(player_info)
            df = pd.merge(
                df,
                players_df[['playerId', 'firstname', 'surname', 'displayName', 'shortDisplayName']],
                how='left',
                on='playerId'
            )
        else:
            logging.warning(f"Player info not found in score flow data for match {self.match_id} in league {self.league_id}.")
            print(f"Player info not found in score flow data for match {self.match_id} in league {self.league_id}.")

        self.data = df
        print(f"Fetched {len(df)} score flow records for match {self.match_id}.")
