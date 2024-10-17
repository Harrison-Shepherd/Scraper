import requests
import os
import pandas as pd
from Utils.sport_category import determine_sport_category
from Utils.sanitize_filename import sanitize_filename
from Core.LeaguesList import League 

class Fixture:
    def __init__(self, league_id, fixture_id, regulation_periods):
        self.league_id = league_id
        self.fixture_id = fixture_id
        self.regulation_periods = regulation_periods
        self.data = pd.DataFrame()
    
    def fetch_data(self):
        print(f"Fetching fixture data for league {self.league_id}.")
        
        # Ensure that league_info is populated
        if not League.league_info:
            # Fetch leagues to populate league_info
            League.fetch_leagues()
        
        league_name_and_season = League.get_league_name_and_season(self.league_id)
        
        url = f'http://mc.championdata.com/data/{self.league_id}/fixture.json?/'

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to retrieve fixture data for league {self.league_id}: {response.status_code}")
            return
        
        data = response.json()
        
        if 'fixture' in data and 'match' in data['fixture']:
            matches = data['fixture']['match']
            if not isinstance(matches, list):
                matches = [matches]
            
            if matches:
                matches_df = pd.DataFrame(matches)
                matches_df = matches_df[~matches_df['matchStatus'].isin(['incomplete', 'scheduled'])]  # Remove incomplete and scheduled matches
    
                # Determine sport category and ID
                sport_category, sport_id = determine_sport_category(
                    self.regulation_periods, 
                    matches_df['homeSquadId'].tolist(), 
                    league_name_and_season,
                    self.league_id
                )
                
                sanitized_league_name = sanitize_filename(league_name_and_season)
                
                # Map the sport category to a sport ID (if available)
                sport_id_map = {
                    'AFL Mens': 1, 'AFL Womens': 2, 'NRL Mens': 3, 'NRL Womens': 4,
                    'FAST5 Mens': 5, 'FAST5 Womens': 6, 'International & NZ Netball Mens': 7,
                    'International & NZ Netball Womens': 8, 'Australian Netball Mens': 9,
                    'Australian Netball Womens': 10
                }
                sport_id = sport_id_map.get(sport_category, None)
                matches_df['sportId'] = sport_id
                matches_df['fixtureId'] = self.fixture_id

                # Generate uniqueFixtureId (composite of fixtureId and matchId)
                matches_df['uniqueFixtureId'] = matches_df.apply(
                    lambda row: f"{self.fixture_id}-{row['matchId']}" if pd.notnull(row['matchId']) else 'Unknown', axis=1
                )

                # Generate unique squad IDs for home and away squads
                matches_df['uniqueHomeSquadId'] = matches_df.apply(
                    lambda row: f"{row['homeSquadId']}-{row['homeSquadName']}" if pd.notnull(row['homeSquadId']) and pd.notnull(row['homeSquadName']) else 'Unknown', axis=1
                )
                matches_df['uniqueAwaySquadId'] = matches_df.apply(
                    lambda row: f"{row['awaySquadId']}-{row['awaySquadName']}" if pd.notnull(row['awaySquadId']) and pd.notnull(row['awaySquadName']) else 'Unknown', axis=1
                )

                self.data = matches_df
            else:
                print(f"No match data found for league {self.league_id}.")
        else:
            print(f"Fixture data for league {self.league_id} is not in the expected format.")
