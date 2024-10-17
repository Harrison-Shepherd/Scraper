import requests
import os
import pandas as pd
import logging
from Utils.sport_category import determine_sport_category
from Utils.sanitize_filename import sanitize_filename
from Core.LeaguesList import League


class Fixture:
    def __init__(self, league_id, fixture_id, regulation_periods):
        self.league_id = league_id
        self.fixture_id = fixture_id
        self.regulation_periods = regulation_periods
        self.data = pd.DataFrame()

    # Fetch fixture data for the league
    def fetch_data(self):
        logging.info(f"Fetching fixture data for league {self.league_id}.")
        
        # Ensure that league_info is populated
        if not League.league_info:
            logging.info("League info not found, fetching leagues...")
            League.fetch_leagues()
        
        # Fetch fixture data from the Champion Data API
        league_name_and_season = League.get_league_name_and_season(self.league_id)

        # Sanitize the league name and season
        sanitized_league_name = sanitize_filename(league_name_and_season)
        
        url = f'http://mc.championdata.com/data/{self.league_id}/fixture.json?/'
        logging.info(f"Requesting fixture data from URL: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Failed to retrieve fixture data for league {self.league_id}: {response.status_code}")
            return
        
        data = response.json()
        
        # Check if the data contains fixture and match information
        if 'fixture' in data and 'match' in data['fixture']:
            matches = data['fixture']['match']
            if not isinstance(matches, list):
                matches = [matches]
            
            # Filter out incomplete and scheduled matches
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
                
                # Map the sport category to a sport ID (if available)
                sport_id_map = {
                    'AFL Mens': 1, 'AFL Womens': 2, 'NRL Mens': 3, 'NRL Womens': 4,
                    'FAST5 Mens': 5, 'FAST5 Womens': 6, 'International & NZ Netball Mens': 7,
                    'International & NZ Netball Womens': 8, 'Australian Netball Mens': 9,
                    'Australian Netball Womens': 10
                }

                # Assign the sport ID to the matches_df
                sport_id = sport_id_map.get(sport_category, None)
                matches_df['sportId'] = sport_id
                matches_df['fixtureId'] = self.fixture_id

                # Log missing sport category
                if sport_id is None:
                    logging.error(f"Sport category '{sport_category}' not found in sport_id_map for league {self.league_id}.")
                
                # Generate uniqueFixtureId (composite of fixtureId and matchId)
                matches_df['uniqueFixtureId'] = matches_df.apply(
                    lambda row: f"{self.fixture_id}-{row['matchId']}" if pd.notnull(row['matchId']) else 'Unknown', axis=1
                )
                # Log missing match IDs
                if matches_df['uniqueFixtureId'].str.contains('Unknown').any():
                    logging.error(f"Some matches in league {self.league_id} are missing matchId, setting 'uniqueFixtureId' to 'Unknown'.")

                # Generate unique squad IDs for home and away squads
                matches_df['uniqueHomeSquadId'] = matches_df.apply(
                    lambda row: f"{row['homeSquadId']}-{row['homeSquadName']}" if pd.notnull(row['homeSquadId']) and pd.notnull(row['homeSquadName']) else 'Unknown', axis=1
                )
                matches_df['uniqueAwaySquadId'] = matches_df.apply(
                    lambda row: f"{row['awaySquadId']}-{row['awaySquadName']}" if pd.notnull(row['awaySquadId']) and pd.notnull(row['awaySquadName']) else 'Unknown', axis=1
                )

                # Log missing squad IDs
                if matches_df['uniqueHomeSquadId'].str.contains('Unknown').any():
                    logging.error(f"Some matches in league {self.league_id} are missing homeSquadId or homeSquadName.")
                if matches_df['uniqueAwaySquadId'].str.contains('Unknown').any():
                    logging.error(f"Some matches in league {self.league_id} are missing awaySquadId or awaySquadName.")

                # Assign the processed data to self.data
                self.data = matches_df
            else:
                logging.error(f"No match data found for league {self.league_id}.")
        else:
            logging.error(f"Fixture data for league {self.league_id} is not in the expected format.")
