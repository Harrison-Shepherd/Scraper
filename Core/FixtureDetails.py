import requests
import os
import pandas as pd
import logging
import re
from Utils.sport_category import determine_sport_category
from Utils.sanitize_filename import sanitize_filename
from Core.LeaguesList import League

class Fixture:
    def __init__(self, league_id, fixture_id, regulation_periods, info_logger, error_logger):
        self.league_id = league_id
        self.fixture_id = fixture_id
        self.regulation_periods = regulation_periods
        self.data = pd.DataFrame()
        self.info_logger = info_logger
        self.error_logger = error_logger

    # Fetch fixture data for the league
    def fetch_data(self):
        self.info_logger.info(f"Fetching fixture data for league {self.league_id}.")
        
        # Ensure that league_info is populated
        if not League.league_info:
            self.info_logger.info("League info not found, fetching leagues...")
            League.fetch_leagues()
        
        # Fetch fixture data from the Champion Data API
        league_name_and_season = League.get_league_name_and_season(self.league_id)

        # Sanitize the league name and season
        sanitized_league_name = sanitize_filename(league_name_and_season)
        
        url = f'http://mc.championdata.com/data/{self.league_id}/fixture.json?/'
        self.info_logger.info(f"Requesting fixture data from URL: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            self.error_logger.error(f"Failed to retrieve fixture data for league {self.league_id}: {response.status_code}")
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
                squad_ids = pd.unique(
                    matches_df[['homeSquadId', 'awaySquadId']].values.ravel()
                ).tolist()
                sport_category, sport_id = determine_sport_category(
                    self.regulation_periods, 
                    squad_ids, 
                    league_name_and_season,
                    self.league_id
                )
                
                # Normalize sport category
                sport_category = sport_category.strip()
                sport_category = re.sub(r'\s+', ' ', sport_category)
                sport_category_lower = sport_category.lower()
                
                # Use the same sport_id_map as in Scraper.py
                sport_id_map = {
                    'afl mens': 1, 
                    'afl womens': 2, 
                    'nrl mens': 3, 
                    'nrl womens': 4,
                    'fast5 mens': 5, 
                    'fast5 womens': 6, 
                    'netball mens': 7,
                    'netball womens nz': 8,
                    'netball womens australia': 9,
                    'netball womens international': 10,
                    'netball unknown': 11,
                    'nrl unknown': 12
                }
                
                # Convert the sport_id_map keys to lowercase
                sport_id_map_lower = {k.lower(): v for k, v in sport_id_map.items()}
                
                # Check if the sport category exists in the map
                if sport_category_lower in sport_id_map_lower:
                    sport_id = sport_id_map_lower[sport_category_lower]
                    self.info_logger.info(f"Sport ID found: {sport_id} for category: '{sport_category}'")
                else:
                    self.error_logger.error(f"Sport category '{sport_category}' not found in sport_id_map for league {self.league_id}.")
                    self.error_logger.error(f"Available sport categories: {list(sport_id_map.keys())}")
                    sport_id = None  # If the category is not in the map, log and skip to avoid failures
                
                # Assign the sport ID to the matches_df
                matches_df['sportId'] = sport_id
                matches_df['fixtureId'] = self.fixture_id

                # Generate uniqueFixtureId (composite of fixtureId and matchId)
                matches_df['uniqueFixtureId'] = matches_df.apply(
                    lambda row: f"{self.fixture_id}-{row['matchId']}" if pd.notnull(row['matchId']) else 'Unknown', axis=1
                )

                # Ensure 'uniqueFixtureId' is of string type
                matches_df['uniqueFixtureId'] = matches_df['uniqueFixtureId'].astype(str)
                # Replace any 'nan' strings with 'Unknown'
                matches_df['uniqueFixtureId'] = matches_df['uniqueFixtureId'].replace('nan', 'Unknown')

                # Log missing match IDs
                if matches_df['uniqueFixtureId'].str.contains('Unknown').any():
                    self.error_logger.warning(f"Some matches in league {self.league_id} are missing matchId, setting 'uniqueFixtureId' to 'Unknown'.")

                # Generate unique squad IDs for home and away squads
                matches_df['uniqueHomeSquadId'] = matches_df.apply(
                    lambda row: f"{row['homeSquadId']}-{row['homeSquadName']}" if pd.notnull(row['homeSquadId']) and pd.notnull(row['homeSquadName']) else 'Unknown', axis=1
                )
                matches_df['uniqueAwaySquadId'] = matches_df.apply(
                    lambda row: f"{row['awaySquadId']}-{row['awaySquadName']}" if pd.notnull(row['awaySquadId']) and pd.notnull(row['awaySquadName']) else 'Unknown', axis=1
                )

                # Ensure 'uniqueHomeSquadId' and 'uniqueAwaySquadId' are of string type
                matches_df['uniqueHomeSquadId'] = matches_df['uniqueHomeSquadId'].astype(str).replace('nan', 'Unknown')
                matches_df['uniqueAwaySquadId'] = matches_df['uniqueAwaySquadId'].astype(str).replace('nan', 'Unknown')

                # Log missing squad IDs
                if matches_df['uniqueHomeSquadId'].str.contains('Unknown').any():
                    self.error_logger.warning(f"Some matches in league {self.league_id} are missing homeSquadId or homeSquadName.")
                if matches_df['uniqueAwaySquadId'].str.contains('Unknown').any():
                    self.error_logger.warning(f"Some matches in league {self.league_id} are missing awaySquadId or awaySquadName.")

                # Assign the processed data to self.data
                self.data = matches_df
            else:
                self.error_logger.error(f"No match data found for league {self.league_id}.")
        else:
            self.error_logger.error(f"Fixture data for league {self.league_id} is not in the expected format.")
