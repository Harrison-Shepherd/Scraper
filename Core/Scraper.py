import logging
from mysql.connector import Error as mysql_error
import json
import os
import pandas as pd
import re
import traceback
from DatabaseUtils.SqlConnector import connect
from DatabaseUtils.database_helper import DatabaseHelper
from Utils.logger import setup_logging  
from Utils.JsonLoader import load_json_fields  
from Core.LeaguesList import League
from Core.FixtureDetails import Fixture
from Core.MatchDetails import Match
from Core.MatchDetails import PeriodData
from Core.MatchDetails import ScoreFlow
from Utils.sport_category import determine_sport_category

class Scraper:
    def __init__(self):
        # Setup logging with both error and info logs
        self.info_logger, self.error_logger = setup_logging()

        self.connection = connect()
        if self.connection is None:
            self.error_logger.error("Failed to connect to the database.")
            raise ConnectionError("Database connection failed.")
        self.connection.autocommit = False  # Turn off auto-commit
        self.db_helper = DatabaseHelper(self.connection, self.info_logger, self.error_logger)

        # Load JSON fields for each table
        self.json_fields = load_json_fields()
        self.fixture_fields = self.json_fields['fixture_fields']
        self.match_fields = self.json_fields['match_fields']
        self.period_fields = self.json_fields['period_fields']
        self.score_flow_fields = self.json_fields['score_flow_fields']
        self.player_fields = self.json_fields['player_fields']
        self.squad_fields = self.json_fields['squad_fields']
        self.sport_fields = self.json_fields['sport_fields']

    def scrape_entire_database(self):
        # Define the sport_id_map
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

        # Fetch leagues
        leagues_df, _ = League.fetch_leagues()
        print(f"Fetched {len(leagues_df)} leagues.")

        fallback_player_counter = {}  # Initialize the fallback player counter

        for _, league in leagues_df.iterrows():
            league_id = league['id']
            league_name = league['league_season']
            regulation_periods = league['regulationPeriods']
            fixture_id = league['id']

            fixture = Fixture(league_id, fixture_id, regulation_periods, self.info_logger, self.error_logger)
            fixture.fetch_data()
            print(f"Fetched {len(fixture.data)} fixtures for league {league_id}.")
            if fixture.data.empty:
                continue

            # Extract squad ids from fixture data
            squad_ids = pd.unique(
                fixture.data[['homeSquadId', 'awaySquadId']].values.ravel()
            ).tolist()
            print(f"Extracted squad IDs: {squad_ids}")

            # Filter sport category and sport id using sport category function
            sport_category, sport_id = determine_sport_category(
                regulation_periods,
                squad_ids,
                league_name,
                league_id
            )

            # Normalize the sport category for logging and comparison
            sport_category = sport_category.strip()
            sport_category = re.sub(r'\s+', ' ', sport_category)  # Remove extra spaces
            sport_category_lower = sport_category.lower()

            # Log the normalized category
            self.info_logger.info(f"Normalized sport category: '{sport_category}' for league: {league_id}")
            self.info_logger.debug(f"Sport category representation: {repr(sport_category)}")
            self.info_logger.debug(f"Length of sport_category: {len(sport_category)}")

            # Convert the sport_id_map keys to lowercase
            sport_id_map_lower = {k.lower(): v for k, v in sport_id_map.items()}

            # Log the keys of sport_id_map_lower
            self.info_logger.debug("Keys in sport_id_map_lower:")
            for key in sport_id_map_lower.keys():
                self.info_logger.debug(f"Key: '{key}', repr: {repr(key)}, length: {len(key)}")

            # Check if the normalized sport category exists in the sport_id_map_lower
            if sport_category_lower in sport_id_map_lower:
                sport_id = sport_id_map_lower[sport_category_lower]
                self.info_logger.info(f"Sport ID found: {sport_id} for category: '{sport_category}'")
            else:
                self.error_logger.error(f"Sport category '{sport_category}' not found in sport_id_map for league {league_id}.")
                self.error_logger.error(f"Available sport categories: {list(sport_id_map.keys())}")
                self.error_logger.error(f"Sport category representation: {repr(sport_category)}")
                self.error_logger.error(f"Length of sport_category: {len(sport_category)}")
                # Log the keys of sport_id_map_lower
                self.error_logger.error("Keys in sport_id_map_lower:")
                for key in sport_id_map_lower.keys():
                    self.error_logger.error(f"Key: '{key}', repr: {repr(key)}, length: {len(key)}")
                sport_id = None  # If the category is not in the map, log and skip to avoid failures

            match_year = re.search(r'\b(20\d{2})\b', league_name)
            fixture_year = match_year.group(1) if match_year else None

            # Start the transaction
            try:
                # Begin transaction
                self.connection.start_transaction()

                # Process sport info
                sport_info_data = {
                    'sportId': str(sport_id),
                    'sportName': sport_category,
                    'fixtureId': str(fixture_id),
                    'fixtureTitle': league_name,
                    'fixtureYear': None  # Will be set below
                }

                # Generate unique sport ID
                uniqueSportId = f"{sport_id}-{fixture_id}" if sport_id and fixture_id else 'Unknown'

                # Add uniqueSportId to sport_info_data dictionary
                sport_info_data['uniqueSportId'] = uniqueSportId

                # Extract fixture year from league_name
                match_year = re.search(r'\b(20\d{2})\b', league_name)
                if match_year:
                    sport_info_data['fixtureYear'] = match_year.group(1)
                    fixture_title = league_name.replace(match_year.group(1), '').strip()
                    fixture_title = re.sub(r'\(\)$', '', fixture_title).strip()
                    sport_info_data['fixtureTitle'] = fixture_title
                else:
                    sport_info_data['fixtureYear'] = None

                # Collect data for batch insertion
                squad_info_list = []
                fixture_data_list = []
                player_info_list = []
                match_data_list = []
                period_data_list = []
                score_flow_data_list = []

                # For table names
                table_prefix = sport_category_lower.replace(' ', '_')
                fixture_table = f"{table_prefix}_fixture"
                match_table = f"{table_prefix}_match"
                period_table = f"{table_prefix}_period"
                score_flow_table = f"{table_prefix}_score_flow"

                for index, match_row in fixture.data.iterrows():
                    if match_row['matchStatus'] in ['scheduled', 'incomplete']:
                        continue

                    match_id = match_row['matchId'] or 'Unknown'
                    fixture.data.at[index, 'sportId'] = sport_id

                    # Ensure fixture_id and match_id are not null for generating uniqueFixtureId
                    uniqueFixtureId = f"{fixture_id}-{match_id}"
                    print(f"Unique fixture ID: {uniqueFixtureId}")

                    # Ensure matchName is populated (fallback if it's None)
                    match_name = match_row.get('matchName', 'Unknown Match')

                    # Log uniqueMatchId
                    uniqueMatchId = f"{match_id}-{fixture_id}"
                    self.info_logger.info(f"Generated uniqueMatchId: {uniqueMatchId} for match {match_id}.")

                    # Collect fixture data
                    fixture_data = {
                        **match_row,
                        'fixtureId': fixture_id,
                        'sportId': sport_id,
                        'matchId': match_id,
                        'uniqueFixtureId': uniqueFixtureId,
                        'matchName': match_name,  
                        'uniqueSportId': uniqueSportId  
                    }
                    fixture_data_list.append(fixture_data)

                    # Collect squad info for both home and away
                    for squad_side in ['home', 'away']:
                        squad_id = str(match_row.get(f'{squad_side}SquadId', 'Unknown'))
                        squad_name = str(match_row.get(f'{squad_side}SquadName', 'Unknown Squad'))

                        # Log warnings if squadId or squadName are missing
                        if not squad_id or squad_id == 'Unknown':
                            self.error_logger.warning(f"Missing squadId for {squad_side} side in match {match_id}. Using fallback value.")
                        if not squad_name or squad_name == 'Unknown Squad':
                            self.error_logger.warning(f"Missing squadName for {squad_side} side in match {match_id}. Using fallback value.")

                        # Generate uniqueSquadId
                        uniqueSquadId = f"{squad_id}-{squad_name}"
                        self.info_logger.info(f"Generated uniqueSquadId: {uniqueSquadId} for {squad_side} side in match {match_id}.")

                        squad_info_data = {
                            'squadId': squad_id,
                            'squadName': squad_name,
                            'uniqueSquadId': uniqueSquadId,
                            'fixtureTitle': sport_info_data['fixtureTitle'],
                            'fixtureYear': sport_info_data['fixtureYear']
                        }
                        squad_info_list.append(squad_info_data)

                    # Fetch match data
                    match = Match(league_id, match_id, fixture_id, sport_id, fixture_year)
                    match.fetch_data()

                    if match.data.empty:
                        self.error_logger.warning(f"Match data is empty for matchId: {match_id}, leagueId: {league_id}.")
                    else:
                        print(f"Fetched {len(match.data)} match records for match {match_id}.")

                    # Process and collect match data
                    for _, row in match.data.iterrows():
                        player_id = str(row.get('playerId', 'Unknown'))
                        squad_id = str(row.get('squadId', 'Unknown'))
                        squad_name = str(row.get('squadName', 'Unknown Squad'))

                        # Check if player_id is None or empty and generate a fallback player_id
                        if player_id == '0' or not player_id.isdigit():
                            # Initialize the fallback counter for this match if not already initialized
                            if match_id not in fallback_player_counter:
                                fallback_player_counter[match_id] = 1
                            else:
                                fallback_player_counter[match_id] += 1

                            # Generate a fallback playerId like 'unknownPlayer1'
                            player_id = f"unknownPlayer{fallback_player_counter[match_id]}"

                        # Log uniquePlayerId
                        uniquePlayerId = f"{player_id}-{squad_id}"
                        self.info_logger.info(f"Generated uniquePlayerId: {uniquePlayerId} for player {player_id} in match {match_id}.")

                        row['matchId'] = str(match_id)
                        row['playerId'] = player_id  # Update row with fallback playerId
                        row['squadId'] = squad_id
                        row['squadName'] = squad_name

                        # Generate unique IDs
                        uniqueMatchId = f"{match_id}-{player_id}"
                        uniqueSquadId = f"{squad_id}-{squad_name}"
                        uniqueSportId = f"{sport_id}-{fixture_id}"
                        uniqueFixtureId = f"{fixture_id}-{match_id}"

                        row['uniquePlayerId'] = uniquePlayerId
                        row['uniqueMatchId'] = uniqueMatchId
                        row['uniqueSquadId'] = uniqueSquadId
                        row['uniqueSportId'] = uniqueSportId
                        row['uniqueFixtureId'] = uniqueFixtureId

                        match_data_list.append(row.to_dict())

                        # Collect player info
                        player_info_data = {
                            'playerId': player_id,
                            'firstname': row.get('firstname', 'Unknown'),
                            'surname': row.get('surname', 'Unknown'),
                            'displayName': row.get('displayName', 'Unknown'),
                            'shortDisplayName': row.get('shortDisplayName', 'Unknown'),
                            'squadName': squad_name,
                            'squadId': squad_id,
                            'sportId': sport_id,
                            'uniqueSquadId': uniqueSquadId,
                            'uniquePlayerId': uniquePlayerId
                        }
                        player_info_list.append(player_info_data)

                    print(f"Collected {len(match_data_list)} match entries for league {league_id}.")

                    # Fetch period data
                    period_data = PeriodData(league_id, match_id)
                    period_data.fetch_data()
                    print(f"Fetched {len(period_data.data)} period records for match {match_id}.")

                    if not period_data.data.empty:
                        period_data.data['matchId'] = str(match_id)

                        for idx, row in period_data.data.iterrows():
                            period_num = str(row.get('period', 'Unknown'))
                            period_id = f"{match_id}_{period_num}"

                            # Fetch the playerId and squadId for generating uniquePlayerId
                            player_id = str(row.get('playerId', 'Unknown'))
                            squad_id = str(row.get('squadId', 'Unknown'))
                            squad_name = str(row.get('squadName', 'Unknown Squad'))

                            # Apply the same fallback logic for playerId
                            if player_id == '0' or not player_id.isdigit():
                                # Initialize the fallback counter for this match if not already initialized
                                if match_id not in fallback_player_counter:
                                    fallback_player_counter[match_id] = 1
                                else:
                                    fallback_player_counter[match_id] += 1

                                # Generate a fallback playerId like 'unknownPlayer1'
                                player_id = f"unknownPlayer{fallback_player_counter[match_id]}"

                            row['playerId'] = player_id

                            # Generate unique IDs
                            uniquePlayerId = f"{player_id}-{squad_id}"
                            uniqueMatchId = f"{match_id}-{player_id}"
                            uniqueSquadId = f"{squad_id}-{squad_name}"
                            uniqueSportId = f"{sport_id}-{fixture_id}"
                            uniqueFixtureId = f"{fixture_id}-{match_id}"
                            uniquePeriodId = period_id  # Use period_id as the uniquePeriodId

                            # Assign the generated IDs back to the row
                            row['uniquePlayerId'] = uniquePlayerId
                            row['uniqueMatchId'] = uniqueMatchId
                            row['uniqueSquadId'] = uniqueSquadId
                            row['uniqueSportId'] = uniqueSportId
                            row['uniqueFixtureId'] = uniqueFixtureId
                            row['periodId'] = period_id
                            row['uniquePeriodId'] = uniquePeriodId

                            # Add the row to the period_data_list
                            period_data_list.append(row.to_dict())

                    # Fetch score flow data
                    score_flow = ScoreFlow(league_id, match_id)
                    score_flow.fetch_data()
                    print(f"Fetched {len(score_flow.data)} score flow records for match {match_id}.")
                    if not score_flow.data.empty:
                        score_flow_counter = 1
                        for idx, row in score_flow.data.iterrows():
                            score_flow_id = f"{match_id}_flow_{score_flow_counter}"
                            score_flow.data.at[idx, 'scoreFlowId'] = score_flow_id
                            score_flow_counter += 1

                            row['uniqueMatchId'] = f"{match_id}-{row.get('playerId', 'Unknown')}"
                            row['uniquePlayerId'] = f"{row.get('playerId', 'Unknown')}-{row.get('squadId', 'Unknown')}"

                            score_flow_data_list.append(row.to_dict())

                print(f"Collected {len(squad_info_list)} squad info entries.")
                print(f"Collected {len(player_info_list)} player info entries.")
                print(f"Collected {len(fixture_data_list)} fixture entries.")
                print(f"Collected {len(match_data_list)} match entries.")
                print(f"Collected {len(period_data_list)} period data entries.")
                print(f"Collected {len(score_flow_data_list)} score flow entries.")

                # Insert data with individual error handling

                # 1. Insert squad info
                for squad_info_data in squad_info_list:
                    print(f"Inserting squad info: {squad_info_data}")
                    try:
                        self.db_helper.insert_data_dynamically('squad_info', squad_info_data, self.squad_fields)
                    except mysql_error as err:
                        self.error_logger.error(f"MySQL error inserting squad info for fixtureId: {fixture_id}, squadId: {squad_info_data['squadId']}. Error: {err}")
                        self.error_logger.error(f"Data causing error: {squad_info_data}")
                        continue  # Skip this squad_info_data and continue with the next one

                # 2. Insert sport info
                print(f"Inserting sport info: {sport_info_data}")
                try:
                    self.db_helper.insert_data_dynamically('sport_info', sport_info_data, self.sport_fields)
                except mysql_error as err:
                    self.error_logger.error(f"MySQL error inserting sport info for fixtureId: {fixture_id}. Error: {err}")
                    self.error_logger.error(f"Data causing error: {sport_info_data}")
                    self.connection.rollback()
                    self.error_logger.error(f"Transaction rolled back for fixtureId: {fixture_id}")
                    continue  # Skip to the next fixture

                # 3. Insert player info
                for player_info_data in player_info_list:
                    print(f"Inserting player info: {player_info_data}")
                    try:
                        self.db_helper.insert_data_dynamically('player_info', player_info_data, self.player_fields)
                    except mysql_error as err:
                        self.error_logger.error(f"MySQL error inserting player info for playerId: {player_info_data['playerId']}. Error: {err}")
                        self.error_logger.error(f"Data causing error: {player_info_data}")
                        continue  # Skip this player_info_data and continue with the next one

                # 4. Insert fixture data
                for fixture_data in fixture_data_list:
                    print(f"Inserting fixture data: {fixture_data}")
                    try:
                        self.db_helper.insert_data_dynamically(fixture_table, fixture_data, self.fixture_fields)
                    except mysql_error as err:
                        self.error_logger.error(f"MySQL error inserting fixture data for uniqueFixtureId: {fixture_data['uniqueFixtureId']}. Error: {err}")
                        self.error_logger.error(f"Data causing error: {fixture_data}")
                        continue  # Skip this fixture_data and continue with the next one

                # 5. Insert match data
                for match_data in match_data_list:
                    print(f"Inserting match data: {match_data}")
                    try:
                        self.db_helper.insert_data_dynamically(match_table, match_data, self.match_fields)
                    except mysql_error as err:
                        self.error_logger.error(f"MySQL error inserting match data for uniqueMatchId: {match_data['uniqueMatchId']}. Error: {err}")
                        self.error_logger.error(f"Data causing error: {match_data}")
                        continue  # Skip this match_data and continue with the next one

                # 6. Insert period data
                for period_row in period_data_list:
                    print(f"Inserting period data: {period_row}")
                    try:
                        self.db_helper.insert_data_dynamically(period_table, period_row, self.period_fields)
                    except mysql_error as err:
                        self.error_logger.error(f"MySQL error inserting period data for uniquePeriodId: {period_row['uniquePeriodId']}. Error: {err}")
                        self.error_logger.error(f"Data causing error: {period_row}")
                        continue  # Skip this period_row and continue with the next one

                # 7. Insert score flow data
                for score_flow_row in score_flow_data_list:
                    print(f"Inserting score flow data: {score_flow_row}")
                    try:
                        self.db_helper.insert_data_dynamically(score_flow_table, score_flow_row, self.score_flow_fields)
                    except mysql_error as err:
                        self.error_logger.error(f"MySQL error inserting score flow data for scoreFlowId: {score_flow_row['scoreFlowId']}. Error: {err}")
                        self.error_logger.error(f"Data causing error: {score_flow_row}")
                        continue  # Skip this score_flow_row and continue with the next one

                # Commit the transaction after successful batch insertion
                self.connection.commit()
                print(f"Transaction committed successfully for fixtureId: {fixture_id}")

            except mysql_error as err:
                # Log the error and rollback the transaction
                self.error_logger.error(f"MySQL error during transaction for fixtureId: {fixture_id}, leagueId: {league_id}. Error: {err}")
                self.error_logger.error(f"MySQL Error Code: {err.errno}, SQLSTATE: {err.sqlstate}, Message: {err.msg}")
                self.connection.rollback()
                self.error_logger.error(f"Transaction rolled back for fixtureId: {fixture_id}")
                continue  # Skip to the next fixture

            except Exception as e:
                # Log any other exceptions and rollback the transaction
                self.error_logger.error(f"Unexpected error during transaction for fixtureId: {fixture_id}, leagueId: {league_id}. Error: {e}")
                self.error_logger.error(f"Traceback: {traceback.format_exc()}")
                self.connection.rollback()
                self.error_logger.error(f"Transaction rolled back for fixtureId: {fixture_id}")
                continue  # Skip to the next fixture
