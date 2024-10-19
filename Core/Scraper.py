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
from Core.PeriodData import PeriodData
from Core.ScoreFlowData import ScoreFlow
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
        self.db_helper = DatabaseHelper(
            self.connection, self.info_logger, self.error_logger)

        # Load JSON fields for each table
        self.json_fields = load_json_fields()
        self.fixture_fields = self.json_fields['fixture_fields']
        self.match_fields = self.json_fields['match_fields']
        self.period_fields = self.json_fields['period_fields']
        self.score_flow_fields = self.json_fields['score_flow_fields']
        self.player_fields = self.json_fields['player_fields']
        self.squad_fields = self.json_fields['squad_fields']
        self.sport_fields = self.json_fields['sport_fields']

        # Path to the BrokenFixtures.json file
        self.broken_fixtures_file = os.path.join(
            'Assets', 'Jsons', 'BrokenFixtures.json')
        # Initialize the broken fixtures list
        self.broken_fixtures = []

        # Load existing broken fixtures if the file exists
        if os.path.exists(self.broken_fixtures_file):
            with open(self.broken_fixtures_file, 'r') as f:
                try:
                    self.broken_fixtures = json.load(f)
                except json.JSONDecodeError:
                    self.broken_fixtures = []

    def add_broken_fixture(self, fixture_id):
        if fixture_id not in self.broken_fixtures:
            self.broken_fixtures.append(fixture_id)
            # Write the updated list to the JSON file
            with open(self.broken_fixtures_file, 'w') as f:
                json.dump(self.broken_fixtures, f)
            self.error_logger.info(
                f"Added fixtureId {fixture_id} to broken fixtures list.")

    def find_player_id(self, firstname, surname, squad_name=None):
        # Normalize the names
        firstname = firstname.strip().lower()
        surname = surname.strip().lower()
        params = [firstname, surname]

        # Prepare the base query
        query = """
        SELECT playerId FROM static_player_info
        WHERE LOWER(firstname) = %s AND LOWER(surname) = %s
        """

        # If squad_name is provided and not 'Unknown Squad', include it
        if squad_name and squad_name.lower() != 'unknown squad':
            squad_name = squad_name.strip().lower()
            query += " AND LOWER(squadName) = %s"
            params.append(squad_name)

        # Log the query and parameters
        self.error_logger.debug(
            f"Executing query: {query} with params: {params}")

        # Execute the query
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        cursor.close()

        # Process the result
        if len(result) == 1:
            player_id_found = result[0][0]
            self.error_logger.info(
                f"Found playerId {player_id_found} for {firstname} "
                f"{surname} with squadName {squad_name}.")
            return player_id_found  # Return the playerId
        elif len(result) > 1:
            self.error_logger.warning(
                f"Multiple playerIds found for {firstname} {surname} with "
                f"squadName {squad_name}. Using the first one.")
            return result[0][0]
        else:
            self.error_logger.warning(
                f"No playerId found for {firstname} {surname} with "
                f"squadName {squad_name}.")
            return None  # No match found

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

        for _, league in leagues_df.iterrows():
            league_id = league['id']
            league_name = league['league_season']
            regulation_periods = league['regulationPeriods']
            fixture_id = league['id']

            fixture = Fixture(
                league_id, fixture_id, regulation_periods,
                self.info_logger, self.error_logger)
            fixture.fetch_data()
            print(f"Fetched {len(fixture.data)} fixtures for league "
                  f"{league_id}.")
            if fixture.data.empty:
                continue

            # Extract squad ids from fixture data
            squad_ids = pd.unique(
                fixture.data[['homeSquadId', 'awaySquadId']].values.ravel()
            ).tolist()
            print(f"Extracted squad IDs: {squad_ids}")

            # Filter sport category and sport id
            sport_category, sport_id = determine_sport_category(
                regulation_periods,
                squad_ids,
                league_name,
                league_id
            )

            # Normalize the sport category
            sport_category = sport_category.strip()
            sport_category = re.sub(
                r'\s+', ' ', sport_category)  # Remove extra spaces
            sport_category_lower = sport_category.lower()

            # Log the normalized category
            self.info_logger.info(
                f"Normalized sport category: '{sport_category}' "
                f"for league: {league_id}")

            # Convert the sport_id_map keys to lowercase
            sport_id_map_lower = {k.lower(): v for k, v in sport_id_map.items()}

            # Check if the sport category exists in the map
            if sport_category_lower in sport_id_map_lower:
                sport_id = sport_id_map_lower[sport_category_lower]
                self.info_logger.info(
                    f"Sport ID found: {sport_id} for category: "
                    f"'{sport_category}'")
            else:
                self.error_logger.error(
                    f"Sport category '{sport_category}' not found in "
                    f"sport_id_map for league {league_id}.")
                sport_id = None  # Skip if category not in map

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
                    'fixtureYear': fixture_year,
                    'uniqueSportId': f"{sport_id}-{fixture_id}"
                    if sport_id and fixture_id else 'Unknown'
                }

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

                # Initialize sets to track processed IDs
                processed_unique_match_ids = set()
                processed_unique_squad_ids = set()

                for index, match_row in fixture.data.iterrows():
                    if match_row['matchStatus'] in ['scheduled', 'incomplete']:
                        continue

                    match_id = match_row['matchId'] or 'Unknown'
                    fixture.data.at[index, 'sportId'] = sport_id

                    # Generate uniqueFixtureId
                    uniqueFixtureId = f"{fixture_id}-{match_id}"
                    print(f"Unique fixture ID: {uniqueFixtureId}")

                    # Ensure matchName is populated
                    match_name = match_row.get('matchName') or (
                        f"{match_row['homeSquadName']} vs "
                        f"{match_row['awaySquadName']} | "
                        f"{match_row['localStartTime']}")

                    # Log uniqueMatchId
                    uniqueMatchId = f"{match_id}-{fixture_id}"
                    self.info_logger.info(
                        f"Generated uniqueMatchId: {uniqueMatchId} for "
                        f"match {match_id}.")

                    # Collect fixture data
                    fixture_data = {
                        **match_row,
                        'fixtureId': fixture_id,
                        'sportId': sport_id,
                        'matchId': match_id,
                        'uniqueFixtureId': uniqueFixtureId,
                        'matchName': match_name,
                        'uniqueSportId': sport_info_data['uniqueSportId']
                    }
                    fixture_data_list.append(fixture_data)

                    # Collect squad info for both home and away
                    for squad_side in ['home', 'away']:
                        squad_id = str(match_row.get(
                            f'{squad_side}SquadId', 'Unknown'))
                        squad_name_raw = match_row.get(
                            f'{squad_side}SquadName', '')
                        # Handle NaN values for squad_name
                        if not isinstance(squad_name_raw, str) or pd.isnull(
                                squad_name_raw):
                            squad_name = 'Unknown Squad'
                        else:
                            squad_name = squad_name_raw.strip()

                        # Generate uniqueSquadId
                        uniqueSquadId = f"{squad_id}-{squad_name}"
                        self.info_logger.info(
                            f"Generated uniqueSquadId: {uniqueSquadId} "
                            f"for {squad_side} side in match {match_id}.")

                        if uniqueSquadId not in processed_unique_squad_ids:
                            squad_info_data = {
                                'squadId': squad_id,
                                'squadName': squad_name,
                                'uniqueSquadId': uniqueSquadId,
                                'fixtureTitle': sport_info_data[
                                    'fixtureTitle'],
                                'fixtureYear': sport_info_data['fixtureYear']
                            }
                            squad_info_list.append(squad_info_data)
                            processed_unique_squad_ids.add(uniqueSquadId)

                    # Fetch match data
                    match = Match(
                        league_id, match_id, fixture_id, sport_id,
                        fixture_year)
                    match.fetch_data()

                    if match.data.empty:
                        self.error_logger.warning(
                            f"Match data is empty for matchId: {match_id}, "
                            f"leagueId: {league_id}.")
                        continue  # Skip to next match

                    print(f"Fetched {len(match.data)} match records for "
                          f"match {match_id}.")

                    # Ensure 'firstname' and 'surname' are in match.data
                    if 'firstname' not in match.data.columns or 'surname' not \
                            in match.data.columns:
                        self.error_logger.error(
                            f"'firstname' or 'surname' not found in match "
                            f"data for matchId: {match_id}. Skipping match.")
                        continue  # Skip this match

                    # Process and collect match data
                    for _, row in match.data.iterrows():
                        player_id = str(row.get('playerId', 'Unknown'))
                        squad_id = str(row.get('squadId', 'Unknown'))

                        # Extract squad_name, handle NaN values
                        squad_name_raw = row.get('squadName', '')
                        if not isinstance(squad_name_raw, str) or pd.isnull(
                                squad_name_raw):
                            squad_name = 'Unknown Squad'
                        else:
                            squad_name = squad_name_raw.strip()

                        # Extract firstname and surname
                        firstname = row.get('firstname', '')
                        surname = row.get('surname', '')

                        # Handle non-string types and NaN values
                        if not isinstance(firstname, str) or pd.isnull(
                                firstname):
                            firstname = ''
                        else:
                            firstname = firstname.strip()
                        if not isinstance(surname, str) or pd.isnull(surname):
                            surname = ''
                        else:
                            surname = surname.strip()

                        # Check if player_id is missing or invalid
                        if player_id == '0' or not player_id.isdigit():
                            self.error_logger.warning(
                                f"Invalid or missing playerId '{player_id}' "
                                f"for row: {row.to_dict()}")
                            if firstname and surname:
                                self.error_logger.info(
                                    f"Attempting to find playerId for "
                                    f"{firstname} {surname} with squadName "
                                    f"'{squad_name}'.")
                                found_player_id = self.find_player_id(
                                    firstname, surname, squad_name)
                                if found_player_id:
                                    player_id = str(found_player_id)
                                    self.info_logger.info(
                                        f"Found playerId {player_id} for "
                                        f"{firstname} {surname}.")
                                else:
                                    self.error_logger.warning(
                                        f"Could not find playerId for "
                                        f"{firstname} {surname} in match "
                                        f"{match_id}. Skipping row.")
                                    continue  # Skip this row
                            else:
                                self.error_logger.warning(
                                    f"Missing firstname or surname for player "
                                    f"in match {match_id}. Skipping row.")
                                continue  # Skip this row

                        # Log uniquePlayerId
                        uniquePlayerId = f"{player_id}-{squad_id}"
                        self.info_logger.info(
                            f"Generated uniquePlayerId: {uniquePlayerId} for "
                            f"player {player_id} in match {match_id}.")

                        row['matchId'] = str(match_id)
                        row['playerId'] = player_id  # Update playerId
                        row['squadId'] = squad_id
                        row['squadName'] = squad_name

                        # Generate unique IDs
                        uniqueMatchId = f"{match_id}-{player_id}"
                        uniqueSquadId = f"{squad_id}-{squad_name}"
                        uniqueSportId = sport_info_data['uniqueSportId']
                        uniqueFixtureId = f"{fixture_id}-{match_id}"

                        row['uniquePlayerId'] = uniquePlayerId
                        row['uniqueMatchId'] = uniqueMatchId
                        row['uniqueSquadId'] = uniqueSquadId
                        row['uniqueSportId'] = uniqueSportId
                        row['uniqueFixtureId'] = uniqueFixtureId

                        match_data_list.append(row.to_dict())

                        # Add the uniqueMatchId to the set
                        processed_unique_match_ids.add(uniqueMatchId)

                        # Collect player info
                        player_info_data = {
                            'playerId': player_id,
                            'firstname': firstname or 'Unknown',
                            'surname': surname or 'Unknown',
                            'displayName': row.get('displayName', 'Unknown'),
                            'shortDisplayName': row.get(
                                'shortDisplayName', 'Unknown'),
                            'squadName': squad_name,
                            'squadId': squad_id,
                            'sportId': sport_id,
                            'uniqueSquadId': uniqueSquadId,
                            'uniquePlayerId': uniquePlayerId
                        }
                        player_info_list.append(player_info_data)

                    print(f"Collected {len(match_data_list)} match entries "
                          f"for league {league_id}.")

                    # Fetch period data
                    period_data = PeriodData(league_id, match_id)
                    period_data.fetch_data()
                    print(f"Fetched {len(period_data.data)} period records "
                          f"for match {match_id}.")

                    if not period_data.data.empty:
                        period_data.data['matchId'] = str(match_id)

                        # Ensure 'firstname' and 'surname' are present
                        if 'firstname' not in period_data.data.columns or \
                                'surname' not in period_data.data.columns:
                            self.error_logger.error(
                                f"'firstname' or 'surname' not found in "
                                f"period data for matchId: {match_id}. "
                                f"Skipping period data.")
                        else:
                            for idx, row in period_data.data.iterrows():
                                period_num = str(row.get('period', 'Unknown'))
                                period_id = f"{match_id}_{period_num}"

                                # Fetch playerId and squadId
                                player_id = str(row.get(
                                    'playerId', 'Unknown'))
                                squad_id = str(row.get('squadId', 'Unknown'))

                                # Extract squad_name, handle NaN values
                                squad_name_raw = row.get('squadName', '')
                                if not isinstance(
                                        squad_name_raw, str) or pd.isnull(
                                        squad_name_raw):
                                    squad_name = 'Unknown Squad'
                                else:
                                    squad_name = squad_name_raw.strip()

                                # Extract firstname and surname
                                firstname = row.get('firstname', '')
                                surname = row.get('surname', '')

                                # Handle non-string types and NaN values
                                if not isinstance(
                                        firstname, str) or pd.isnull(
                                        firstname):
                                    firstname = ''
                                else:
                                    firstname = firstname.strip()
                                if not isinstance(
                                        surname, str) or pd.isnull(surname):
                                    surname = ''
                                else:
                                    surname = surname.strip()

                                # Apply the same logic for playerId
                                if player_id == '0' or not player_id.isdigit():
                                    self.error_logger.warning(
                                        f"Invalid or missing playerId "
                                        f"'{player_id}' for period data row: "
                                        f"{row.to_dict()}")
                                    if firstname and surname:
                                        self.error_logger.info(
                                            f"Attempting to find playerId for "
                                            f"{firstname} {surname} with "
                                            f"squadName '{squad_name}'.")
                                        found_player_id = self.find_player_id(
                                            firstname, surname, squad_name)
                                        if found_player_id:
                                            player_id = str(found_player_id)
                                            self.info_logger.info(
                                                f"Found playerId {player_id} "
                                                f"for {firstname} {surname} "
                                                f"in period data.")
                                        else:
                                            self.error_logger.warning(
                                                f"Could not find playerId for "
                                                f"{firstname} {surname} in "
                                                f"period data for match "
                                                f"{match_id}. Skipping row.")
                                            continue  # Skip this row
                                    else:
                                        self.error_logger.warning(
                                            f"Missing firstname or surname "
                                            f"for player in period data for "
                                            f"match {match_id}. Skipping row.")
                                        continue  # Skip this row

                                # Generate uniqueMatchId
                                uniqueMatchId = f"{match_id}-{player_id}"

                                # Check if uniqueMatchId was processed
                                if uniqueMatchId not in \
                                        processed_unique_match_ids:
                                    self.error_logger.warning(
                                        f"Match data for uniqueMatchId "
                                        f"{uniqueMatchId} not found. "
                                        f"Skipping period data row.")
                                    continue  # Skip this period data row

                                row['playerId'] = player_id

                                # Generate unique IDs
                                uniquePlayerId = f"{player_id}-{squad_id}"
                                uniqueSquadId = f"{squad_id}-{squad_name}"
                                uniqueSportId = sport_info_data[
                                    'uniqueSportId']
                                uniqueFixtureId = f"{fixture_id}-{match_id}"
                                uniquePeriodId = period_id

                                # Assign IDs back to the row
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
                    print(f"Fetched {len(score_flow.data)} score flow records "
                          f"for match {match_id}.")
                    if not score_flow.data.empty:
                        if 'firstname' not in score_flow.data.columns or \
                                'surname' not in score_flow.data.columns:
                            self.error_logger.error(
                                f"'firstname' or 'surname' not found in "
                                f"score flow data for matchId: {match_id}. "
                                f"Skipping score flow data.")
                        else:
                            score_flow_counter = 1
                            for idx, row in score_flow.data.iterrows():
                                score_flow_id = f"{match_id}_flow_" \
                                                f"{score_flow_counter}"
                                score_flow_counter += 1

                                # Similar logic for playerId
                                player_id = str(row.get(
                                    'playerId', 'Unknown'))
                                squad_id = str(row.get('squadId', 'Unknown'))

                                # Extract squad_name, handle NaN values
                                squad_name_raw = row.get('squadName', '')
                                if not isinstance(
                                        squad_name_raw, str) or pd.isnull(
                                        squad_name_raw):
                                    squad_name = 'Unknown Squad'
                                else:
                                    squad_name = squad_name_raw.strip()

                                # Extract firstname and surname
                                firstname = row.get('firstname', '')
                                surname = row.get('surname', '')

                                # Handle non-string types and NaN values
                                if not isinstance(
                                        firstname, str) or pd.isnull(
                                        firstname):
                                    firstname = ''
                                else:
                                    firstname = firstname.strip()
                                if not isinstance(
                                        surname, str) or pd.isnull(surname):
                                    surname = ''
                                else:
                                    surname = surname.strip()

                                if player_id == '0' or not player_id.isdigit():
                                    self.error_logger.warning(
                                        f"Invalid or missing playerId "
                                        f"'{player_id}' for score flow data "
                                        f"row: {row.to_dict()}")
                                    if firstname and surname:
                                        self.error_logger.info(
                                            f"Attempting to find playerId for "
                                            f"{firstname} {surname} with "
                                            f"squadName '{squad_name}'.")
                                        found_player_id = self.find_player_id(
                                            firstname, surname, squad_name)
                                        if found_player_id:
                                            player_id = str(found_player_id)
                                            self.info_logger.info(
                                                f"Found playerId {player_id} "
                                                f"for {firstname} {surname} "
                                                f"in score flow data.")
                                        else:
                                            self.error_logger.warning(
                                                f"Could not find playerId for "
                                                f"{firstname} {surname} in "
                                                f"score flow data for match "
                                                f"{match_id}. Skipping row.")
                                            continue  # Skip this row
                                    else:
                                        self.error_logger.warning(
                                            f"Missing firstname or surname "
                                            f"for player in score flow data "
                                            f"for match {match_id}. "
                                            f"Skipping row.")
                                        continue  # Skip this row

                                # Generate uniqueMatchId
                                uniqueMatchId = f"{match_id}-{player_id}"

                                # Check if uniqueMatchId was processed
                                if uniqueMatchId not in \
                                        processed_unique_match_ids:
                                    self.error_logger.warning(
                                        f"Match data for uniqueMatchId "
                                        f"{uniqueMatchId} not found. "
                                        f"Skipping score flow data row.")
                                    continue  # Skip this score flow data row

                                row['playerId'] = player_id
                                row['uniqueMatchId'] = uniqueMatchId
                                row['uniquePlayerId'] = f"{player_id}-" \
                                                        f"{squad_id}"
                                row['scoreFlowId'] = score_flow_id

                                # Generate unique IDs
                                uniqueSquadId = f"{squad_id}-{squad_name}"
                                uniqueSportId = sport_info_data[
                                    'uniqueSportId']
                                uniqueFixtureId = f"{fixture_id}-{match_id}"

                                row['uniqueSquadId'] = uniqueSquadId
                                row['uniqueSportId'] = uniqueSportId
                                row['uniqueFixtureId'] = uniqueFixtureId

                                score_flow_data_list.append(row.to_dict())

                print(f"Collected {len(squad_info_list)} squad info entries.")
                print(f"Collected {len(player_info_list)} player info entries.")
                print(f"Collected {len(fixture_data_list)} fixture entries.")
                print(f"Collected {len(match_data_list)} match entries.")
                print(f"Collected {len(period_data_list)} period data entries.")
                print(f"Collected {len(score_flow_data_list)} score flow "
                      f"entries.")

                # Insert data with individual error handling

                # 1. Insert squad info
                for squad_info_data in squad_info_list:
                    print(f"Inserting squad info: {squad_info_data}")
                    try:
                        self.db_helper.insert_data_dynamically(
                            'squad_info', squad_info_data, self.squad_fields)
                    except mysql_error as err:
                        self.error_logger.error(
                            f"MySQL error inserting squad info for fixtureId: "
                            f"{fixture_id}, squadId: "
                            f"{squad_info_data['squadId']}. Error: {err}")
                        self.error_logger.error(
                            f"Data causing error: {squad_info_data}")
                        continue  # Skip this squad_info_data

                # 2. Insert sport info
                print(f"Inserting sport info: {sport_info_data}")
                try:
                    self.db_helper.insert_data_dynamically(
                        'sport_info', sport_info_data, self.sport_fields)
                except mysql_error as err:
                    self.error_logger.error(
                        f"MySQL error inserting sport info for fixtureId: "
                        f"{fixture_id}. Error: {err}")
                    self.error_logger.error(
                        f"Data causing error: {sport_info_data}")
                    self.connection.rollback()
                    self.error_logger.error(
                        f"Transaction rolled back for fixtureId: {fixture_id}")
                    # Add the fixtureId to the broken fixtures list
                    self.add_broken_fixture(fixture_id)
                    continue  # Skip to the next fixture

                # 3. Insert player info
                for player_info_data in player_info_list:
                    print(f"Inserting player info: {player_info_data}")
                    try:
                        self.db_helper.insert_data_dynamically(
                            'player_info', player_info_data, self.player_fields)
                    except mysql_error as err:
                        self.error_logger.error(
                            f"MySQL error inserting player info for "
                            f"playerId: {player_info_data['playerId']}. "
                            f"Error: {err}")
                        self.error_logger.error(
                            f"Data causing error: {player_info_data}")
                        continue  # Skip this player_info_data

                # 4. Insert fixture data
                for fixture_data in fixture_data_list:
                    print(f"Inserting fixture data: {fixture_data}")
                    try:
                        self.db_helper.insert_data_dynamically(
                            fixture_table, fixture_data, self.fixture_fields)
                    except mysql_error as err:
                        self.error_logger.error(
                            f"MySQL error inserting fixture data for "
                            f"uniqueFixtureId: {fixture_data['uniqueFixtureId']}."
                            f" Error: {err}")
                        self.error_logger.error(
                            f"Data causing error: {fixture_data}")
                        continue  # Skip this fixture_data

                # 5. Insert match data
                for match_data in match_data_list:
                    print(f"Inserting match data: {match_data}")
                    try:
                        self.db_helper.insert_data_dynamically(
                            match_table, match_data, self.match_fields)
                    except mysql_error as err:
                        self.error_logger.error(
                            f"MySQL error inserting match data for "
                            f"uniqueMatchId: {match_data['uniqueMatchId']}."
                            f" Error: {err}")
                        self.error_logger.error(
                            f"Data causing error: {match_data}")
                        continue  # Skip this match_data

                # 6. Insert period data
                for period_row in period_data_list:
                    print(f"Inserting period data: {period_row}")
                    try:
                        self.db_helper.insert_data_dynamically(
                            period_table, period_row, self.period_fields)
                    except mysql_error as err:
                        self.error_logger.error(
                            f"MySQL error inserting period data for "
                            f"uniquePeriodId: {period_row['uniquePeriodId']}."
                            f" Error: {err}")
                        self.error_logger.error(
                            f"Data causing error: {period_row}")
                        continue  # Skip this period_row

                # 7. Insert score flow data
                for score_flow_row in score_flow_data_list:
                    print(f"Inserting score flow data: {score_flow_row}")
                    try:
                        self.db_helper.insert_data_dynamically(
                            score_flow_table, score_flow_row,
                            self.score_flow_fields)
                    except mysql_error as err:
                        self.error_logger.error(
                            f"MySQL error inserting score flow data for "
                            f"scoreFlowId: {score_flow_row['scoreFlowId']}."
                            f" Error: {err}")
                        self.error_logger.error(
                            f"Data causing error: {score_flow_row}")
                        continue  # Skip this score_flow_row

                # Commit the transaction after successful batch insertion
                self.connection.commit()
                print(f"Transaction committed successfully for fixtureId: "
                      f"{fixture_id}")

            except mysql_error as err:
                # Log the error and rollback the transaction
                self.error_logger.error(
                    f"MySQL error during transaction for fixtureId: "
                    f"{fixture_id}, leagueId: {league_id}. Error: {err}")
                self.error_logger.error(
                    f"MySQL Error Code: {err.errno}, SQLSTATE: {err.sqlstate}, "
                    f"Message: {err.msg}")
                self.connection.rollback()
                self.error_logger.error(
                    f"Transaction rolled back for fixtureId: {fixture_id}")
                # Add the fixtureId to the broken fixtures list
                self.add_broken_fixture(fixture_id)
                continue  # Skip to the next fixture

            except Exception as e:
                # Log any other exceptions and rollback the transaction
                self.error_logger.error(
                    f"Unexpected error during transaction for fixtureId: "
                    f"{fixture_id}, leagueId: {league_id}. Error: {e}")
                self.error_logger.error(f"Traceback: {traceback.format_exc()}")
                self.connection.rollback()
                self.error_logger.error(
                    f"Transaction rolled back for fixtureId: {fixture_id}")
                # Add the fixtureId to the broken fixtures list
                self.add_broken_fixture(fixture_id)
                continue  # Skip to the next fixture

        # At the end, write the broken fixtures list to the JSON file
        with open(self.broken_fixtures_file, 'w') as f:
            json.dump(self.broken_fixtures, f)
