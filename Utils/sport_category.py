import re
import json
import os

# Get the directory path for the Assets folder
assets_folder = os.path.join(os.path.dirname(__file__), '..', 'Assets/jsons')

# Load the JSON file with the filtering rules from the Assets folder
json_file_path = os.path.join(assets_folder, 'leagues_filter.json')

with open(json_file_path, 'r') as file:
    league_filters = json.load(file)

def determine_sport_category(regulation_periods, squad_ids, league_name, league_id):
    """
    Determine the sport category and corresponding sport ID based on squad ID patterns, league name, and league ID.
    Ensures New Zealand leagues are in NZ folders and AFL is only split into AFL Mens/Womens.
    """
    
    # Initialize sport category and sport_id as None
    sport_category = 'Unknown_Sport'
    sport_id = None
    
    # Strip the year from the league name to prevent interference with filtering
    league_name_cleaned = re.sub(r"\(\d{4}\)", "", league_name).strip()  # Remove years in parentheses, e.g., "(2009)"
    
    # AFL Check
    if "AFL" in league_name_cleaned.upper():
        # Check the squad ID to determine AFL Men's or Women's
        for squad_id in squad_ids:
            str_squad_id = str(squad_id)
            if str_squad_id.startswith('1') or str_squad_id.startswith('9815') or str_squad_id.startswith('9835'):
                return "AFL Mens", 1
            elif str_squad_id.startswith('73') or str_squad_id.startswith('78'):
                return "AFL Womens", 2
    
    # FAST5 Check (prioritize filtering for FAST5 by checking for "FAST")
    if "FAST" in league_name_cleaned.upper():
        # If the league name contains FAST, further filter by SquadID
        for squad_id in squad_ids:
            str_squad_id = str(squad_id)
            if str_squad_id.startswith(('95', '97')):  # FAST5 Men's SquadIDs
                return "FAST5 Mens", 5
            elif str_squad_id.startswith('88'):  # FAST5 Women's SquadIDs 
                return "FAST5 Womens", 6

    # Load league filters from the JSON file
    international_leagues = league_filters["international_leagues"]
    australian_leagues = league_filters["australian_leagues"]
    nz_leagues = league_filters["nz_leagues"]
    afl_mens_leagues = league_filters["afl_mens_leagues"]
    afl_womens_leagues = league_filters["afl_womens_leagues"]
    
    # Check for league name in pre-determined lists (now using cleaned league name)
    if league_name_cleaned in international_leagues:
        return "Netball Womens International", 10  # International folder
    elif league_name_cleaned in australian_leagues:
        return "Netball Womens Australia", 9  # Australian folder (specific Australian rules)
    elif league_name_cleaned in nz_leagues:
        return "Netball Womens NZ", 8  # NZ folder (even for international rules)
    elif league_name_cleaned in afl_mens_leagues:
        return "AFL Mens", 1  # AFL Mens folder
    elif league_name_cleaned in afl_womens_leagues:
        return "AFL Womens", 2  # AFL Womens folder
    
    # Squad ID filtering (fallback if not captured by league filtering)
    
    # Use the existing squad ID filtering mechanism
    for squad_id in squad_ids:
        str_squad_id = str(squad_id)  # Convert squad_id to string for pattern matching
        
        # Specific rules for NRL (we won't touch the NRL filtering since it's 100% working)
        if regulation_periods == 2:
            if str_squad_id.startswith(('3', '81', '74')):
                return 'NRL Mens', 3  # NRL Men's based on squad ID patterns
            elif str_squad_id.startswith(('92', '96', '97')):
                return 'NRL Womens', 4  # NRL Women's based on squad ID patterns

        # Specific rules for Netball
        elif regulation_periods == 4:
            if str_squad_id.startswith('949'):
                return 'Netball Mens', 7  # International & NZ Netball Mens
            
            elif str_squad_id.startswith(('71', '72', '73', '75', '77', '79',)):
                return 'Netball Womens NZ', 8  # NZ Women's Netball
            
            elif str_squad_id.startswith(('78', '80', '81', '91')):
                return 'Netball Womens Australia', 9  # Australian Women's Netball
            
            elif str_squad_id.startswith(('76', '83', '87', '95', '97')):
                return 'Netball Womens International', 10  # International Women's Netball  

    # Fallback if nothing matches
    if regulation_periods == 4:
        return 'Netball Unknown', 11  # General fallback for Netball
    elif regulation_periods == 2:
        return 'NRL Unknown', 12  # General fallback for NRL
    
    # Return the category and ID if all else fails
    return sport_category, sport_id