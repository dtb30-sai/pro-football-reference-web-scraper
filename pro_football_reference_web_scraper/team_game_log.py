# nfl_game_log_scraper.py

import logging
from datetime import date
from haversine import haversine, Unit
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ================================
# Configuration and Setup
# ================================

# Configure logging to capture warnings and errors
logging.basicConfig(
    filename='nfl_game_log_scraper.log',  # Log file name
    level=logging.WARNING,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Mapping of NFL team names to their abbreviations on Pro-Football-Reference
team_hrefs = {
    'Arizona Cardinals': 'crd',
    'Baltimore Colts': 'clt',
    'St. Louis Cardinals': 'crd',
    'Boston Patriots': 'nwe',
    'Chicago Bears': 'chi',
    'Green Bay Packers': 'gnb',
    'New York Giants': 'nyg',
    'Detroit Lions': 'det',
    'Washington Commanders': 'was',
    'Washington Football Team': 'was',
    'Washington Redskins': 'was',
    'Philadelphia Eagles': 'phi',
    'Pittsburgh Steelers': 'pit',
    'Los Angeles Chargers': 'sdg',       # San Diego Chargers (now Los Angeles)
    'San Francisco 49ers': 'sfo',
    'Houston Oilers': 'oti',              # Now Tennessee Titans
    'Cleveland Browns': 'cle',
    'Indianapolis Colts': 'clt',
    'Dallas Cowboys': 'dal',
    'Kansas City Chiefs': 'kan',
    'Los Angeles Rams': 'ram',            # Previously St. Louis Rams
    'Denver Broncos': 'den',
    'New York Jets': 'nyj',
    'New England Patriots': 'nwe',
    'Las Vegas Raiders': 'rai',           # Previously Oakland/San Diego Raiders
    'Tennessee Titans': 'oti',
    'Tennessee Oilers': 'oti',
    'Phoenix Cardinals': 'crd',
    'Buffalo Bills': 'buf',
    'Minnesota Vikings': 'min',
    'Atlanta Falcons': 'atl',
    'Miami Dolphins': 'mia',
    'New Orleans Saints': 'nor',
    'Cincinnati Bengals': 'cin',
    'Seattle Seahawks': 'sea',
    'Tampa Bay Buccaneers': 'tam',
    'Carolina Panthers': 'car',
    'Jacksonville Jaguars': 'jax',
    'Baltimore Ravens': 'rav',
    'Houston Texans': 'htx',
    'Oakland Raiders': 'rai',
    'San Diego Chargers': 'sdg',
    'St. Louis Rams': 'ram',
}

# Mapping of month names to their numerical representations
months = {
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
    "January": 1
}

# Mapping of cities to their geographical data
locations = {
    'Boston': {'latitude': 42.3656, 'longitude': 71.0096, 'airport': 'BOS'},
    'Phoenix': {'latitude': 33.4352, 'longitude': 112.0101, 'airport': 'PHX'},
    'Chicago': {'latitude': 41.9803, 'longitude': 87.9090, 'airport': 'ORD'},
    'Green Bay': {'latitude': 44.4923, 'longitude': 88.1278, 'airport': 'GRB'},
    'New York': {'latitude': 40.6895, 'longitude': 74.1745, 'airport': 'EWR'},
    'Detroit': {'latitude': 42.2162, 'longitude': 83.3554, 'airport': 'DTW'},
    'Washington DC': {'latitude': 38.9531, 'longitude': 77.4565, 'airport': 'IAD'},
    'Philadelphia': {'latitude': 39.9526, 'longitude': 75.1652, 'airport': 'PHL'},
    'Pittsburgh': {'latitude': 40.4919, 'longitude': 80.2352, 'airport': 'PIT'},
    'Los Angeles': {'latitude': 33.9416, 'longitude': 118.4085, 'airport': 'LAX'},
    'San Francisco': {'latitude': 37.3639, 'longitude': 121.9289, 'airport': 'SJC'},
    'Cleveland': {'latitude': 41.4058, 'longitude': 81.8539, 'airport': 'CLE'},
    'Indianapolis': {'latitude': 39.7169, 'longitude': 86.2956, 'airport': 'IND'},
    'Dallas': {'latitude': 32.8998, 'longitude': 97.0403, 'airport': 'DFW'},
    'Kansas City': {'latitude': 39.3036, 'longitude': 94.7093, 'airport': 'MCI'},
    'Denver': {'latitude': 39.8564, 'longitude': 104.6764, 'airport': 'DEN'},
    'Providence': {'latitude': 41.7235, 'longitude': 71.4270, 'airport': 'PVD'},
    'Las Vegas': {'latitude': 36.0840, 'longitude': 115.1537, 'airport': 'LAS'},
    'Nashville': {'latitude': 36.1263, 'longitude': 86.6774, 'airport': 'BNA'},
    'Buffalo': {'latitude': 42.9397, 'longitude': 78.7295, 'airport': 'BUF'},
    'Minneapolis': {'latitude': 44.8848, 'longitude': 93.2223, 'airport': 'MSP'},
    'Atlanta': {'latitude': 33.6407, 'longitude': 84.4277, 'airport': 'ATL'},
    'Miami': {'latitude': 26.0742, 'longitude': 80.1506, 'airport': 'FLL'},
    'New Orleans': {'latitude': 29.9911, 'longitude': 90.2592, 'airport': 'MSY'},
    'Cincinnati': {'latitude': 39.0508, 'longitude': 84.6673, 'airport': 'CVG'},
    'Seattle': {'latitude': 47.4480, 'longitude': 122.3088, 'airport': 'SEA'},
    'Tampa Bay': {'latitude': 27.9772, 'longitude': 82.5311, 'airport': 'TPA'},
    'Charlotte': {'latitude': 35.2144, 'longitude': 80.9473, 'airport': 'CLT'},
    'Jacksonville': {'latitude': 30.4941, 'longitude': 81.6879, 'airport': 'JAX'},
    'Baltimore': {'latitude': 39.1774, 'longitude': 76.6684, 'airport': 'BWI'},
    'Houston': {'latitude': 29.9902, 'longitude': 95.3368, 'airport': 'IAH'},
    'Oakland': {'latitude': 37.7126, 'longitude': 122.2197, 'airport': 'OAK'},
    'San Diego': {'latitude': 32.7338, 'longitude': 117.1933, 'airport': 'SAN'},
    'St. Louis': {'latitude': 38.7499, 'longitude': 90.3748, 'airport': 'STL'},
}

# Mapping of team names to their respective cities
cities = {
    'Arizona Cardinals': 'Phoenix',
    'Chicago Bears': 'Chicago',
    'Green Bay Packers': 'Green Bay',
    'New York Giants': 'New York',
    'Detroit Lions': 'Detroit',
    'Washington Commanders': 'Washington DC',
    'Washington Football Team': 'Washington DC',
    'Washington Redskins': 'Washington DC',
    'Philadelphia Eagles': 'Philadelphia',
    'Pittsburgh Steelers': 'Pittsburgh',
    'Los Angeles Chargers': 'Los Angeles',
    'San Francisco 49ers': 'San Francisco',
    'Houston Oilers': 'Houston',
    'Cleveland Browns': 'Cleveland',
    'Indianapolis Colts': 'Indianapolis',
    'Dallas Cowboys': 'Dallas',
    'Kansas City Chiefs': 'Kansas City',
    'Los Angeles Rams': 'Los Angeles',
    'Denver Broncos': 'Denver',
    'New York Jets': 'New York',
    'New England Patriots': 'Boston',
    'Las Vegas Raiders': 'Las Vegas',
    'Tennessee Titans': 'Nashville',
    'Tennessee Oilers': 'Nashville',
    'Phoenix Cardinals': 'Phoenix',
    'Buffalo Bills': 'Buffalo',
    'Minnesota Vikings': 'Minneapolis',
    'Atlanta Falcons': 'Atlanta',
    'Miami Dolphins': 'Miami',
    'New Orleans Saints': 'New Orleans',
    'Cincinnati Bengals': 'Cincinnati',
    'Seattle Seahawks': 'Seattle',
    'Tampa Bay Buccaneers': 'Tampa Bay',
    'Carolina Panthers': 'Charlotte',
    'Jacksonville Jaguars': 'Jacksonville',
    'Baltimore Ravens': 'Baltimore',
    'Houston Texans': 'Houston',
    'Oakland Raiders': 'Oakland',
    'San Diego Chargers': 'San Diego',
    'St. Louis Rams': 'St. Louis',
    'Baltimore Colts': 'Baltimore',
    'St. Louis Cardinals': 'St. Louis',
}

# ================================
# Helper Functions
# ================================

def safe_int_or_nan(text):
    """
    Safely converts text to integer. Returns NaN if conversion fails.

    Args:
        text (str): The text to convert.

    Returns:
        int or pd.NA: Converted integer or NaN.
    """
    if isinstance(text, str):
        text = text.strip()
        if text.isdigit():
            return int(text)
        elif text == '':
            return pd.NA
        else:
            logging.warning(f"Unexpected value encountered: '{text}'. Setting to NaN.")
            return pd.NA
    else:
        return pd.NA

def calculate_distance(city1: dict, city2: dict) -> float:
    """
    Calculates the haversine distance between two cities.

    Args:
        city1 (dict): Dictionary containing 'latitude' and 'longitude' of the first city.
        city2 (dict): Dictionary containing 'latitude' and 'longitude' of the second city.

    Returns:
        float: Distance in miles between the two cities.
    """
    coordinates1 = (city1['latitude'], city1['longitude'])
    coordinates2 = (city2['latitude'], city2['longitude'])
    return haversine(coordinates1, coordinates2, unit=Unit.MILES)

# ================================
# Main Scraping and Processing
# ================================

def get_team_game_log(team: str, season: int) -> pd.DataFrame:
    """
    Retrieves a team's game log for a given season from Pro-Football-Reference.

    Args:
        team (str): NFL team's full name (case-sensitive).
        season (int): Year of the season.

    Returns:
        pd.DataFrame: DataFrame containing the game log.
    """
    # Validate team name
    if team not in team_hrefs:
        raise ValueError(f"Invalid team name: '{team}'. Please check the team_hrefs mapping.")

    # Make HTTP request
    response = make_request(team, season)

    if response.status_code == 404:
        raise ValueError(f"404 Error: The team '{team}' may not have existed in the {season} season.")

    # Parse HTML
    soup = get_soup(response)

    # Collect and process data
    df = collect_data(soup, season, team)

    # Create 'is_played' indicator
    df['is_played'] = df['points_allowed'].notna().astype(int)

    return df

def make_request(team: str, season: int) -> requests.Response:
    """
    Constructs the URL and makes the HTTP GET request.

    Args:
        team (str): NFL team's full name.
        season (int): Year of the season.

    Returns:
        requests.Response: The HTTP response object.
    """
    team_abbr = team_hrefs.get(team)
    url = f'https://www.pro-football-reference.com/teams/{team_abbr}/{season}.htm'
    return requests.get(url)

def get_soup(response: requests.Response) -> BeautifulSoup:
    """
    Parses the HTML content using BeautifulSoup.

    Args:
        response (requests.Response): The HTTP response object.

    Returns:
        BeautifulSoup: Parsed HTML content.
    """
    return BeautifulSoup(response.text, 'html.parser')

def collect_data(soup: BeautifulSoup, season: int, team: str) -> pd.DataFrame:
    """
    Extracts game data from the parsed HTML and constructs a DataFrame.

    Args:
        soup (BeautifulSoup): Parsed HTML content.
        season (int): Year of the season.
        team (str): NFL team's full name.

    Returns:
        pd.DataFrame: DataFrame containing the game log.
    """
    # Define columns for the DataFrame
    data_columns = [
        'week', 'day', 'rest_days', 'home_team', 'distance_travelled',
        'opp', 'result', 'points_for', 'points_allowed',
        'tot_yds', 'pass_yds', 'rush_yds',
        'opp_tot_yds', 'opp_pass_yds', 'opp_rush_yds'
    ]
    data = {col: [] for col in data_columns}
    df = pd.DataFrame(data)

    # Find all game rows in the second tbody
    # The first tbody might be for regular games; second for playoff or other
    try:
        games = soup.find_all('tbody')[0].find_all('tr')
    except IndexError:
        logging.error("No tbody found in the HTML content.")
        return df

    # Remove playoff games by stopping at the 'Playoffs' marker
    games_cleaned = []
    for game in games:
        game_date = game.find('td', {'data-stat': 'game_date'})
        if game_date and game_date.text.strip() == 'Playoffs':
            break  # Stop processing after playoffs
        games_cleaned.append(game)

    # Remove bye weeks and canceled games
    games_final = []
    for game in games_cleaned:
        opp = game.find('td', {'data-stat': 'opp'})
        boxscore = game.find('td', {'data-stat': 'boxscore_word'})
        if opp and opp.text.strip() == 'Bye Week':
            continue  # Skip bye weeks
        if boxscore and boxscore.text.strip().lower() == 'canceled':
            continue  # Skip canceled games
        games_final.append(game)

    # Process each game
    for game in games_final:
        # Week
        week_text = game.find('th', {'data-stat': 'week_num'})
        week = safe_int_or_nan(week_text.text) if week_text else pd.NA

        # Day of the week
        day = game.find('td', {'data-stat': 'game_day_of_week'})
        day = day.text.strip() if day else ''

        # Game Date
        game_date_text = game.find('td', {'data-stat': 'game_date'})
        game_date = game_date_text.text.strip() if game_date_text else ''

        # Calculate rest_days
        # For the first game, assume a default rest period (e.g., 10 days)
        # For subsequent games, calculate based on the previous game date
        # Note: This requires maintaining state of previous game dates
        # We'll handle this outside the loop for simplicity

        # Opponent
        opp = game.find('td', {'data-stat': 'opp'})
        opp = opp.text.strip() if opp else ''

        # Game Location
        game_location = game.find('td', {'data-stat': 'game_location'})
        is_home = True if game_location and game_location.text.strip() != '@' else False

        # Distance Travelled
        if not is_home:
            # Calculate distance between team's city and opponent's city
            team_city = cities.get(team)
            opp_city = cities.get(opp)
            if not team_city:
                logging.warning(f"Team city not found for team: '{team}'. Setting distance_travelled to NaN.")
                distance_travelled = pd.NA
            elif not opp_city:
                logging.warning(f"Opponent city not found for opponent: '{opp}'. Setting distance_travelled to NaN.")
                distance_travelled = pd.NA
            else:
                city1 = locations.get(team_city)
                city2 = locations.get(opp_city)
                if not city1 or not city2:
                    logging.warning(f"Location data missing for cities: '{team_city}' or '{opp_city}'. Setting distance_travelled to NaN.")
                    distance_travelled = pd.NA
                else:
                    distance_travelled = calculate_distance(city1, city2)
        else:
            distance_travelled = 0.0  # Home games have zero travel distance

        # Result
        result = game.find('td', {'data-stat': 'game_outcome'})
        result = result.text.strip() if result else ''

        # Points For
        pts_off_text = game.find('td', {'data-stat': 'pts_off'})
        points_for = safe_int_or_nan(pts_off_text.text) if pts_off_text else pd.NA

        # Points Allowed
        pts_def_text = game.find('td', {'data-stat': 'pts_def'})
        points_allowed = safe_int_or_nan(pts_def_text.text) if pts_def_text else pd.NA

        # Total Yards
        yards_off_text = game.find('td', {'data-stat': 'yards_off'})
        tot_yds = safe_int_or_nan(yards_off_text.text) if yards_off_text else pd.NA

        # Passing Yards
        pass_yds_off_text = game.find('td', {'data-stat': 'pass_yds_off'})
        pass_yds = safe_int_or_nan(pass_yds_off_text.text) if pass_yds_off_text else pd.NA

        # Rushing Yards
        rush_yds_off_text = game.find('td', {'data-stat': 'rush_yds_off'})
        rush_yds = safe_int_or_nan(rush_yds_off_text.text) if rush_yds_off_text else pd.NA

        # Opponent's Total Yards
        yards_def_text = game.find('td', {'data-stat': 'yards_def'})
        opp_tot_yds = safe_int_or_nan(yards_def_text.text) if yards_def_text else pd.NA

        # Opponent's Passing Yards
        pass_yds_def_text = game.find('td', {'data-stat': 'pass_yds_def'})
        opp_pass_yds = safe_int_or_nan(pass_yds_def_text.text) if pass_yds_def_text else pd.NA

        # Opponent's Rushing Yards
        rush_yds_def_text = game.find('td', {'data-stat': 'rush_yds_def'})
        opp_rush_yds = safe_int_or_nan(rush_yds_def_text.text) if rush_yds_def_text else pd.NA

        # Append data to DataFrame
        df = df.append({
            'week': week,
            'day': day,
            'rest_days': pd.NA,  # To be calculated later
            'home_team': is_home,
            'distance_travelled': distance_travelled,
            'opp': opp,
            'result': result,
            'points_for': points_for,
            'points_allowed': points_allowed,
            'tot_yds': tot_yds,
            'pass_yds': pass_yds,
            'rush_yds': rush_yds,
            'opp_tot_yds': opp_tot_yds,
            'opp_pass_yds': opp_pass_yds,
            'opp_rush_yds': opp_rush_yds,
        }, ignore_index=True)

    # Calculate 'rest_days' based on game dates
    df = calculate_rest_days(df, season, team)

    return df

def calculate_rest_days(df: pd.DataFrame, season: int, team: str) -> pd.DataFrame:
    """
    Calculates the number of rest days between consecutive games.

    Args:
        df (pd.DataFrame): DataFrame containing the game log.
        season (int): Year of the season.
        team (str): NFL team's full name.

    Returns:
        pd.DataFrame: Updated DataFrame with 'rest_days' calculated.
    """
    game_dates = df['game_date'].tolist()
    rest_days = [pd.NA]  # First game has no previous game

    for i in range(1, len(game_dates)):
        prev_game_date = game_dates[i - 1]
        current_game_date = game_dates[i]

        # Parse game dates
        try:
            prev_month, prev_day = prev_game_date.split(' ')
            prev_month_num = months.get(prev_month, pd.NA)
            prev_day_num = int(prev_day)
            if prev_month_num is pd.NA:
                logging.warning(f"Unknown month: '{prev_month}' in previous game date.")
                rest_days.append(pd.NA)
                continue

            current_month, current_day = current_game_date.split(' ')
            current_month_num = months.get(current_month, pd.NA)
            current_day_num = int(current_day)
            if current_month_num is pd.NA:
                logging.warning(f"Unknown month: '{current_month}' in current game date.")
                rest_days.append(pd.NA)
                continue

            # Handle year transition (December to January)
            if prev_month_num == 12 and current_month_num == 1:
                prev_year = season
                current_year = season + 1
            else:
                prev_year = season
                current_year = season

            prev_date = date(prev_year, prev_month_num, prev_day_num)
            current_date = date(current_year, current_month_num, current_day_num)

            delta = current_date - prev_date
            rest_days.append(delta.days)
        except Exception as e:
            logging.error(f"Error parsing game dates: '{prev_game_date}' and '{current_game_date}'. Error: {e}")
            rest_days.append(pd.NA)

    df['rest_days'] = rest_days
    return df

# ================================
# Example Usage
# ================================

def main():
    """
    Main function to execute the game log scraping and processing.
    """
    team = 'Kansas City Chiefs'  # Team name (case-sensitive)
    season = 2024                # Season year

    try:
        # Fetch game log
        game_log_df = get_team_game_log(team, season)
        print(f"Fetched {len(game_log_df)} games for {team} in {season} season.")

        # Save cleaned game log to CSV
        csv_filename = f"{team.replace(' ', '_')}_game_log_{season}.csv"
        game_log_df.to_csv(csv_filename, index=False)
        print(f"Cleaned game log saved to '{csv_filename}'.")

    except ValueError as ve:
        logging.error(ve)
        print(ve)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
