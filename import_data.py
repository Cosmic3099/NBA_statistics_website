from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests import Session

# Set up a requests Session with timeout
session = Session()

# Function to fetch player stats
def fetch_player_career_stats(player):
    try:
        # Fetch player stats from nba_api
        player_id = player['id']
        player_name = player['full_name']
        
        # Call NBA API to get player career stats, adding timeout to the request
        career = playercareerstats.PlayerCareerStats(player_id=player_id, timeout=1000)  # Timeout set to 1000 seconds
        career_df = career.get_data_frames()[0]
        
        # Calculate career averages for basic stats
        if career_df.empty:
            return None
        
        # Calculate the player's career stats (points, rebounds, assists, etc.)
        total_games = career_df['GP'].sum()
        career_stats = {
            'name': player_name,
            'games_played': total_games,
            'points_per_game': career_df['PTS'].sum() / total_games,
            'rebounds_per_game': career_df['REB'].sum() / total_games,
            'assists_per_game': career_df['AST'].sum() / total_games,
            'steals_per_game': career_df['STL'].sum() / total_games,
            'blocks_per_game': career_df['BLK'].sum() / total_games,
            'turnovers_per_game': career_df['TOV'].sum() / total_games,
            'field_goal_pct': career_df['FG_PCT'].mean(),
            'three_point_pct': career_df['FG3_PCT'].mean(),
            'free_throw_pct': career_df['FT_PCT'].mean()
        }
        
        # Add a small sleep to avoid triggering rate limits (optional)
        time.sleep(0.5)
        
        return career_stats
    
    except Exception as e:
        print(f"Error fetching data for {player['full_name']}: {e}")
        return None

# Function to use multithreading for fetching data
def fetch_all_players_concurrently(players_list, max_workers=10):
    player_stats = []
    
    # Use ThreadPoolExecutor for concurrent fetching
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks to fetch player data concurrently
        futures = {executor.submit(fetch_player_career_stats, player): player for player in players_list}
        
        # Collect results as they complete
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                player_stats.append(result)
    
    return player_stats

# Get all players (current and historical)
all_players = players.get_players()

# Fetch player stats concurrently for all players
player_data = fetch_all_players_concurrently(all_players, max_workers=15)  # Fetch stats with 15 concurrent workers

# Convert to DataFrame for easy viewing and save to CSV
player_df = pd.DataFrame(player_data)
print(player_df.head())

# Save the processed data to a CSV file
player_df.to_csv('nba_player_career_stats.csv', index=False)