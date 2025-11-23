"""
NBA Daily Player Stats Updater - Database Only
----------------------------------------------
Fetches player stats from yesterday and inserts into PlayerGameStats table.

Designed to run daily via cron.
Cron setup (runs daily at 6 AM):
0 6 * * * cd /Users/dylanbarlava/Desktop/CPSC408/HoopHub && /opt/homebrew/bin/python3 DailyPlayerStatsInsert.py >> logs/stats_updater.log 2>&1
"""
from nba_api.stats.endpoints import playergamelogs
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ==================== CONFIGURATION ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'CPSC408!',
    'database': 'HoopHub'
}

SEASON = "2025-26"
TIMEZONE = ZoneInfo("America/Los_Angeles")

# ==================== SETUP ====================
now = datetime.now(TIMEZONE)
yesterday = now - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# ==================== CONNECT TO DATABASE ====================
try:
    db = mysql.connector.connect(**DB_CONFIG)
    db.autocommit = False
    cursor = db.cursor()
except Exception as e:
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - Database connection failed: {e}")
    exit(1)

# ==================== BUILD PLAYER MAPPING ====================
cursor.execute("""
    SELECT p.PlayerID, p.FirstName, p.LastName
    FROM Player p
""")
db_players = cursor.fetchall()

player_name_to_db_id = {}
for player_id, first, last in db_players:
    full_name = f"{first} {last}".strip().lower()
    player_name_to_db_id[full_name] = player_id

# ==================== GET EXISTING STATS FROM DATABASE ====================
cursor.execute("SELECT GameID, PlayerID FROM PlayerGameStats")
existing_stats = {(row[0], row[1]) for row in cursor.fetchall()}

# ==================== FETCH YESTERDAY'S STATS FROM API ====================
try:
    logs = playergamelogs.PlayerGameLogs(
        season_nullable=SEASON,
        season_type_nullable='Regular Season'
    )
    
    df = logs.get_data_frames()[0]
    
    if df.empty:
        cursor.close()
        db.close()
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - No stats available, inserted 0 entries")
        exit(0)
    
    # Convert GAME_DATE and filter to yesterday only
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE']).dt.date.astype(str)
    df_yesterday = df[df['GAME_DATE'] == yesterday_str].copy()
    
    if df_yesterday.empty:
        cursor.close()
        db.close()
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - No games on {yesterday_str}, inserted 0 entries")
        exit(0)
        
except Exception as e:
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - API fetch failed: {e}")
    cursor.close()
    db.close()
    exit(1)

# ==================== MAP PLAYER NAMES TO DB PLAYER IDS ====================
df_yesterday['PLAYER_NAME_LOWER'] = df_yesterday['PLAYER_NAME'].str.strip().str.lower()
df_yesterday['PlayerID'] = df_yesterday['PLAYER_NAME_LOWER'].map(player_name_to_db_id)

# Track unmapped players
unmapped_players = df_yesterday[df_yesterday['PlayerID'].isna()]['PLAYER_NAME'].unique()
if len(unmapped_players) > 0:
    for player_name in unmapped_players:
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - Missing player in database: {player_name}")
    # Treat missing players as a hard error: roll back and exit with no inserts
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR: One or more players from yesterday's games are not in the Player table. Rolling back, no stats inserted.")
    db.rollback()
    cursor.close()
    db.close()
    exit(1)

# Remove unmapped players (defensive, in case you later relax the rollback behavior)
df_yesterday = df_yesterday[df_yesterday['PlayerID'].notna()]

# Convert GameID format
df_yesterday['GameID'] = df_yesterday['GAME_ID'].astype(str).str.lstrip('0')
df_yesterday['GameID'] = pd.to_numeric(df_yesterday['GameID'], errors='coerce').astype('Int64')

# ==================== INSERT INTO DATABASE ====================
insert_query = """
    INSERT INTO PlayerGameStats (GameID, PlayerID, Points, Rebounds, Assists, Steals, Blocks, Turnovers, Fouls, Minutes)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

inserted_count = 0
skipped_count = 0

try:
    for _, row in df_yesterday.iterrows():
        # Parse IDs; if they are bad, treat as an error and abort
        game_id = int(row['GameID'])
        player_id = int(row['PlayerID'])

        # Skip if already exists in database
        if (game_id, player_id) in existing_stats:
            skipped_count += 1
            continue

        cursor.execute(insert_query, (
            game_id,
            player_id,
            int(row.get('PTS', 0)) if pd.notna(row.get('PTS')) else 0,
            int(row.get('REB', 0)) if pd.notna(row.get('REB')) else 0,
            int(row.get('AST', 0)) if pd.notna(row.get('AST')) else 0,
            int(row.get('STL', 0)) if pd.notna(row.get('STL')) else 0,
            int(row.get('BLK', 0)) if pd.notna(row.get('BLK')) else 0,
            int(row.get('TOV', 0)) if pd.notna(row.get('TOV')) else 0,
            int(row.get('PF', 0)) if pd.notna(row.get('PF')) else 0,
            int(row.get('MIN', 0)) if pd.notna(row.get('MIN')) else 0
        ))
        inserted_count += 1

    # If we reach here, all inserts succeeded
    db.commit()
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - Inserted {inserted_count} entries to database (skipped {skipped_count} already-existing rows)")

except Exception as e:
    # Any error during insert: roll back everything
    db.rollback()
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR inserting player stats, rolled back transaction. No stats were inserted.")
    print(f"Details: {e}")

# ==================== CLEANUP ====================
cursor.close()
db.close()