"""
NBA Daily Game Updater
----------------------
Automatically fetches yesterday's games and adds them to the master file.
Designed to run daily via cron to keep your data up-to-date.

Cron setup (runs daily at 6 AM):
0 6 * * * cd /path/to/HoopHub && /usr/bin/python3 daily_updater.py >> logs/updater.log 2>&1

For testing (run every 10 minutes):
*/10 * * * * cd /Users/dylanbarlava/Desktop/CPSC408/HoopHub/daily_updater.py && /usr/bin/python3 daily_updater.py >> logs/updater.log 2>&1
"""

from nba_api.stats.endpoints import leaguegamefinder, scoreboardv2
import pandas as pd
import mysql.connector
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os

# ==================== CONFIGURATION ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'CPSC408!',
    'database': 'HoopHub'
}

SEASON = "2025-26"
TIMEZONE = ZoneInfo("America/Los_Angeles")
OUTPUT_DIR = "historical_data"
MASTER_FILE = f"{OUTPUT_DIR}/nba_season_{SEASON.replace('-', '_')}_master.csv"

# Retry settings for API calls
MAX_RETRIES = 3
API_TIMEOUT = 15

# ==================== SETUP ====================
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)  # Create logs directory

now = datetime.now(TIMEZONE)
timestamp = now.strftime("%Y%m%d_%H%M%S")

# Calculate yesterday's date
yesterday = now - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# ==================== LOAD EXISTING DATA ====================
existing_df = None
existing_games = set()

if os.path.exists(MASTER_FILE):
    try:
        existing_df = pd.read_csv(MASTER_FILE)
        # Ensure existing game IDs are properly formatted with leading zeros
        existing_df['API_GAME_ID'] = existing_df['API_GAME_ID'].astype(str).str.zfill(10)
        existing_games = set(existing_df['API_GAME_ID'].tolist())
    except Exception as e:
        existing_df = None
else:
    exit(1)

# ==================== CONNECT TO DATABASE ====================
try:
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()
except Exception as e:
    exit(1)

# ==================== BUILD TEAM ID MAPPING ====================
cursor.execute("SELECT TeamID, Abbreviation FROM Team ORDER BY TeamID")
db_teams = cursor.fetchall()

team_id_map = {}
team_abbr_to_db_id = {abbr: team_id for team_id, abbr in db_teams}

# ==================== BUILD VENUE ID MAPPING ====================
cursor.execute("SELECT VenueID, Name FROM Venue ORDER BY VenueID")
db_venues = cursor.fetchall()

venue_name_to_id = {name.strip(): venue_id for venue_id, name in db_venues}

# ==================== FETCH YESTERDAY'S GAMES FROM NBA API ====================
try:
    all_games = leaguegamefinder.LeagueGameFinder(
        season_nullable=SEASON,
        league_id_nullable="00"
    ).get_data_frames()[0].copy()
    
    if all_games.empty:
        all_games = pd.DataFrame()
    else:
        # Filter to only yesterday's games
        all_games = all_games[all_games["GAME_DATE"] == yesterday_str]
    
except Exception as e:
    cursor.close()
    db.close()
    exit(1)

# ==================== PROCESS GAMES ====================
rows = []
vs_pat = re.compile(r"\svs\.?\s", flags=re.IGNORECASE)
at_pat = re.compile(r"\s@\s")
skipped_count = 0
duplicate_count = 0

if not all_games.empty:
    for gid, grp in all_games.groupby("GAME_ID"):
        # Format game ID consistently with leading zeros
        formatted_gid = str(gid).zfill(10)
        
        # Skip if already in existing data
        if formatted_gid in existing_games:
            duplicate_count += 1
            continue
            
        if len(grp) < 2:
            continue
        
        # Determine home vs away rows
        home_cand = grp[grp["MATCHUP"].str.contains(vs_pat, na=False)]
        away_cand = grp[grp["MATCHUP"].str.contains(at_pat, na=False)]
        
        if len(home_cand) == 1 and len(away_cand) == 1:
            home_row = home_cand.iloc[0]
            away_row = away_cand.iloc[0]
        else:
            r0, r1 = grp.iloc[0], grp.iloc[1]
            if "@" in str(r0["MATCHUP"]):
                away_row, home_row = r0, r1
            elif "vs" in str(r0["MATCHUP"]).lower():
                home_row, away_row = r0, r1
            else:
                continue
        
        # Map team IDs
        home_abbr = str(home_row["TEAM_ABBREVIATION"]).strip()
        away_abbr = str(away_row["TEAM_ABBREVIATION"]).strip()
        
        db_home_id = team_abbr_to_db_id.get(home_abbr)
        db_away_id = team_abbr_to_db_id.get(away_abbr)
        
        if not db_home_id or not db_away_id:
            skipped_count += 1
            continue
        
        # Store NBA API to DB mapping
        team_id_map[int(home_row["TEAM_ID"])] = db_home_id
        team_id_map[int(away_row["TEAM_ID"])] = db_away_id
        
        # Create game record
        game_record = {
            "API_GAME_ID": formatted_gid,
            "GAME_DATE": str(home_row["GAME_DATE"]),
            "HOME_TEAM_ID": db_home_id,
            "AWAY_TEAM_ID": db_away_id,
            "HOME_TEAM": str(home_row["TEAM_NAME"]).strip(),
            "AWAY_TEAM": str(away_row["TEAM_NAME"]).strip(),
            "HOME_ABBR": home_abbr,
            "AWAY_ABBR": away_abbr,
            "HOME_SCORE": int(home_row["PTS"]) if pd.notna(home_row.get("PTS")) else None,
            "AWAY_SCORE": int(away_row["PTS"]) if pd.notna(away_row.get("PTS")) else None,
        }
        
        rows.append(game_record)

# ==================== FETCH ARENA & ATTENDANCE WITH RETRY ====================
if rows:
    arena_map = {}
    attendance_map = {}
    venue_id_map = {}

    success = False
    for attempt in range(MAX_RETRIES):
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=yesterday_str, timeout=API_TIMEOUT)
            gh = sb.game_header.get_data_frame()
            
            if gh.empty:
                success = True
                break
            
            found = 0
            if "GAME_ID" in gh.columns:
                for _, ghr in gh.iterrows():
                    gid = str(ghr.get("GAME_ID"))
                    
                    if "ARENA_NAME" in gh.columns:
                        arena = str(ghr.get("ARENA_NAME") or "").strip() or None
                        if arena:
                            arena_map[gid] = arena
                            # Look up venue ID from database
                            venue_id = venue_name_to_id.get(arena)
                            if venue_id:
                                venue_id_map[gid] = venue_id
                            found += 1
                    
                    if "ATTENDANCE" in gh.columns:
                        att = ghr.get("ATTENDANCE")
                        if pd.notna(att) and att > 0:
                            attendance_map[gid] = int(att)
            
            success = True
            break
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            else:
                pass

    # Add arena, venue_id & attendance to all rows
    for r in rows:
        gid = r["API_GAME_ID"]
        r["ARENA"] = arena_map.get(gid)
        r["VENUE_ID"] = venue_id_map.get(gid)
        r["ATTENDANCE"] = attendance_map.get(gid)

# ==================== MERGE WITH EXISTING DATA ====================
if rows:
    new_df = pd.DataFrame(rows)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['API_GAME_ID'], keep='last')
    combined_df = combined_df.sort_values(['GAME_DATE', 'HOME_TEAM'])
else:
    combined_df = existing_df

# ==================== SAVE & FINAL OUTPUT ====================
new_games_added = len(rows)
combined_df.to_csv(MASTER_FILE, index=False)

print("="*80)
print("DAILY UPDATE COMPLETE!")
print("="*80)
if new_games_added > 0:
    print(f"✓ Added {new_games_added} game(s) at {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')}")
else:
    print(f"No new games added at {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')}")
print("="*80 + "\n")

# ==================== CLEANUP ====================
cursor.close()
db.close()