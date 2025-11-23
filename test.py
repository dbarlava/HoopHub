"""
NBA Season Historical Data Fetcher
-----------------------------------
Fetches all NBA games from the start of the season up to November 6, 2025.
This is a one-time historical data pull. Use the separate daily updater for ongoing updates.

Run once to populate your database with historical game data.
"""

from nba_api.stats.endpoints import leaguegamefinder, scoreboardv2
import pandas as pd
import mysql.connector
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import os

# ==================== CONFIGURATION ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'CPSC408!',
    'database': 'HoopHub'
}

SEASON = "2024-25"
SEASON_START_DATE = "2025-10-21"  # Season start
SEASON_END_DATE = "2025-11-06"    # Fixed end date - November 6
TIMEZONE = ZoneInfo("America/Los_Angeles")
OUTPUT_DIR = "historical_data"  # Directory to save CSV files

# ==================== SETUP ====================
# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Get current timestamp for filenames
now = datetime.now(TIMEZONE)
timestamp = now.strftime("%Y%m%d_%H%M%S")

print("="*80)
print(f"NBA Historical Data Fetcher - {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print("="*80)
print(f"Season: {SEASON}")
print(f"Date Range: {SEASON_START_DATE} to {SEASON_END_DATE}")
print(f"Output Directory: {OUTPUT_DIR}/")
print("="*80 + "\n")

# ==================== CONNECT TO DATABASE ====================
print("Connecting to database...")
try:
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()
    print("✓ Database connected\n")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
    exit(1)

# ==================== BUILD TEAM ID MAPPING ====================
print("Building team ID mapping...")
cursor.execute("SELECT TeamID, Abbreviation FROM Team ORDER BY TeamID")
db_teams = cursor.fetchall()

team_id_map = {}  # NBA_API_ID -> Your_DB_ID
team_abbr_to_db_id = {abbr: team_id for team_id, abbr in db_teams}

print(f"✓ Loaded {len(team_abbr_to_db_id)} teams from database\n")

# ==================== FETCH GAMES FROM NBA API ====================
print(f"Fetching games from NBA API for season {SEASON}...")
try:
    all_games = leaguegamefinder.LeagueGameFinder(
        season_nullable=SEASON,
        league_id_nullable="00"  # NBA only (excludes G-League)
    ).get_data_frames()[0].copy()
    
    if all_games.empty:
        print(f"⚠️  No games returned for season {SEASON}")
        print(f"   This might be normal if the season hasn't started yet")
        all_games = pd.DataFrame()
    else:
        # Filter by date window
        all_games = all_games[(all_games["GAME_DATE"] >= SEASON_START_DATE) & 
                              (all_games["GAME_DATE"] <= SEASON_END_DATE)]
        
        print(f"✓ Found {len(all_games)} game records (2 per game)\n")
    
except Exception as e:
    print(f"✗ Error fetching games: {e}")
    cursor.close()
    db.close()
    exit(1)

# ==================== PROCESS GAMES ====================
print("Processing games...")
rows = []
vs_pat = re.compile(r"\svs\.?\s", flags=re.IGNORECASE)
at_pat = re.compile(r"\s@\s")
skipped_count = 0

if not all_games.empty:
    for gid, grp in all_games.groupby("GAME_ID"):
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
        
        # Store NBA API to DB mapping for reference
        team_id_map[int(home_row["TEAM_ID"])] = db_home_id
        team_id_map[int(away_row["TEAM_ID"])] = db_away_id
        
        # Create game record
        game_record = {
            "API_GAME_ID": str(gid),
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

if rows:
    print(f"✓ Processed {len(rows)} NBA games")
    if skipped_count > 0:
        print(f"⚠️  Skipped {skipped_count} non-NBA games (G-League, etc.)\n")
    else:
        print()
else:
    print(f"⚠️  No games found for the specified date range")
    print(f"   This is normal if no games were played during this period\n")

# ==================== FETCH ARENA & ATTENDANCE ====================
if rows:
    print("Fetching arena and attendance data...")
    dates = sorted({r["GAME_DATE"] for r in rows})
    arena_map = {}
    attendance_map = {}

    for d in dates:
        try:
            print(f"  Checking {d}...", end=" ")
            sb = scoreboardv2.ScoreboardV2(game_date=d, timeout=8)
            gh = sb.game_header.get_data_frame()
            
            if gh.empty:
                print("No data")
                continue
            
            found = 0
            if "GAME_ID" in gh.columns:
                for _, ghr in gh.iterrows():
                    gid = str(ghr.get("GAME_ID"))
                    
                    if "ARENA_NAME" in gh.columns:
                        arena = str(ghr.get("ARENA_NAME") or "").strip() or None
                        if arena:
                            arena_map[gid] = arena
                            found += 1
                    
                    if "ATTENDANCE" in gh.columns:
                        att = ghr.get("ATTENDANCE")
                        if pd.notna(att) and att > 0:
                            attendance_map[gid] = int(att)
            
            print(f"Found {found} games")
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error: {e}")

    print(f"\n✓ Fetched arena for {len(arena_map)} games")
    print(f"✓ Fetched attendance for {len(attendance_map)} games\n")

    # Add arena & attendance to all rows
    for r in rows:
        gid = r["API_GAME_ID"]
        r["ARENA"] = arena_map.get(gid)
        r["ATTENDANCE"] = attendance_map.get(gid)

# ==================== CREATE DATAFRAME ====================
if rows:
    df = pd.DataFrame(rows)
    df = df.sort_values(['GAME_DATE', 'HOME_TEAM'])
else:
    # Create empty dataframe with proper columns
    df = pd.DataFrame(columns=[
        "API_GAME_ID", "GAME_DATE", "HOME_TEAM_ID", "AWAY_TEAM_ID",
        "HOME_TEAM", "AWAY_TEAM", "HOME_ABBR", "AWAY_ABBR",
        "HOME_SCORE", "AWAY_SCORE", "ARENA", "ATTENDANCE"
    ])

# ==================== SUMMARY STATISTICS ====================
total_games = len(df)
games_with_scores = df['HOME_SCORE'].notna().sum()
games_with_arena = df['ARENA'].notna().sum()
games_with_attendance = df['ATTENDANCE'].notna().sum()

print("="*80)
print("SUMMARY")
print("="*80)
print(f"Total games: {total_games}")
if total_games > 0:
    print(f"Games with scores: {games_with_scores} ({games_with_scores/total_games*100:.1f}%)")
    print(f"Games with arena: {games_with_arena} ({games_with_arena/total_games*100:.1f}%)")
    print(f"Games with attendance: {games_with_attendance} ({games_with_attendance/total_games*100:.1f}%)")
    print()
    
    # Show date range of games
    print(f"Date range: {df['GAME_DATE'].min()} to {df['GAME_DATE'].max()}")
    print(f"Teams represented: {df['HOME_ABBR'].nunique()} unique teams")
else:
    print("No games found for the specified date range")
print()

# ==================== SAVE TO CSV ====================
# Save with timestamp
csv_filename = f"{OUTPUT_DIR}/nba_historical_{SEASON.replace('-', '_')}_{SEASON_START_DATE}_to_{SEASON_END_DATE}_{timestamp}.csv"
df.to_csv(csv_filename, index=False)
print(f"✓ Historical data saved to: {csv_filename}")

# Also save as a simple named file
simple_filename = f"{OUTPUT_DIR}/nba_historical_{SEASON.replace('-', '_')}_through_nov6.csv"
df.to_csv(simple_filename, index=False)
print(f"✓ Also saved as: {simple_filename}")

# Save team mapping for reference
if team_id_map:
    mapping_df = pd.DataFrame([
        {'NBA_API_TeamID': nba_id, 'Your_DB_TeamID': db_id}
        for nba_id, db_id in team_id_map.items()
    ]).sort_values('Your_DB_TeamID')
    mapping_filename = f"{OUTPUT_DIR}/team_mapping_{timestamp}.csv"
    mapping_df.to_csv(mapping_filename, index=False)
    print(f"✓ Team mapping saved to: {mapping_filename}")

print("\n" + "="*80)
print("HISTORICAL DATA FETCH COMPLETE!")
print("="*80)
print(f"Fetched all NBA games from {SEASON_START_DATE} to {SEASON_END_DATE}")
print(f"Total games retrieved: {total_games}")
print()
print("Next steps:")
print("1. Review the CSV file to verify data quality")
print("2. Import this data into your database")
print("3. Use the separate daily updater script for ongoing updates")
print()

# ==================== CLEANUP ====================
cursor.close()
db.close()