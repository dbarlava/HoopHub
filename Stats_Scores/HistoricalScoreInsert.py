"""
NBA Season Historical Data Fetcher - Database Insert Mode
----------------------------------------------------------
Fetches NBA games for a specified date range and inserts into database.
Re-run with different dates to build up your complete dataset.

Usage:
1. Set SEASON_START_DATE and SEASON_END_DATE below
2. Run the script
3. Change the dates and run again - new data will be added to database
"""

from nba_api.stats.endpoints import leaguegamefinder, scoreboardv2
import pandas as pd
import mysql.connector
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

# ==================== CONFIGURATION ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'CPSC408!',
    'database': 'HoopHub'
}

SEASON = "2025-26"

# ==================== SET YOUR DATE RANGE HERE ====================
SEASON_START_DATE = "2025-11-08"  # Change this to fetch different dates
SEASON_END_DATE = "2025-11-09"    # Change this to fetch different dates

TIMEZONE = ZoneInfo("America/Los_Angeles")

# Retry settings for API calls
MAX_RETRIES = 3
API_TIMEOUT = 15

# ==================== SETUP ====================
now = datetime.now(TIMEZONE)

print("="*80)
print(f"NBA Historical Data Fetcher - {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print("="*80)
print(f"Season: {SEASON}")
print(f"Fetching Date Range: {SEASON_START_DATE} to {SEASON_END_DATE}")
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

team_abbr_to_db_id = {abbr: team_id for team_id, abbr in db_teams}

print(f"✓ Loaded {len(team_abbr_to_db_id)} teams from database\n")

# ==================== BUILD VENUE ID MAPPING ====================
print("Building venue ID mapping...")
cursor.execute("SELECT VenueID, Name FROM Venue ORDER BY VenueID")
db_venues = cursor.fetchall()

venue_name_to_id = {name.strip(): venue_id for venue_id, name in db_venues}

print(f"✓ Loaded {len(venue_name_to_id)} venues from database")
print(f"   Sample venues: {list(venue_name_to_id.keys())[:3]}\n")

# ==================== GET EXISTING GAMES FROM DATABASE ====================
print("Checking for existing games in database...")
cursor.execute("SELECT GameID FROM Game")
existing_game_ids = {row[0] for row in cursor.fetchall()}
print(f"✓ Found {len(existing_game_ids)} existing games in database\n")

# ==================== FETCH GAMES FROM NBA API ====================
print(f"Fetching games from NBA API for {SEASON_START_DATE} to {SEASON_END_DATE}...")
try:
    all_games = leaguegamefinder.LeagueGameFinder(
        season_nullable=SEASON,
        league_id_nullable="00"
    ).get_data_frames()[0].copy()
    
    if all_games.empty:
        print(f"⚠️  No games returned for season {SEASON}")
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
games_to_insert = []
vs_pat = re.compile(r"\svs\.?\s", flags=re.IGNORECASE)
at_pat = re.compile(r"\s@\s")
skipped_count = 0
duplicate_count = 0

if not all_games.empty:
    for gid, grp in all_games.groupby("GAME_ID"):
        # Convert API Game ID to our GameID (remove leading zero if present)
        game_id = int(str(gid).lstrip('0')) if str(gid) != '0' else 0
        
        # Skip if already in database
        if game_id in existing_game_ids:
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
        
        # Create game record with placeholder venue
        game_record = {
            "game_id": game_id,
            "game_date": str(home_row["GAME_DATE"]),
            "home_team_id": db_home_id,
            "away_team_id": db_away_id,
            "home_score": int(home_row["PTS"]) if pd.notna(home_row.get("PTS")) else None,
            "away_score": int(away_row["PTS"]) if pd.notna(away_row.get("PTS")) else None,
            "venue_id": None,  # Will be updated later
            "attendance": None  # Will be updated later
        }
        
        games_to_insert.append(game_record)

print(f"✓ Processed {len(games_to_insert)} new NBA games")
if duplicate_count > 0:
    print(f"⏭️  Skipped {duplicate_count} games (already in database)")
if skipped_count > 0:
    print(f"⚠️  Skipped {skipped_count} non-NBA games (G-League, etc.)")
print()

# ==================== FETCH ARENA & ATTENDANCE WITH RETRY ====================
if games_to_insert:
    print(f"Fetching arena and attendance data (timeout: {API_TIMEOUT}s, retries: {MAX_RETRIES})...")
    dates = sorted({g["game_date"] for g in games_to_insert})
    venue_id_map = {}
    attendance_map = {}

    for d in dates:
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    print(f"  Retry {attempt}/{MAX_RETRIES-1} for {d}...", end=" ")
                else:
                    print(f"  Checking {d}...", end=" ")
                    
                sb = scoreboardv2.ScoreboardV2(game_date=d, timeout=API_TIMEOUT)
                gh = sb.game_header.get_data_frame()
                
                if gh.empty:
                    print("No data")
                    success = True
                    break
                
                found = 0
                if "GAME_ID" in gh.columns:
                    for _, ghr in gh.iterrows():
                        api_gid = str(ghr.get("GAME_ID"))
                        game_id = int(api_gid.lstrip('0')) if api_gid != '0' else 0
                        
                        if "ARENA_NAME" in gh.columns:
                            arena = str(ghr.get("ARENA_NAME") or "").strip() or None
                            if arena:
                                # Look up venue ID from database
                                venue_id = venue_name_to_id.get(arena)
                                if venue_id:
                                    venue_id_map[game_id] = venue_id
                                    found += 1
                        
                        if "ATTENDANCE" in gh.columns:
                            att = ghr.get("ATTENDANCE")
                            if pd.notna(att) and att > 0:
                                attendance_map[game_id] = int(att)
                
                print(f"Found {found} games")
                success = True
                break
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f"Failed ({e}), retrying...")
                    time.sleep(2)
                else:
                    print(f"Failed after {MAX_RETRIES} attempts: {e}")
        
        if success:
            time.sleep(0.1)

    print(f"\n✓ Matched venue IDs for {len(venue_id_map)} games")
    print(f"✓ Fetched attendance for {len(attendance_map)} games\n")

    # Update games with venue_id and attendance
    for game in games_to_insert:
        game["venue_id"] = venue_id_map.get(game["game_id"])
        game["attendance"] = attendance_map.get(game["game_id"])

# ==================== INSERT INTO DATABASE ====================
if games_to_insert:
    print("Inserting games into database...")
    
    # Get the default venue ID (first venue in the table) for games without a matched venue
    cursor.execute("SELECT VenueID FROM Venue ORDER BY VenueID LIMIT 1")
    default_venue_id = cursor.fetchone()[0]
    
    insert_query = """
        INSERT INTO Game (GameID, Date, HomeTeamID, AwayTeamID, VenueID, HomeTeamScore, AwayTeamScore, Attendance)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    inserted_count = 0
    failed_count = 0
    games_without_venue = 0
    
    for game in games_to_insert:
        try:
            # Use default venue if no venue was matched
            venue_id = game["venue_id"] if game["venue_id"] is not None else default_venue_id
            if game["venue_id"] is None:
                games_without_venue += 1
            
            cursor.execute(insert_query, (
                game["game_id"],
                game["game_date"],
                game["home_team_id"],
                game["away_team_id"],
                venue_id,
                game["home_score"],
                game["away_score"],
                game["attendance"]
            ))
            inserted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to insert game {game['game_id']}: {e}")
            failed_count += 1
    
    db.commit()
    
    print(f"✓ Successfully inserted {inserted_count} games")
    if games_without_venue > 0:
        print(f"⚠️  {games_without_venue} games used default venue (arena not matched)")
    if failed_count > 0:
        print(f"✗ Failed to insert {failed_count} games")
    print()

# ==================== SUMMARY ====================
cursor.execute("SELECT COUNT(*) FROM Game")
total_games = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM Game WHERE HomeTeamScore IS NOT NULL")
games_with_scores = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM Game WHERE Attendance IS NOT NULL")
games_with_attendance = cursor.fetchone()[0]

print("="*80)
print("SUMMARY")
print("="*80)
print(f"New games inserted: {inserted_count if games_to_insert else 0}")
print(f"Total games in database: {total_games}")
if total_games > 0:
    print(f"Games with scores: {games_with_scores} ({games_with_scores/total_games*100:.1f}%)")
    print(f"Games with attendance: {games_with_attendance} ({games_with_attendance/total_games*100:.1f}%)")
print()

print("="*80)
print("COMPLETE!")
print("="*80)
print(f"Fetched games from {SEASON_START_DATE} to {SEASON_END_DATE}")
print(f"Database now contains {total_games} total games")
print()
print("Next steps:")
print("1. Query the Game table to verify data quality")
print("2. To fetch more dates, change SEASON_START_DATE and SEASON_END_DATE and run again")
print("3. The script will automatically skip games already in the database")
print()

# ==================== CLEANUP ====================
cursor.close()
db.close()