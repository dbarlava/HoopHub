"""
Daily NBA Game Fetcher - Database Insert Mode
----------------------------------------------
Fetches yesterday's NBA games and inserts into database.
Now includes proper venue lookup like the historical script.
"""

from nba_api.stats.endpoints import leaguegamefinder, scoreboardv2
import pandas as pd
import mysql.connector
import re
import time
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

# Retry settings for API calls
MAX_RETRIES = 3
API_TIMEOUT = 15

# ==================== SETUP ====================
now = datetime.now(TIMEZONE)

# Calculate yesterday's date
yesterday = now - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"Looking for games from: {yesterday_str}")
print("-" * 60)

# ==================== CONNECT TO DATABASE ====================
try:
    db = mysql.connector.connect(**DB_CONFIG)
    db.autocommit = False
    cursor = db.cursor()
    print("✓ Connected to database")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
    exit(1)

# ==================== BUILD TEAM ID MAPPING ====================
cursor.execute("SELECT TeamID, Abbreviation FROM Team ORDER BY TeamID")
db_teams = cursor.fetchall()
team_abbr_to_db_id = {abbr: team_id for team_id, abbr in db_teams}
print(f"✓ Loaded {len(team_abbr_to_db_id)} teams")

# ==================== BUILD VENUE ID MAPPING ====================
cursor.execute("SELECT VenueID, Name FROM Venue ORDER BY VenueID")
db_venues = cursor.fetchall()
venue_name_to_id = {name.strip(): venue_id for venue_id, name in db_venues}
print(f"✓ Loaded {len(venue_name_to_id)} venues")

# ==================== GET EXISTING GAMES FROM DATABASE ====================
cursor.execute("SELECT GameID FROM Game")
existing_game_ids = {row[0] for row in cursor.fetchall()}
print(f"✓ Found {len(existing_game_ids)} existing games in database")

# ==================== FETCH YESTERDAY'S GAMES FROM NBA API ====================
print(f"\nFetching games from NBA API for {yesterday_str}...")
try:
    all_games = leaguegamefinder.LeagueGameFinder(
        season_nullable=SEASON,
        league_id_nullable="00"
    ).get_data_frames()[0].copy()
    
    print(f"✓ API returned {len(all_games)} total game records")
    
    if all_games.empty:
        print("⚠️  No games in API response")
        all_games = pd.DataFrame()
    else:
        # Show date range in API data
        print(f"  Date range in API: {all_games['GAME_DATE'].min()} to {all_games['GAME_DATE'].max()}")
        
        # Filter to only yesterday's games
        all_games = all_games[all_games["GAME_DATE"] == yesterday_str]
        print(f"  After filtering to {yesterday_str}: {len(all_games)} records")
        
        if not all_games.empty:
            print(f"  Games found: {len(all_games) // 2} (each game has 2 records)")
            print(f"\n  Sample game IDs from API:")
            for gid in list(all_games['GAME_ID'].unique())[:3]:
                print(f"    - {gid}")
    
except Exception as e:
    print(f"✗ Error fetching games: {e}")
    cursor.close()
    db.close()
    exit(1)

# ==================== PROCESS GAMES ====================
print(f"\nProcessing games...")
games_to_insert = []
vs_pat = re.compile(r"\svs\.?\s", flags=re.IGNORECASE)
at_pat = re.compile(r"\s@\s")
skipped_duplicate = 0
skipped_other = 0

if not all_games.empty:
    for gid, grp in all_games.groupby("GAME_ID"):
        game_id = int(str(gid).lstrip('0')) if str(gid) != '0' else 0
        
        print(f"  Processing API Game ID: {gid} -> DB Game ID: {game_id}")
        
        if game_id in existing_game_ids:
            print(f"    ⏭️  Already in database")
            skipped_duplicate += 1
            continue
            
        if len(grp) < 2:
            print(f"    ⚠️  Only {len(grp)} record(s), need 2")
            skipped_other += 1
            continue
        
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
                print(f"    ⚠️  Could not determine home/away")
                skipped_other += 1
                continue
        
        home_abbr = str(home_row["TEAM_ABBREVIATION"]).strip()
        away_abbr = str(away_row["TEAM_ABBREVIATION"]).strip()
        
        print(f"    {away_abbr} @ {home_abbr}")
        
        db_home_id = team_abbr_to_db_id.get(home_abbr)
        db_away_id = team_abbr_to_db_id.get(away_abbr)
        
        if not db_home_id or not db_away_id:
            print(f"    ⚠️  Team not found in database")
            skipped_other += 1
            continue
        
        game_record = {
            "game_id": game_id,
            "game_date": str(home_row["GAME_DATE"]),
            "home_team_id": db_home_id,
            "away_team_id": db_away_id,
            "home_score": int(home_row["PTS"]) if pd.notna(home_row.get("PTS")) else None,
            "away_score": int(away_row["PTS"]) if pd.notna(away_row.get("PTS")) else None,
            "venue_id": None,  # Will be fetched below
            "attendance": None  # Will be fetched below
        }
        
        print(f"    ✓ Ready to insert")
        games_to_insert.append(game_record)

print(f"\n✓ {len(games_to_insert)} games ready to insert")
print(f"  Skipped {skipped_duplicate} duplicates")
print(f"  Skipped {skipped_other} for other reasons")

# ==================== FETCH ARENA & ATTENDANCE ====================
if games_to_insert:
    print(f"\nFetching arena and attendance data for {yesterday_str}...")
    venue_id_map = {}
    attendance_map = {}
    missing_arena_by_game = {}
    
    success = False
    for attempt in range(MAX_RETRIES):
        try:
            if attempt > 0:
                print(f"  Retry {attempt}/{MAX_RETRIES-1}...", end=" ")
            else:
                print(f"  Checking...", end=" ")
                
            sb = scoreboardv2.ScoreboardV2(game_date=yesterday_str, timeout=API_TIMEOUT)
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
                                print(f"\n    ✓ Matched '{arena}' -> VenueID {venue_id}")
                            else:
                                print(f"\n    ⚠️  Arena '{arena}' not found in database")
                                missing_arena_by_game[game_id] = arena
                    
                    if "ATTENDANCE" in gh.columns:
                        att = ghr.get("ATTENDANCE")
                        if pd.notna(att) and att > 0:
                            attendance_map[game_id] = int(att)
            
            print(f"\n  Found {found} venue matches")
            success = True
            break
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Failed ({e}), retrying...")
                time.sleep(2)
            else:
                print(f"Failed after {MAX_RETRIES} attempts: {e}")
    
    print(f"✓ Matched venue IDs for {len(venue_id_map)} games")
    print(f"✓ Fetched attendance for {len(attendance_map)} games")

    # Update games with venue_id and attendance
    for game in games_to_insert:
        game["venue_id"] = venue_id_map.get(game["game_id"])
        game["attendance"] = attendance_map.get(game["game_id"])

# ==================== INSERT INTO DATABASE ====================
inserted_count = 0

if games_to_insert:
    try:
        # Optional: default venue if you still want to allow inserts with missing arena mapping.
        # If you *never* want to allow that, you can remove this and rely on the explicit check below.
        cursor.execute("SELECT VenueID FROM Venue ORDER BY VenueID LIMIT 1")
        default_venue_id = cursor.fetchone()[0]

        print(f"\nInserting into database (using default venue ID {default_venue_id} only if allowed)...")

        insert_query = """
            INSERT INTO Game (GameID, Date, HomeTeamID, AwayTeamID, VenueID, HomeTeamScore, AwayTeamScore, Attendance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Pre-check for missing arenas: if any game has no venue_id, treat it as an error
        games_missing_arena = [
            game for game in games_to_insert
            if game.get("venue_id") is None
        ]

        if games_missing_arena:
            print("\n🚨 ERROR: One or more games are missing arena/venue mappings. Rolling back, no games inserted.")
            for game in games_missing_arena:
                gid = game["game_id"]
                arena_name = None
                # Try to show the arena name if we captured it earlier
                # (may not exist for all games)
                # missing_arena_by_game is built in the scoreboard section
                if 'missing_arena_by_game' in globals():
                    arena_name = missing_arena_by_game.get(gid)

                if arena_name:
                    print(f"  - GameID {gid}: arena '{arena_name}' not found in Venue table")
                else:
                    print(f"  - GameID {gid}: venue_id is missing (no matching arena found)")
            # Explicit rollback and skip inserts entirely
            db.rollback()
        else:
            games_without_venue = 0

            for game in games_to_insert:
                venue_id = game["venue_id"]
                if venue_id is None:
                    # This shouldn't happen because of the pre-check, but guard anyway
                    raise ValueError(f"Missing venue_id for game {game['game_id']}")

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
                print(f"  ✓ Inserted game {game['game_id']} (Venue: {venue_id})")

            # If we got here, everything worked: commit once
            db.commit()
            print(f"\n✓ Committed {inserted_count} games to database")

    except Exception as e:
        db.rollback()
        print("\n🚨 ERROR: Rolling back transaction, no games inserted.")
        print(f"Details: {e}")
else:
    print("\n⚠️  No games to insert")

# ==================== OUTPUT ====================
print("\n" + "="*60)
print(f"SUMMARY: Inserted {inserted_count} game(s) from {yesterday_str}")
print("="*60)

# ==================== CLEANUP ====================
cursor.close()
db.close()