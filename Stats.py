"""
NBA Player Game Stats Fetcher - Database Insert Version
-------------------------------------------------------
Fetches player statistics and inserts into PlayerGameStats table.
GameID, PlayerID, Points, Rebounds, Assists, Steals, Blocks, Turnovers, Fouls, Minutes

All IDs are mapped to your database IDs (not API IDs).
"""

from nba_api.stats.endpoints import playergamelogs
import pandas as pd
import mysql.connector

# ==================== CONFIGURATION ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'CPSC408!',
    'database': 'HoopHub'
}

SEASON = "2025-26"
START_DATE = "2025-11-08"
END_DATE = "2025-11-09"

print("="*80)
print(f"NBA Player Game Stats Fetcher - Database Insert")
print(f"Date range: {START_DATE} to {END_DATE}")
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

# ==================== BUILD PLAYER MAPPING ====================
print("Building player ID mapping...")
cursor.execute("""
    SELECT p.PlayerID, p.FirstName, p.LastName, t.Abbreviation
    FROM Player p
    JOIN Team t ON p.TeamID = t.TeamID
""")
db_players = cursor.fetchall()

# Create mapping: player name (lowercase) -> DB PlayerID
player_name_to_db_id = {}
for player_id, first, last, team_abbr in db_players:
    full_name = f"{first} {last}".strip().lower()
    player_name_to_db_id[full_name] = player_id

print(f"✓ Loaded {len(player_name_to_db_id)} players from database\n")

# ==================== GET EXISTING STATS FROM DATABASE ====================
print("Loading existing stats from database...")
cursor.execute("SELECT GameID, PlayerID FROM PlayerGameStats")
existing_stats = {(row[0], row[1]) for row in cursor.fetchall()}
print(f"✓ Found {len(existing_stats)} existing stat records in database\n")

# ==================== FETCH PLAYER STATS FROM API ====================
print(f"Fetching player game logs from API...")
print("This may take a minute...\n")

try:
    logs = playergamelogs.PlayerGameLogs(
        season_nullable=SEASON,
        season_type_nullable='Regular Season'
    )
    
    df = logs.get_data_frames()[0]
    
    print(f"✓ Retrieved {len(df)} player game log entries")
    print(f"   Date range in API: {df['GAME_DATE'].min()} to {df['GAME_DATE'].max()}\n")
    
    # Convert GAME_DATE to just date (remove timestamp)
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE']).dt.date.astype(str)
    
    # Filter to date range
    df_filtered = df[(df['GAME_DATE'] >= START_DATE) & (df['GAME_DATE'] <= END_DATE)].copy()
    
    print(f"✓ Filtered to {len(df_filtered)} entries between {START_DATE} and {END_DATE}")
    print(f"   Unique games: {df_filtered['GAME_ID'].nunique()}")
    print(f"   Unique players: {df_filtered['PLAYER_ID'].nunique()}\n")
    
    if df_filtered.empty:
        print("⚠️  No data in your date range")
        cursor.close()
        db.close()
        exit(0)
    
except Exception as e:
    print(f"✗ Error fetching data: {e}")
    cursor.close()
    db.close()
    exit(1)

# ==================== MAP API PLAYER NAMES TO DB PLAYER IDS ====================
print("Mapping players to database IDs...")

df_filtered['PLAYER_NAME_LOWER'] = df_filtered['PLAYER_NAME'].str.strip().str.lower()
df_filtered['PlayerID'] = df_filtered['PLAYER_NAME_LOWER'].map(player_name_to_db_id)

# Check mapping success
mapped_count = df_filtered['PlayerID'].notna().sum()
unmapped_count = df_filtered['PlayerID'].isna().sum()

print(f"✓ Mapped {mapped_count}/{len(df_filtered)} entries to database PlayerIDs")
match_rate = (mapped_count / len(df_filtered) * 100) if len(df_filtered) > 0 else 0
print(f"   Match rate: {match_rate:.1f}%")

if unmapped_count > 0:
    print(f"⚠️  Could not map {unmapped_count} entries ({unmapped_count/len(df_filtered)*100:.1f}%)")
    unmapped_players = df_filtered[df_filtered['PlayerID'].isna()]['PLAYER_NAME'].unique()
    print(f"   Unmapped players ({len(unmapped_players)}):")
    for name in sorted(unmapped_players)[:10]:
        print(f"     - {name}")
    if len(unmapped_players) > 10:
        print(f"     ... and {len(unmapped_players) - 10} more")
    print("   (These will be excluded from output)")

print()

# ==================== PREPARE OUTPUT ====================
print("Preparing data for database insert...")

# Select only columns matching PlayerGameStats schema
output_cols = {
    'GAME_ID': 'GameID',
    'PlayerID': 'PlayerID',
    'PTS': 'Points',
    'REB': 'Rebounds',
    'AST': 'Assists',
    'STL': 'Steals',
    'BLK': 'Blocks',
    'TOV': 'Turnovers',
    'PF': 'Fouls',
    'MIN': 'Minutes'
}

# Select and rename columns
export_cols = [col for col in output_cols.keys() if col in df_filtered.columns]
df_export = df_filtered[export_cols].copy()
df_export = df_export.rename(columns=output_cols)

# Remove rows where PlayerID couldn't be mapped
original_count = len(df_export)
df_export = df_export[df_export['PlayerID'].notna()]
removed_count = original_count - len(df_export)

if removed_count > 0:
    print(f"⚠️  Removed {removed_count} rows with unmapped PlayerIDs\n")

# Convert GameID to match your database format (remove leading zeros)
df_export['GameID'] = df_export['GameID'].astype(str).str.lstrip('0')
df_export['GameID'] = pd.to_numeric(df_export['GameID'], errors='coerce').astype('Int64')

# Convert PlayerID to integer
df_export['PlayerID'] = df_export['PlayerID'].astype(int)

# Convert all numeric columns to proper types
numeric_cols = ['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'Turnovers', 'Fouls']
for col in numeric_cols:
    if col in df_export.columns:
        df_export[col] = pd.to_numeric(df_export[col], errors='coerce').fillna(0).astype(int)

# Handle Minutes (might be decimal)
if 'Minutes' in df_export.columns:
    df_export['Minutes'] = pd.to_numeric(df_export['Minutes'], errors='coerce').fillna(0).astype(int)

# ==================== INSERT INTO DATABASE ====================
print("\nInserting records into database...")

insert_query = """
    INSERT INTO PlayerGameStats (GameID, PlayerID, Points, Rebounds, Assists, Steals, Blocks, Turnovers, Fouls, Minutes)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

inserted_count = 0
skipped_count = 0
error_count = 0

for _, row in df_export.iterrows():
    try:
        game_id = int(row['GameID'])
        player_id = int(row['PlayerID'])
    except (ValueError, TypeError):
        error_count += 1
        continue
    
    # Skip if already exists in database
    if (game_id, player_id) in existing_stats:
        skipped_count += 1
        continue
    
    try:
        cursor.execute(insert_query, (
            game_id,
            player_id,
            int(row['Points']),
            int(row['Rebounds']),
            int(row['Assists']),
            int(row['Steals']),
            int(row['Blocks']),
            int(row['Turnovers']),
            int(row['Fouls']),
            int(row['Minutes'])
        ))
        inserted_count += 1
    except Exception as e:
        error_count += 1

db.commit()

print()
print("="*80)
print("SUCCESS!")
print("="*80)
print(f"✓ Inserted {inserted_count} records into database")
if skipped_count > 0:
    print(f"  Skipped {skipped_count} duplicates already in database")
if error_count > 0:
    print(f"  {error_count} errors occurred during insertion")
print()
print("Database Summary:")
print(f"  New records inserted: {inserted_count}")
print(f"  Games covered: {df_export['GameID'].nunique()}")
print(f"  Players processed: {df_export['PlayerID'].nunique()}")
print()
if inserted_count > 0:
    print("Top 5 scorers in newly inserted data:")
    # Filter to only newly inserted records
    newly_inserted = df_export[df_export.apply(
        lambda r: (int(r['GameID']), int(r['PlayerID'])) not in existing_stats, axis=1
    )]
    if len(newly_inserted) > 0:
        top_scorers = newly_inserted.nlargest(5, 'Points')[['PlayerID', 'GameID', 'Points', 'Rebounds', 'Assists']]
        print(top_scorers.to_string(index=False))
        print()
print("✓ All IDs are from your database (not API IDs)")
print("✓ Data successfully inserted into PlayerGameStats table!")
print("="*80)

# ==================== CLEANUP ====================
cursor.close()
db.close()