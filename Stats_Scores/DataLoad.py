# import mysql.connector
# import pandas as pd
# import math

# # --- NBA API import (safe) ---
# try:
#     from nba_api.stats.endpoints import commonteamroster
#     from nba_api.stats.static import teams as nba_static_teams
# except Exception as _e:
#     commonteamroster = None
#     nba_static_teams = None
#     print("[WARN] nba_api not available. Install with: pip install nba_api")

# # --- 1. Connect to the database ---
# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="CPSC408!",
#     database="HoopHub"
# )


# cursor = connection.cursor()

# # --- Height parsing helper: "6-7" -> 79 inches ---
# def height_to_inches(height_str):
#     """Convert height strings like '6-7', '6-07', '6 7' to total inches (int).
#     Returns None if parsing fails."""
#     if height_str is None:
#         return None
#     s = str(height_str).strip()
#     if not s:
#         return None
#     # Normalize separators
#     s = s.replace(" ", "-")
#     parts = s.split("-")
#     try:
#         ft = int(parts[0]) if parts[0] else 0
#         inch = int(parts[1]) if len(parts) > 1 and parts[1] else 0
#         return ft * 12 + inch
#     except Exception:
#         # Last resort: try to extract digits
#         import re
#         nums = re.findall(r"\d+", s)
#         if not nums:
#             return None
#         if len(nums) == 1:
#             # assume already inches
#             try:
#                 return int(nums[0])
#             except Exception:
#                 return None
#         try:
#             ft = int(nums[0]); inch = int(nums[1])
#             return ft * 12 + inch
#         except Exception:
#             return None




# # --- NEW: Build Team->CoachID mapping (coach stored as FirstName/LastName) ---
# # import unicodedata

# # def normalize(s: str) -> str:
# #     if s is None:
# #         return ""
# #     s = " ".join(str(s).split()).strip().lower()
# #     s = unicodedata.normalize("NFKD", s)
# #     return "".join(ch for ch in s if not unicodedata.combining(ch))

# # # 1) Load coaches from DB and build name->id map
# # cursor.execute("SELECT CoachID, FirstName, LastName FROM HeadCoach")
# # coach_rows = cursor.fetchall()
# # name_to_id = {}
# # for cid, fn, ln in coach_rows:
# #     full = f"{(fn or '').strip()} {(ln or '').strip()}".strip()
# #     if full:
# #         name_to_id[normalize(full)] = cid

# # --- Map NBA abbreviations to your DB TeamIDs ---
# nba_abbr_to_full = {
#     "ATL": "Atlanta Hawks",
#     "BOS": "Boston Celtics",
#     "BKN": "Brooklyn Nets",
#     "CHA": "Charlotte Hornets",
#     "CHI": "Chicago Bulls",
#     "CLE": "Cleveland Cavaliers",
#     "DAL": "Dallas Mavericks",
#     "DEN": "Denver Nuggets",
#     "DET": "Detroit Pistons",
#     "GSW": "Golden State Warriors",
#     "HOU": "Houston Rockets",
#     "IND": "Indiana Pacers",
#     "LAC": "Los Angeles Clippers",
#     "LAL": "Los Angeles Lakers",
#     "MEM": "Memphis Grizzlies",
#     "MIA": "Miami Heat",
#     "MIL": "Milwaukee Bucks",
#     "MIN": "Minnesota Timberwolves",
#     "NOP": "New Orleans Pelicans",
#     "NYK": "New York Knicks",
#     "OKC": "Oklahoma City Thunder",
#     "ORL": "Orlando Magic",
#     "PHI": "Philadelphia 76ers",
#     "PHX": "Phoenix Suns",
#     "POR": "Portland Trail Blazers",
#     "SAC": "Sacramento Kings",
#     "SAS": "San Antonio Spurs",
#     "TOR": "Toronto Raptors",
#     "UTA": "Utah Jazz",
#     "WAS": "Washington Wizards",
# }

# # Build NBA abbreviation -> NBA team_id map using nba_api static teams
# nba_abbr_to_id = {}
# if nba_static_teams is not None:
#     try:
#         for t in nba_static_teams.get_teams():
#             ab = t.get("abbreviation", "").strip().upper()
#             tid = t.get("id", None)
#             if ab and tid is not None:
#                 nba_abbr_to_id[ab] = int(tid)
#     except Exception as _e:
#         print("[WARN] Could not build nba_abbr_to_id from nba_api static teams:", _e)

# # Build DB name->id map from Team table
# cursor.execute("SELECT TeamID, Name FROM Team")
# rows = cursor.fetchall()
# db_name_to_id = {row[1].strip().lower(): row[0] for row in rows}

# # Also build DB abbreviation -> TeamID map (resolves ATL reliably)
# cursor.execute("SELECT TeamID, Abbreviation FROM Team")
# rows_abbr = cursor.fetchall()
# db_abbr_to_id = {}
# for tid, ab in rows_abbr:
#     if ab:
#         db_abbr_to_id[ab.strip().upper()] = tid


# def _normalize(name: str) -> str:
#     if name is None:
#         return ""
#     import unicodedata
#     s = " ".join(str(name).split()).strip().lower()
#     s = unicodedata.normalize("NFKD", s)
#     return "".join(ch for ch in s if not unicodedata.combining(ch))

# def get_db_team_id_from_abbr(abbr: str):
#     if not abbr:
#         return None

#     # 0) Try direct DB abbreviation match first (handles ATL even if Name text differs)
#     tid = db_abbr_to_id.get(abbr.strip().upper())
#     if tid is not None:
#         return tid

#     nba_name = nba_abbr_to_full.get(abbr.strip().upper())
#     if not nba_name:
#         return None

#     # Temporary direct match for Atlanta Hawks by Name if abbreviation wasn't present
#     if abbr.strip().upper() == "ATL":
#         try:
#             cursor.execute("SELECT TeamID FROM Team WHERE Name = 'Atlanta Hawks'")
#             result = cursor.fetchone()
#             if result:
#                 return result[0]
#         except Exception as e:
#             print(f"[WARN] Could not fetch Atlanta Hawks TeamID directly: {e}")

#     norm_full = _normalize(nba_name)

#     # 1) direct match: NBA full name -> DB Name (normalized)
#     tid = db_name_to_id.get(norm_full)
#     if tid is not None:
#         return tid

#     return None

# # --- Insert Teams from NBATeams.csv (atomic, CoachID auto-link) ---
# # import unicodedata

# # def _safe_norm(s: str) -> str:
# #     if s is None:
# #         return ""
# #     s = " ".join(str(s).split()).strip().lower()
# #     s = unicodedata.normalize("NFKD", s)
# #     return "".join(ch for ch in s if not unicodedata.combining(ch))

# # # Build Coach full-name -> CoachID map (already loaded above as name_to_id)
# # coach_map = name_to_id

# # # Path to your Teams CSV
# # teams_csv_path = "/Users/dylanbarlava/Desktop/CPSC408/HoopHub/NBATeams.csv"

# # # Read CSV
# # teams_df = pd.read_csv(teams_csv_path)
# # required_cols = ["Name","Abbreviation","City","State","Conference","Division","Coach"]
# # missing_cols = [c for c in required_cols if c not in teams_df.columns]
# # if missing_cols:
# #     raise ValueError(f"NBATeams.csv is missing required columns: {missing_cols}")

# # # Build rows and validate coach links
# # TEAMS_DATA = []  # (Name, Abbreviation, City, State, Conference, Division, CoachID)
# # unmatched = []
# # for _, r in teams_df.iterrows():
# #     name = str(r["Name"]).strip()
# #     abbr = str(r["Abbreviation"]).strip()
# #     city = str(r["City"]).strip()
# #     state = str(r["State"]).strip()
# #     conf = str(r["Conference"]).strip()
# #     div  = str(r["Division"]).strip()
# #     coach_name = str(r["Coach"]).strip()

# #     # use normalize from above; fall back if not available
# #     try:
# #         key = normalize(coach_name)
# #     except Exception:
# #         key = _safe_norm(coach_name)

# #     coach_id = coach_map.get(key)
# #     if coach_id is None:
# #         unmatched.append((name, coach_name))
# #     TEAMS_DATA.append((name, abbr, city, state, conf, div, coach_id))

# # # Abort if coaches didn't resolve or row count is not 30
# # if unmatched or len(TEAMS_DATA) != 30:
# #     print("\n❌ Aborting insert: issues detected.")
# #     if len(TEAMS_DATA) != 30:
# #         print(f" - Expected 30 rows, got {len(TEAMS_DATA)}")
# #     if unmatched:
# #         print(" - Coach names that didn’t match HeadCoach (fix spelling in CSV or add to HeadCoach):")
# #         for team_name, coach_name in unmatched:
# #             print(f"    * {team_name}: '{coach_name}'")
# # else:
# #     try:
# #         # fresh transaction
# #         if getattr(connection, "in_transaction", False):
# #             connection.rollback()
# #         connection.start_transaction()

# #         insert_sql = (
# #             "INSERT INTO Team (Name, Abbreviation, City, State, Conference, Division, CoachID) "
# #             "VALUES (%s, %s, %s, %s, %s, %s, %s)"
# #         )
# #         cursor.executemany(insert_sql, TEAMS_DATA)
# #         connection.commit()
# #         print("\n✅ All 30 teams inserted successfully from NBATeams.csv (atomic).")
# #     except Exception as e:
# #         connection.rollback()
# #         print(f"\n🚨 Error occurred. Rolled back transaction.\nDetails: {e}")

# # --- 2. Read from CSV ---
# # file_path = "/Users/dylanbarlava/Desktop/CPSC408/HoopHub/NBAHeadCoaches.csv"
# # df = pd.read_csv(file_path)

# # --- 3. Insert into HeadCoach table ---
# # for _, row in df.iterrows():
# #     first = str(row["First Name"]).strip()
# #     last = str(row["Last Name"]).strip()
# #     query = "INSERT INTO HeadCoach (FirstName, LastName) VALUES (%s, %s)"
# #     cursor.execute(query, (first, last))

# # # --- New section: Read from NBAVenues.csv and insert into Venue table ---
# # venue_file_path = "/Users/dylanbarlava/Desktop/CPSC408/HoopHub/NBAArenas.csv"
# # df = pd.read_csv(venue_file_path)

# # # --- 3) Discover actual Venue table column names ---
# # cursor.execute("SHOW COLUMNS FROM Venue")
# # cols_db = [row[0] for row in cursor.fetchall()]
# # cols_db_l = [c.lower() for c in cols_db]

# # def find_col(candidates):
# #     for cand in candidates:
# #         if cand.lower() in cols_db_l:
# #             # return original-cased name from DB
# #             return cols_db[cols_db_l.index(cand.lower())]
# #     return None

# # # Try to resolve DB columns for the four fields we need
# # db_name_col = find_col(["VenueName", "Name", "Venue", "Arena", "ArenaName"])
# # db_city_col = find_col(["City", "Town"])
# # db_state_col = find_col(["State", "StateProvince", "Province", "Region"])
# # db_capacity_col = find_col(["Capacity", "Seats", "SeatingCapacity"])

# # missing = [n for n, v in {
# #     "Venue(Name)": db_name_col,
# #     "City": db_city_col,
# #     "State/Province": db_state_col
# # }.items() if v is None]

# # if missing:
# #     print("ERROR: Could not find required Venue columns in your DB.")
# #     print("Missing logical columns:", missing)
# #     print("Columns present in Venue table:", cols_db)
# #     raise SystemExit(1)

# # # --- 4) Build INSERT with actual DB column names ---
# # insert_cols = [db_name_col, db_city_col, db_state_col]
# # placeholders = ["%s", "%s", "%s"]
# # if db_capacity_col:
# #     insert_cols.append(db_capacity_col)
# #     placeholders.append("%s")

# # insert_sql = f"INSERT INTO Venue (`{'`, `'.join(insert_cols)}`) VALUES ({', '.join(placeholders)})"

# # # --- 5) Insert rows ---
# # for _, row in df.iterrows():
# #     name = str(row["VenueName"]).strip()
# #     city = str(row["City"]).strip()
# #     state = str(row["State"]).strip()

# #     cap_raw = row.get("Capacity", None)
# #     if cap_raw is None or (isinstance(cap_raw, float) and math.isnan(cap_raw)):
# #         capacity = None
# #     else:
# #         try:
# #             capacity = int(cap_raw)
# #         except Exception:
# #             capacity = None

# #     values = [name, city, state]
# #     if db_capacity_col:
# #         values.append(capacity)

# #     cursor.execute(insert_sql, values)

# # # --- 6) Commit + close ---
# # connection.commit()
# # cursor.close()
# # connection.close()

# # print("All venues inserted successfully!")

# # === INSERT PLAYERS FROM NBA API (2025-26) ===
# import time
# import pandas as pd

# if commonteamroster is None:
#     print("[ERROR] nba_api is not available. Install with: pip install nba_api")
# else:
#     # 1) Discover Player columns so we only insert what exists
#     cursor.execute("SHOW COLUMNS FROM Player")
#     player_cols = {row[0] for row in cursor.fetchall()}

#     def _pick(*cands):
#         for c in cands:
#             if c in player_cols:
#                 return c
#         return None

#     col_TeamID   = _pick("TeamID")
#     col_First    = _pick("FirstName", "First")
#     col_Last     = _pick("LastName", "Last")
#     col_Age      = _pick("Age", "PlayerAge")
#     col_Pos      = _pick("Position", "Pos")
#     col_Number   = _pick("Number", "Jersey", "No", "Num")  # keep as text to preserve "00"
#     col_HeightIn = _pick("HeightIn", "HeightInches")
#     col_WeightLb = _pick(
#         "WeightPounds",  # your column
#         "WeightLb", "Weight", "WeightLbs", "WeightLB",
#         "PlayerWeight", "Wt", "WeightInPounds", "Weight_Pounds"
#     )
#     if col_WeightLb is None:
#         print("[WARN] Player weight column not found in DB (expected one of: WeightLb/Weight/WeightLbs/WeightLB/PlayerWeight/Wt/WeightInPounds/Weight_Pounds). Skipping weight.")

#     needed = [col_TeamID, col_First, col_Last]
#     if not all(needed):
#         print("❌ Aborting: Player table must contain TeamID, FirstName, LastName.")
#     else:
#         # Update-only mode: do not insert new players, only update weight
#         UPDATE_ONLY = True
#         # 2) Get your DB teams for abbreviations
#         cursor.execute("SELECT TeamID, Name, Abbreviation FROM Team")
#         teams = cursor.fetchall()
#         if not teams:
#             print("❌ No teams found in Team. Insert teams first.")
#         else:
#             def _to_int(x):
#                 if x is None or (isinstance(x, float) and pd.isna(x)):
#                     return None
#                 s = str(x).strip()
#                 if s == "":
#                     return None
#                 try:
#                     return int(float(s))
#                 except Exception:
#                     return None

#             player_rows = []
#             update_rows = []  # (weight, TeamID, FirstName, LastName)

#             for team_id_db, team_name, abbr in teams:
#                 abbr = (abbr or "").strip().upper()
#                 # Skip Atlanta Hawks (already updated)
#                 if abbr == "ATL":
#                     continue
#                 if not abbr:
#                     print(f"[WARN] Skipping team {team_name} (no abbreviation)")
#                     continue

#                 # Resolve YOUR DB TeamID from the NBA abbreviation via team name mapping
#                 resolved_team_id = get_db_team_id_from_abbr(abbr)
#                 if resolved_team_id is None:
#                     print(f"[WARN] Could not resolve DB TeamID for {abbr}. Check Team.Name spelling in your DB.")
#                     continue

#                 # Resolve NBA numeric team_id from abbreviation for the API call
#                 nba_team_id = nba_abbr_to_id.get(abbr)
#                 if nba_team_id is None:
#                     print(f"[WARN] No NBA team_id for abbr {abbr}.")
#                     continue

#                 # Fetch roster by NBA team_id (correct signature)
#                 try:
#                     df = commonteamroster.CommonTeamRoster(season="2025-26", team_id=nba_team_id).get_data_frames()[0]
#                 except Exception as e1:
#                     print(f"[WARN] Fetch failed for {team_name} ({abbr}, nba_id={nba_team_id}): {e1}")
#                     continue

#                 for _, r in df.iterrows():
#                     full = str(r.get("PLAYER", "")).strip()
#                     parts = full.split()
#                     if not parts:
#                         first, last = "", ""
#                     elif len(parts) == 1:
#                         first, last = parts[0], ""
#                     else:
#                         first, last = " ".join(parts[:-1]), parts[-1]

#                     age    = _to_int(r.get("AGE"))
#                     pos    = (str(r.get("POSITION")).strip() if pd.notna(r.get("POSITION")) else None)
#                     number = (str(r.get("NUM")).strip() if pd.notna(r.get("NUM")) else None)  # text to preserve 00
#                     height = height_to_inches(r.get("HEIGHT"))
#                     weight = _to_int(r.get("WEIGHT"))

#                     row = { col_TeamID: resolved_team_id, col_First: first, col_Last: last }
#                     if col_Age is not None:      row[col_Age] = age
#                     if col_Pos is not None:      row[col_Pos] = pos
#                     if col_Number is not None:   row[col_Number] = number
#                     if col_HeightIn is not None: row[col_HeightIn] = height
#                     if col_WeightLb is not None: row[col_WeightLb] = weight

#                     player_rows.append(row)

#                     if weight is not None and col_WeightLb is not None:
#                         update_rows.append((weight, resolved_team_id, first, last))

#                 time.sleep(0.6)  # respectful rate limiting

#             # UPDATE-ONLY path: update weights and skip inserts
#             if UPDATE_ONLY:
#                 if not update_rows:
#                     print("❌ No weight updates prepared; aborting.")
#                 else:
#                     try:
#                         if getattr(connection, "in_transaction", False):
#                             connection.rollback()
#                         connection.start_transaction()

#                         # Build dynamic UPDATE using your detected column names
#                         sql_upd = (
#                             f"UPDATE Player SET `{col_WeightLb}`=%s "
#                             f"WHERE `{col_TeamID}`=%s AND `{col_First}`=%s AND `{col_Last}`=%s"
#                         )
#                         cursor.executemany(sql_upd, update_rows)
#                         connection.commit()
#                         print(f"✅ Updated weights for {cursor.rowcount} player rows.")
#                     except Exception as e:
#                         connection.rollback()
#                         print(f"🚨 Error during weight updates. Rolled back. Details: {e}")
#                 # Skip the insert path entirely when updating only

#             # 3) Insert atomically (only if not in update-only mode)
#             if not UPDATE_ONLY:
#                 if not player_rows:
#                     print("❌ No player rows prepared; aborting.")
#                 else:
#                     try:
#                         if getattr(connection, "in_transaction", False):
#                             connection.rollback()
#                         connection.start_transaction()
#                         cols = list(player_rows[0].keys())
#                         col_sql = ", ".join(f"`{c}`" for c in cols)
#                         ph = ", ".join(["%s"] * len(cols))
#                         sql = f"INSERT INTO Player ({col_sql}) VALUES ({ph})"
#                         data = [tuple(d[c] for c in cols) for d in player_rows]
#                         cursor.executemany(sql, data)
#                         connection.commit()
#                         print(f"✅ Inserted {len(player_rows)} player rows (2025-26 rosters).")
#                     except Exception as e:
#                         connection.rollback()
#                         print(f"🚨 Error: Rolled back. Details: {e}")