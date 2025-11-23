import streamlit as st
import os
import pandas as pd
from db_operations import db_operations
db = db_operations()
# If you want DB stuff later:
# from db_operations import db_operations
# db = db_operations()

# 🔧 UPDATE THIS to match your actual folder
# Example if your structure is: HoopHub/Logos/LAL.png, etc.
LOGO_DIR = "Logos"

# (Team Name, Abbreviation)
TEAMS = [
    ("Atlanta Hawks", "ATL"),
    ("Boston Celtics", "BOS"),
    ("Brooklyn Nets", "BKN"),
    ("Charlotte Hornets", "CHA"),
    ("Chicago Bulls", "CHI"),
    ("Cleveland Cavaliers", "CLE"),
    ("Dallas Mavericks", "DAL"),
    ("Denver Nuggets", "DEN"),
    ("Detroit Pistons", "DET"),
    ("Golden State Warriors", "GSW"),
    ("Houston Rockets", "HOU"),
    ("Indiana Pacers", "IND"),
    ("LA Clippers", "LAC"),
    ("Los Angeles Lakers", "LAL"),
    ("Memphis Grizzlies", "MEM"),
    ("Miami Heat", "MIA"),
    ("Milwaukee Bucks", "MIL"),
    ("Minnesota Timberwolves", "MIN"),
    ("New Orleans Pelicans", "NOP"),
    ("New York Knicks", "NYK"),
    ("Oklahoma City Thunder", "OKC"),
    ("Orlando Magic", "ORL"),
    ("Philadelphia 76ers", "PHI"),
    ("Phoenix Suns", "PHX"),
    ("Portland Trail Blazers", "POR"),
    ("Sacramento Kings", "SAC"),
    ("San Antonio Spurs", "SAS"),
    ("Toronto Raptors", "TOR"),
    ("Utah Jazz", "UTA"),
    ("Washington Wizards", "WAS"),
]

st.session_state.current_page = "teams"

st.title("NBA Teams")

# Track which team is currently selected
if "selected_team_abbr" not in st.session_state:
    st.session_state.selected_team_abbr = None


def show_team_grid():
    """Show the grid of team logos + buttons."""
    per_row = 5

    for i in range(0, len(TEAMS), per_row):
        row_teams = TEAMS[i:i + per_row]
        cols = st.columns(len(row_teams))

        for col, (name, abbr) in zip(cols, row_teams):
            with col:
                logo_path = os.path.join(LOGO_DIR, f"{abbr}.png")

                # Show logo if file exists
                if os.path.exists(logo_path):
                    st.image(logo_path, width=240)

                # Button under logo to select this team
                if st.button(name, key=f"btn_{abbr}"):
                    st.session_state.selected_team_abbr = abbr
                    st.rerun()


def show_team_detail():
    """Show detail view for the selected team."""
    abbr = st.session_state.selected_team_abbr
    team_name = next((name for name, a in TEAMS if a == abbr), abbr)

    # Back button
    if st.button("Back to all teams"):
        st.session_state.selected_team_abbr = None
        st.rerun()

    st.header(team_name)

    # Show logo again in detail view
    logo_path = os.path.join(LOGO_DIR, f"{abbr}.png")
    if os.path.exists(logo_path):
        st.image(logo_path, width=240)
    st.divider()

    col_info, col_record = st.columns(2)
    
    with col_info:
        st.subheader("Team Information")

        bio = db.get_team_bio(team_name)

        if bio:
            (
                name,
                abbreviation,
                city,
                state,
                conference,
                division,
                coach,
                arena,
            ) = bio

            st.write(f"**Location:** {city}, {state}")
            st.write(f"**Abbreviation:** {abbreviation}")
            st.write(f"**Conference:** {conference}")
            st.write(f"**Division:** {division}")
            st.write(f"**Head Coach:** {coach}")
            st.write(f"**Home Arena:** {arena}")
        else:
            st.write("Bio information not available.")
    
    with col_record:
        st.subheader("Team Record")
        team_record = db.get_team_record(team_name)
        home_record = db.get_team_home_record(team_name)
        away_record = db.get_team_away_record(team_name)
        if team_record:
            st.write(f"**Record:** {team_record[0]} - {team_record[1]}")
            st.write(f"**Home Record:** {home_record[0]} - {home_record[1]}")
            st.write(f"**Away Record:** {away_record[0]} - {away_record[1]}")
        else:
            st.write("Team record not available.")

    st.divider()

    avc_scores = db.get_team_score(team_name)
    if avc_scores:
        ppg = avc_scores[0]
        opp_ppg = avc_scores[1]
        point_diff = ppg - opp_ppg

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Average Points Scored", f"{ppg:.1f}")
        with c2:
            st.metric("Average Points Allowed", f"{opp_ppg:.1f}")
        with c3:
            st.metric("Point Differential", f"{point_diff:+.1f}")
    else:
        st.info("No average scores found for this team.")
    
    st.divider()
    
    # Placeholder for future DB data (roster, recent games, etc.)
    st.subheader("Roster")

    players = db.get_team_roster(team_name)
    if players:
        roster = []
        for player in players:
            roster.append({
                "Name": player[0],
                "Age": player[1],
                "Position": player[2],
                "Number": player[3],
            })
        df = pd.DataFrame(roster)
        st.dataframe(
        df.reset_index(drop=True),
        use_container_width=True,
        hide_index=True
        )
    else:
        st.write("No players found for this team.")
    
    st.subheader("Recent Games")
    games = db.get_team_recent_games(team_name)
    if games:
        for game in games:
            game_date = game[0]      # e.g., date or string
            home_team = game[1]        # "Lakers"
            away_team = game[2]        # "Suns"
            home_score = game[3]
            away_score = game[4]

            card = st.container()
            with card:
                c1, c2, c3, c4 = st.columns([2, 6, 3, 1])

                with c1:
                    st.write(f"**{game_date}**")

                with c2:
                    st.write(f"**{home_team} vs. {away_team}**")

                with c3:
                    st.write(f"**{home_score} - {away_score}**")
                
                with c4:
                    if home_team == team_name:
                        if home_score > away_score:
                            st.write("**W**")
                        else:
                            st.write("**L**")
                    else:
                        if home_score < away_score:
                            st.write("**W**")
                        else:
                            st.write("**L**")

            st.divider()
    else:
        st.write("No recent games found for this team.")

    st.subheader("Head-to-Head Record")
    opponents = db.get_all_teams(team_name)
    selected_opponent = st.selectbox("Select an opponent", opponents, index = None, placeholder = "Select an opponent")
    if selected_opponent:
        record = db.get_head_to_head_record(team_name, selected_opponent)

        # record should be a tuple like (wins, losses)
        if not record:
            st.info(f"No games played between {team_name} and {selected_opponent} yet this season.")
        else:
            wins, losses = record

            # If SUM() returns NULL when there are no matching games
            if wins is None and losses is None:
                st.info(f"No games played between {team_name} and {selected_opponent} yet this season.")
            else:
                st.markdown(f"### {team_name} vs {selected_opponent}")
                st.write(f"**Record:** {wins} – {losses}")

# --- Main page logic ---
if st.session_state.selected_team_abbr is None:
    show_team_grid()
else:
    show_team_detail()