import streamlit as st
import pandas as pd
import os
from db_operations import db_operations

db = db_operations()

st.session_state.current_page = "home"

st.image("Logos/HoopHubLogo.png", use_container_width=True)

st.subheader("Welcome to HoopHub")

st.write("""
HoopHub is your NBA dashboard for exploring teams, players, and game results.
""")

st.divider()

st.subheader("How to Use HoopHub")

st.write("""
1. **Games Page** - See all games played so far this season.
2. **Teams Page** - Browse all NBA teams, view rosters, and check recent games, and other cool stats.  
3. **Standings Page** - See league, conference, or division standings.
4. **Players Page** - Look up any active player and view their bio, stats, and team.
5. **Player Comparison Page** - Compare a players stats against another team or another players stats.
6. **Admin Page** - Update scores and player stats using the NBA API and add or remove players from the database.
""")

st.divider()

st.markdown("### Yesterday's Games")

games_yesterday = db.get_yesterdays_games()

if games_yesterday:
    games = []
    for game_date, home_team, away_team, home_pts, away_pts in games_yesterday:
        games.append({
            "Date": game_date,
            "Matchup": f"{away_team} @ {home_team}",
            "Score": f"{away_pts} - {home_pts}",
        })
    df = pd.DataFrame(games)

    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No games played yesterday or they haven't been added to the database yet.")
