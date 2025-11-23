import streamlit as st

home_page = st.Page("Pages/Home.py", title = "Home")
games_page = st.Page("Pages/Games.py", title = "Games")
teams_page = st.Page("Pages/Teams.py", title = "Teams")
standings_page = st.Page("Pages/Standings.py", title = "Standings")
players_page = st.Page("Pages/Players.py", title = "Players")
player_comparison_page = st.Page("Pages/PlayerComparison.py", title = "Player Comparison")
admin_page = st.Page("Pages/Admin.py", title = "Admin")

pg = st.navigation([home_page, games_page, teams_page, standings_page, players_page, player_comparison_page, admin_page])

st.set_page_config(
    page_title="HoopHub",
    page_icon="🏀",
    initial_sidebar_state="collapsed"   # 👈 hides sidebar by default
)

pg.run()