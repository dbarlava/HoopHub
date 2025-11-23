import streamlit as st
from db_operations import db_operations
import pandas as pd
db = db_operations()

st.title("Standings")
st.write("Welcome to the standings page. This is a restricted area.")

st.segmented_control(label="Standings View", options = ["Conference", "League", "Division"], key="standings_view", default="Conference")

if st.session_state.standings_view == "Conference":
    st.markdown("### Western Conference Standings")
    west_standings = db.get_conference_standings("West")
    west_standings_df = pd.DataFrame(west_standings, columns=["Team", "Wins", "Losses", "WinPercentage"])
    west_standings_df.index = west_standings_df.index + 1
    st.dataframe(west_standings_df, use_container_width=True, height=563)

    st.markdown("### Eastern Conference Standings")
    east_standings = db.get_conference_standings("East")
    east_standings_df = pd.DataFrame(east_standings, columns=["Team", "Wins", "Losses", "WinPercentage"])
    east_standings_df.index = east_standings_df.index + 1
    st.dataframe(east_standings_df, use_container_width=True, height=563)

elif st.session_state.standings_view == "League":
    st.markdown("### League Standings")
    league_standings = db.get_league_standings()
    league_standings_df = pd.DataFrame(league_standings, columns=["Team", "Wins", "Losses", "WinPercentage"])
    league_standings_df.index = league_standings_df.index + 1
    st.dataframe(league_standings_df, use_container_width=True, height=1085)

elif st.session_state.standings_view == "Division":
    st.markdown("### Division Standings")
    divisions = db.get_divisions()
    for division in divisions:
        st.markdown(f"### {division[0]} Division Standings")
        division_standings = db.get_division_standings(division[0])
        division_standings_df = pd.DataFrame(division_standings, columns=["Team", "Wins", "Losses", "WinPercentage"])
        division_standings_df.index = division_standings_df.index + 1
        st.dataframe(division_standings_df)