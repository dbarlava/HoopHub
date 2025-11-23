import streamlit as st
import pandas as pd
from db_operations import db_operations

db = db_operations()

st.title("Players")

players = db.get_all_players()

if not players:
    st.write("No players found in the database.")
    st.stop()

df = pd.DataFrame(players, columns=["PlayerID", "Name", "Team", "Position", "Points", "Rebounds", "Assists", "Blocks", "Steals", "Turnovers", "Fouls"])


st.subheader("Browse & Filter Players")

col1, col2, col3 = st.columns(3)

with col1:
    teams = ["All Teams"] + sorted(df["Team"].dropna().unique().tolist())
    team_filter = st.selectbox("Team", options=teams, index=0, placeholder="All Teams")

with col2:
    positions = ["All Positions"] + sorted(df["Position"].dropna().unique().tolist())
    position_filter = st.selectbox("Position", options=positions, index=0, placeholder="All Positions")

with col3:
    name_search = st.text_input("Search by name")

# Apply filters in pandas
filtered_df = df.copy()

if team_filter != "All Teams":
    filtered_df = filtered_df[filtered_df["Team"] == team_filter]

if position_filter != "All Positions":
    filtered_df = filtered_df[filtered_df["Position"] == position_filter]

if name_search.strip():
    search_lower = name_search.strip().lower()
    filtered_df = filtered_df[filtered_df["Name"].str.lower().str.contains(search_lower)]

st.markdown("### Player List")


st.dataframe(
    filtered_df[["Name", "Points", "Rebounds", "Assists", "Blocks", "Steals", "Turnovers", "Fouls"]],
    use_container_width=True,
    hide_index=True,
)


if filtered_df.empty:
    st.info("No players match the current filters.")
    st.stop()

# Build dropdown options from filtered list
name_options = ["Select a player..."] + filtered_df["Name"].tolist()
selected_name = st.selectbox("View player details", options=name_options, index=0)

if selected_name != "Select a player...":
    selected_row = filtered_df[filtered_df["Name"] == selected_name].iloc[0]
    selected_player_id = int(selected_row["PlayerID"])

    
    player_info = db.get_player_info(selected_player_id)
    # Expected: (FirstName, LastName, TeamName, Position, Age, HeightInches, WeightPounds, JerseyNumber)

    if not player_info:
        st.write("Player information not found.")
    else:
        name, team_name, position, age, height_in, weight_lb, jersey_number = player_info

        st.divider()
        st.header(name)

        col_a, col_b = st.columns(2)

        with col_a:
            st.write(f"**Team:** {team_name}")
            st.write(f"**Position:** {position}")
            st.write(f"**Jersey Number:** {jersey_number}")
            st.write(f"**Age:** {age}")

        with col_b:
            st.write(f"**Height:** {height_in} inches")
            st.write(f"**Weight:** {weight_lb} lbs")