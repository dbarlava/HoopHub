import streamlit as st
import os
import subprocess
import sys
from db_operations import db_operations
db = db_operations()

st.title("Player Comparison")

st.session_state.current_page = "player_comparison"

st.write("This page allows you to see a players performance against another team or see how their performance compares to other players.")

with st.expander("Compare Player Performance Against Another Team"):
    player_name = st.selectbox("Select a Player:", options = db.get_players(), index = None, placeholder = "Select a player", key = "player_comparison_team_player")
    team_name = st.selectbox("Select a Team:", options = db.get_all_teams(), index = None, placeholder = "Select a team", key = "player_comparison_team_team")
    if player_name and team_name:
        player_team_performance = db.get_player_team_performance(player_name, team_name)
        if player_team_performance[0] != 0:
            st.markdown(f"### Here is {player_name}'s performance against {team_name} in {player_team_performance[0]} game(s):")
            st.write(f"Points: {player_team_performance[1]:.1f}")
            st.write(f"Rebounds: {player_team_performance[2]:.1f}")
            st.write(f"Assists: {player_team_performance[3]:.1f}")
            st.write(f"Blocks: {player_team_performance[4]:.1f}")
            st.write(f"Steals: {player_team_performance[5]:.1f}")
            st.write(f"Turnovers: {player_team_performance[6]:.1f}")
            st.write(f"Fouls: {player_team_performance[7]:.1f}")
        else:
            st.warning(f"{player_name} has not played against {team_name} this season.")
    else:
        st.warning("A player and a team must be selected to compare performance.")

with st.expander("Compare Player Performance to Another Player"):
    player1_name = st.selectbox("Select Player 1:", options = db.get_players(), index = None, placeholder = "Select a player", key = "player1_comparison_player1")
    player2_name = st.selectbox("Select Player 2:", options = db.get_players(), index = None, placeholder = "Select a player", key = "player2_comparison_player2")
    if player1_name and player2_name:
        # player1_performance = db.get_player_performance(player1_name)
        # player2_performance = db.get_player_performance(player2_name)
        st.markdown(f"### {player1_name} vs {player2_name} Season Performance Comparison:")
        player1_performance = db.get_player_stats(player1_name)
        player2_performance = db.get_player_stats(player2_name)
        col1, col2 = st.columns(2)
        with col1:
            st.subheader(f"_{player1_name}_:")
            with st.container(border = True):
                st.subheader(f"Games Played: {player1_performance[0]}")
                if player1_performance[1] > player2_performance[1]:
                    st.subheader(f"Points: :green[{player1_performance[1]:.1f}]")
                elif player1_performance[1] < player2_performance[1]:
                    st.subheader(f"Points: :red[{player1_performance[1]:.1f}]")
                else:
                    st.subheader(f"Points: {player1_performance[1]:.1f}")
                if player1_performance[2] > player2_performance[2]:
                    st.subheader(f"Rebounds: :green[{player1_performance[2]:.1f}]")
                elif player1_performance[2] < player2_performance[2]:
                    st.subheader(f"Rebounds: :red[{player1_performance[2]:.1f}]")
                else:
                    st.subheader(f"Rebounds: {player1_performance[2]:.1f}")
                if player1_performance[3] > player2_performance[3]:
                    st.subheader(f"Assists: :green[{player1_performance[3]:.1f}]")
                elif player1_performance[3] < player2_performance[3]:
                    st.subheader(f"Assists: :red[{player1_performance[3]:.1f}]")
                else:
                    st.subheader(f"Assists: {player1_performance[3]:.1f}")
                if player1_performance[4] > player2_performance[4]:
                    st.subheader(f"Blocks: :green[{player1_performance[4]:.1f}]")
                elif player1_performance[4] < player2_performance[4]:
                    st.subheader(f"Blocks: :red[{player1_performance[4]:.1f}]")
                else:
                    st.subheader(f"Blocks: {player1_performance[4]:.1f}")
                if player1_performance[5] > player2_performance[5]:
                    st.subheader(f"Steals: :green[{player1_performance[5]:.1f}]")
                elif player1_performance[5] < player2_performance[5]:
                    st.subheader(f"Steals: :red[{player1_performance[5]:.1f}]")
                else:
                    st.subheader(f"Steals: {player1_performance[5]:.1f}")
                if player1_performance[6] > player2_performance[6]:
                    st.subheader(f"Turnovers: :green[{player1_performance[6]:.1f}]")
                elif player1_performance[6] < player2_performance[6]:
                    st.subheader(f"Turnovers: :red[{player1_performance[6]:.1f}]")
                else:
                    st.subheader(f"Turnovers: {player1_performance[6]:.1f}")
                if player1_performance[7] > player2_performance[7]:
                    st.subheader(f"Fouls: :green[{player1_performance[7]:.1f}]")
                elif player1_performance[7] < player2_performance[7]:
                    st.subheader(f"Fouls: :red[{player1_performance[7]:.1f}]")
                else:
                    st.subheader(f"Fouls: {player1_performance[7]:.1f}")
        with col2:
            st.subheader(f"_{player2_name}_:")
            with st.container(border = True):
                st.subheader(f"Games Played: {player2_performance[0]}")
                if player2_performance[1] > player1_performance[1]:
                    st.subheader(f"Points: :green[{player2_performance[1]:.1f}]")
                elif player2_performance[1] < player1_performance[1]:
                    st.subheader(f"Points: :red[{player2_performance[1]:.1f}]")
                else:
                    st.subheader(f"Points: {player2_performance[1]:.1f}")
                if player2_performance[2] > player1_performance[2]:
                    st.subheader(f"Rebounds: :green[{player2_performance[2]:.1f}]")
                elif player2_performance[2] < player1_performance[2]:
                    st.subheader(f"Rebounds: :red[{player2_performance[2]:.1f}]")
                else:
                    st.subheader(f"Rebounds: {player2_performance[2]:.1f}")
                if player2_performance[3] > player1_performance[3]:
                    st.subheader(f"Assists: :green[{player2_performance[3]:.1f}]")
                elif player2_performance[3] < player1_performance[3]:
                    st.subheader(f"Assists: :red[{player2_performance[3]:.1f}]")
                else:
                    st.subheader(f"Assists: {player2_performance[3]:.1f}")
                if player2_performance[4] > player1_performance[4]:
                    st.subheader(f"Blocks: :green[{player2_performance[4]:.1f}]")
                elif player2_performance[4] < player1_performance[4]:
                    st.subheader(f"Blocks: :red[{player2_performance[4]:.1f}]")
                else:
                    st.subheader(f"Blocks: {player2_performance[4]:.1f}")
                if player2_performance[5] > player1_performance[5]:
                    st.subheader(f"Steals: :green[{player2_performance[5]:.1f}]")
                elif player2_performance[5] < player1_performance[5]:
                    st.subheader(f"Steals: :red[{player2_performance[5]:.1f}]")
                else:
                    st.subheader(f"Steals: {player2_performance[5]:.1f}")
                if player2_performance[6] > player1_performance[6]:
                    st.subheader(f"Turnovers: :green[{player2_performance[6]:.1f}]")
                elif player2_performance[6] < player1_performance[6]:
                    st.subheader(f"Turnovers: :red[{player2_performance[6]:.1f}]")
                else:
                    st.subheader(f"Turnovers: {player2_performance[6]:.1f}")
                if player2_performance[7] > player1_performance[7]:
                    st.subheader(f"Fouls: :green[{player2_performance[7]:.1f}]")
                elif player2_performance[7] < player1_performance[7]:
                    st.subheader(f"Fouls: :red[{player2_performance[7]:.1f}]")
                else:
                    st.subheader(f"Fouls: {player2_performance[7]:.1f}")
        st.divider()
        player1_performance = db.get_player_performance(player1_name, player2_name)
        player2_performance = db.get_player_performance(player2_name, player1_name)
        st.markdown(f"### {player1_name} vs {player2_name} Performance in {player1_performance[0]} Games Against Each Other:")
        if player1_performance[0] != 0:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader(f"_{player1_name}_:")
                with st.container(border = True):
                    if player1_performance[1] > player2_performance[1]:
                        st.subheader(f"Points: :green[{player1_performance[1]:.1f}]")
                    elif player1_performance[1] < player2_performance[1]:
                        st.subheader(f"Points: :red[{player1_performance[1]:.1f}]")
                    else:
                        st.subheader(f"Points: {player1_performance[1]:.1f}")
                    if player1_performance[2] > player2_performance[2]:
                        st.subheader(f"Rebounds: :green[{player1_performance[2]:.1f}]")
                    elif player1_performance[2] < player2_performance[2]:
                        st.subheader(f"Rebounds: :red[{player1_performance[2]:.1f}]")
                    else:
                        st.subheader(f"Rebounds: {player1_performance[2]:.1f}")
                    if player1_performance[3] > player2_performance[3]:
                        st.subheader(f"Assists: :green[{player1_performance[3]:.1f}]")
                    elif player1_performance[3] < player2_performance[3]:
                        st.subheader(f"Assists: :red[{player1_performance[3]:.1f}]")
                    else:
                        st.subheader(f"Assists: {player1_performance[3]:.1f}")
                    if player1_performance[4] > player2_performance[4]:
                        st.subheader(f"Blocks: :green[{player1_performance[4]:.1f}]")
                    elif player1_performance[4] < player2_performance[4]:
                        st.subheader(f"Blocks: :red[{player1_performance[4]:.1f}]")
                    else:
                        st.subheader(f"Blocks: {player1_performance[4]:.1f}")
                    if player1_performance[5] > player2_performance[5]:
                        st.subheader(f"Steals: :green[{player1_performance[5]:.1f}]")
                    elif player1_performance[5] < player2_performance[5]:
                        st.subheader(f"Steals: :red[{player1_performance[5]:.1f}]")
                    else:
                        st.subheader(f"Steals: {player1_performance[5]:.1f}")
                    if player1_performance[6] > player2_performance[6]:
                        st.subheader(f"Turnovers: :green[{player1_performance[6]:.1f}]")
                    elif player1_performance[6] < player2_performance[6]:
                        st.subheader(f"Turnovers: :red[{player1_performance[6]:.1f}]")
                    else:
                        st.subheader(f"Turnovers: {player1_performance[6]:.1f}")
                    if player1_performance[7] > player2_performance[7]:
                        st.subheader(f"Fouls: :green[{player1_performance[7]:.1f}]")
                    elif player1_performance[7] < player2_performance[7]:
                        st.subheader(f"Fouls: :red[{player1_performance[7]:.1f}]")
                    else:
                        st.subheader(f"Fouls: {player1_performance[7]:.1f}")
            with col2:
                st.subheader(f"_{player2_name}_:")
                with st.container(border = True):
                    if player1_performance[1] < player2_performance[1]:
                        st.subheader(f"Points: :green[{player2_performance[1]:.1f}]")
                    elif player1_performance[1] > player2_performance[1]:
                        st.subheader(f"Points: :red[{player2_performance[1]:.1f}]")
                    else:
                        st.subheader(f"Points: {player2_performance[1]:.1f}")
                    if player1_performance[2] < player2_performance[2]:
                        st.subheader(f"Rebounds: :green[{player2_performance[2]:.1f}]")
                    elif player1_performance[2] > player2_performance[2]:
                        st.subheader(f"Rebounds: :red[{player2_performance[2]:.1f}]")
                    else:
                        st.subheader(f"Rebounds: {player2_performance[2]:.1f}")
                    if player1_performance[3] < player2_performance[3]:
                        st.subheader(f"Assists: :green[{player2_performance[3]:.1f}]")
                    elif player1_performance[3] > player2_performance[3]:
                        st.subheader(f"Assists: :red[{player2_performance[3]:.1f}]")
                    else:
                        st.subheader(f"Assists: {player2_performance[3]:.1f}")
                    if player1_performance[4] < player2_performance[4]:
                        st.subheader(f"Blocks: :green[{player2_performance[4]:.1f}]")
                    elif player1_performance[4] > player2_performance[4]:
                        st.subheader(f"Blocks: :red[{player2_performance[4]:.1f}]")
                    else:
                        st.subheader(f"Blocks: {player2_performance[4]:.1f}")
                    if player1_performance[5] < player2_performance[5]:
                        st.subheader(f"Steals: :green[{player2_performance[5]:.1f}]")
                    elif player1_performance[5] > player2_performance[5]:
                        st.subheader(f"Steals: :red[{player2_performance[5]:.1f}]")
                    else:
                        st.subheader(f"Steals: {player2_performance[5]:.1f}")
                    if player1_performance[6] < player2_performance[6]:
                        st.subheader(f"Turnovers: :green[{player2_performance[6]:.1f}]")
                    elif player1_performance[6] > player2_performance[6]:
                        st.subheader(f"Turnovers: :red[{player2_performance[6]:.1f}]")
                    else:
                        st.subheader(f"Turnovers: {player2_performance[6]:.1f}")
                    if player1_performance[7] < player2_performance[7]:
                        st.subheader(f"Fouls: :green[{player2_performance[7]:.1f}]")
                    elif player1_performance[7] > player2_performance[7]:
                        st.subheader(f"Fouls: :red[{player2_performance[7]:.1f}]")
                    else:
                        st.subheader(f"Fouls: {player2_performance[7]:.1f}")
        else:
            st.warning(f"{player1_name} and {player2_name} have not played against each other this season.")
    else:
        st.warning("Two players must be selected to compare performance.")
