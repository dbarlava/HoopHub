import streamlit as st
import os
import subprocess
import sys
from db_operations import db_operations
db = db_operations()

st.title("Admin Panel")

# ---------------------------
# Session state setup
# ---------------------------
if "admin_logged_in" not in st.session_state:
    st.session_state.admin_logged_in = False
# Auto-logout whenever user loads a different page
if st.session_state.get("current_page") != "admin":
    st.session_state.admin_logged_in = False

st.session_state.current_page = "admin"

# ---------------------------
# Login screen
# ---------------------------
if not st.session_state.admin_logged_in:
    st.caption("Enter password to access database tools.")

    password = st.text_input("Password:", type="password")

    if st.button("Login"):
        if password == "Lakers2025":
            st.session_state.admin_logged_in = True
            # Show a temporary toast instead of a success message that stays on the page
            st.rerun()
        else:
            st.error("Incorrect password. Try again.")

    st.stop()  # do not show admin tools if not logged in


# ---------------------------
# Helper: run update scripts
# ---------------------------
def run_script(script_name: str):
    """
    Runs a Python script located in the Stats_Scores folder
    inside the main HoopHub project directory.
    """
    # This file is: HoopHub/pages/Admin.py
    pages_dir = os.path.dirname(__file__)          # .../HoopHub/pages
    project_root = os.path.dirname(pages_dir)      # .../HoopHub
    script_folder = os.path.join(project_root, "Stats_Scores")
    script_path = os.path.join(script_folder, script_name)

    if not os.path.exists(script_path):
        st.error(f"Script not found: {script_path}")
        return

    st.info(f"Running {script_name}...")

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            st.success(f"{script_name} ran successfully")
        else:
            st.error(f"{script_name} exited with code {result.returncode} ❌")

        if result.stdout:
            st.markdown("**Output:**")
            st.code(result.stdout)

        if result.stderr:
            st.markdown("**Errors / Warnings:**")
            st.code(result.stderr)

    except Exception as e:
        st.error(f"Failed to run script: {e}")


# ---------------------------
# Logged-in view
# ---------------------------

if st.button("Logout"):
    st.session_state.admin_logged_in = False
    st.rerun()

st.write("Use the tools below to update your database from the NBA API.")
st.write("You must update the games first, then the stats in order for the stats to be updated correctly.")

col1, col2 = st.columns(2)

with col1:
    if st.button("Insert Yesterday's Games"):
        run_script("DailyScoreInsert.py")

with col2:
    if st.button("Insert Yesterday's Stats"):
        run_script("DailyStatsInsert.py")

st.divider()

with st.expander("Insert a New Player"):
    player_team = st.selectbox("Team:", options = db.get_teams(), index = None, placeholder = "Select a team", key = "insert_player_team")
    player_first_name = st.text_input("First Name:")
    player_last_name = st.text_input("Last Name:")
    player_age = st.number_input("Age:", min_value = 18, max_value = 60, key = "insert_player_age")
    player_position = st.selectbox("Position:", options = db.get_positions(), index = None, placeholder = "Select a position", key = "insert_player_position")
    player_number = st.number_input("Number:", min_value = 0, max_value = 99, key = "insert_player_number")
    player_height = st.number_input("Height (inches):", min_value = 60, key = "insert_player_height")
    player_weight = st.number_input("Weight (lbs):", min_value = 100, key = "insert_player_weight")
    if st.button("Insert Player"):
        if not player_team or not player_first_name or not player_last_name or not player_age or not player_position or not player_height or not player_weight:
            st.error("Please fill all required fields.")
        else:
            success = db.insert_player(
                player_team,
                player_first_name,
                player_last_name,
                player_age,
                player_position,
                player_number,
                player_height,
                player_weight
            )
            if success:
                st.success("Player inserted successfully!")
            else:
                st.error("Failed to insert player.")

with st.expander("Change Player Details"):
    player_name = st.selectbox("Player:", options = db.get_players(), index = None, placeholder = "Select a player")
    if player_name:
        st.markdown(f"### Editing Player: {player_name}")
        
        col1, col2, col3 = st.columns([1, 2, 2])
        with col1:
            st.write("Team:")
        with col2:
            player_team = st.selectbox("", options = db.get_teams(), index = None, placeholder = "Select a team", key = "change_player_team", label_visibility = "collapsed")
        with col3:
            if st.button("Change Player Team"):
                if not player_name or not player_team:
                    st.error("Please fill all required fields.")
                else:
                    success = db.change_player_team(player_name, player_team)
                    if success:
                        st.success("Player team changed successfully!")
                    else:
                        st.error("Failed to change player team.")
        col4, col5, col6 = st.columns([1, 2, 2])
        with col4:
            st.write("Age:")
        with col5:
            player_age = st.number_input("", min_value = 18, max_value = 60, value = None, key = "change_player_age", label_visibility = "collapsed")
        with col6:
            if st.button("Change Player Age"):
                if not player_name or not player_age:
                    st.error("Please fill all required fields.")
                else:
                    success = db.change_player_age(player_name, player_age)
                    if success:
                        st.success("Player age changed successfully!")
                    else:
                        st.error("Failed to change player age.")
        col7, col8, col9 = st.columns([1, 2, 2])
        with col7:
            st.write("Position:")
        with col8:
            player_position = st.selectbox("", options = ['G', 'F', 'C', 'F-C', 'G-F'], index = None, placeholder = "Select a position", key = "change_player_position", label_visibility = "collapsed")
        with col9:
            if st.button("Change Player Position"):
                if not player_name or not player_position:
                    st.error("Please fill all required fields.")
                else:
                    success = db.change_player_position(player_name, player_position)
                    if success:
                        st.success("Player position changed successfully!")
                    else:
                        st.error("Failed to change player position.")
        col10, col11, col12 = st.columns([1, 2, 2])
        with col10:
            st.write("Number:")
        with col11:
            player_number = st.number_input("", min_value = 0, max_value = 99, value = None, key = "change_player_number", label_visibility = "collapsed")
        with col12:
            if st.button("Change Player Number"):
                if not player_name or not player_number:
                    st.error("Please fill all required fields.")
                else:
                    success = db.change_player_number(player_name, player_number)
                    if success:
                        st.success("Player number changed successfully!")
                    else:
                        st.error("Failed to change player number.")
        col13, col14, col15 = st.columns([1, 2, 2])
        with col13: 
            st.write("Height (inches):")
        with col14:
            player_height = st.number_input("", min_value = 60, value = None, key = "change_player_height", label_visibility = "collapsed")
        with col15:
            if st.button("Change Player Height"):
                if not player_name or not player_height:
                    st.error("Please fill all required fields.")
                else:
                    success = db.change_player_height(player_name, player_height)
                    if success:
                        st.success("Player height changed successfully!")
                    else:
                        st.error("Failed to change player height.")

        col16, col17, col18 = st.columns([1, 2, 2])
        with col16:
            st.write("Weight (lbs):")
        with col17:
            player_weight = st.number_input("", min_value = 100, value = None, key = "change_player_weight", label_visibility = "collapsed")
        with col18:
            if st.button("Change Player Weight"):
                if not player_name or not player_weight:
                    st.error("Please fill all required fields.")
                else:
                    success = db.change_player_weight(player_name, player_weight)
                    if success:
                        st.success("Player weight changed successfully!")
                    else:
                        st.error("Failed to change player weight.")

with st.expander("Delete a Player"):
    player_name = st.selectbox("Player:", options = db.get_players(), index = None, placeholder = "Select a player", key = "delete_player_name")
    if player_name:
        st.markdown(f"### Deleting Player: {player_name}")
        if st.button("Delete Player"):
            success = db.delete_player(player_name)
            if success:
                st.success("Player deleted successfully!")
            else:
                st.error("Failed to delete player.")
