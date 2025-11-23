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
    player_team = st.selectbox("Team:", options = db.get_teams(), index = None, placeholder = "Select a team")
    player_first_name = st.text_input("First Name:")
    player_last_name = st.text_input("Last Name:")
    player_age = st.number_input("Age:", min_value = 18, max_value = 60)
    player_position = st.text_input("Position (e.g. G, F, C, F-C, G-F):")
    player_number = st.number_input("Number:", min_value = 0, max_value = 99)
    player_height = st.number_input("Height (inches):", min_value = 60)
    player_weight = st.number_input("Weight (lbs):", min_value = 100)
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
