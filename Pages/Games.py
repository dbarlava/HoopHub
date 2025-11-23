import streamlit as st
import pandas as pd
import datetime
from db_operations import db_operations

db = db_operations()

st.title("Games")

# --- Session state for boxscore "detail view" ---
if "selected_box_game_id" not in st.session_state:
    st.session_state.selected_box_game_id = None
if "selected_box_label" not in st.session_state:
    st.session_state.selected_box_label = ""


def show_games_list():
    st.subheader("See the games played on a specific date")
    st.write("Select a date to see the games played on that date")

    # Date picker (min 2025-10-21, max = yesterday)
    date = st.date_input(
        "Game Date",
        value=datetime.datetime.now() - datetime.timedelta(days=1),
        min_value=datetime.date(2025, 10, 21),
        max_value=datetime.date.today() - datetime.timedelta(days=1),
    )

    if not date:
        return

    game_date_string = date.strftime("%Y-%m-%d")
    games = db.get_games_by_date(game_date_string)
    st.markdown(f"### Games Played on {date.strftime('%m-%d-%Y')}")

    if games:
        for game in games:
            # game[0] = GameID, [1] = Date, [2] = Home, [3] = Away, [4] = HomeScore, [5] = AwayScore
            card = st.container()
            with card:
                c1, c2, c3, c4 = st.columns([1.5, 6, 1.5, 3])

                with c1:
                    st.write(f"**{game[1]}**")

                with c2:
                    st.write(f"**{game[3]}** @ **{game[2]}**")

                with c3:
                    st.write(f"**{game[5]}** - **{game[4]}**")

                with c4:
                    if st.button("View Boxscore", key=f"boxscore_button_{game[0]}"):
                        st.session_state.selected_box_game_id = game[0]
                        st.session_state.selected_box_label = (
                            f"{game[3]} @ {game[2]} ({game[1]})"
                        )
                        st.rerun()

            st.divider()
    else:
        st.write("No games found for that date")


def show_boxscore_view():
    """Full-page boxscore view, similar to team detail view."""
    game_id = st.session_state.selected_box_game_id
    label = st.session_state.selected_box_label

    # Back button like Teams page
    if st.button("Back to Games Page"):
        st.session_state.selected_box_game_id = None
        st.session_state.selected_box_label = ""
        st.rerun()

    st.header("Boxscore")
    if label:
        st.markdown(f"**{label}**")
    

    boxscore = db.get_boxscore(game_id)

    data = []
    for player in boxscore:
        data.append(
            {
                "Team": player[0],
                "Player": player[1],
                "Minutes": player[2],
                "Points": player[3],
                "Rebounds": player[4],
                "Assists": player[5],
                "Blocks": player[6],
                "Steals": player[7],
                "Turnovers": player[8],
                "Fouls": player[9],
            }
        )

    df = pd.DataFrame(data)

    st.download_button(label="Download Boxscore", data=df.to_csv(index=False), file_name=f"{label.replace(' @ ', '_')}_boxscore.csv", mime="text/csv")

    st.divider()

    if not boxscore:
        st.write("No boxscore found for that game.")
        return
    
    teams = df["Team"].unique()

    for team in teams:
        st.markdown(f"### {team}")

        team_df = (
            df[df["Team"] == team]
            .drop(columns=["Team"])            # no need to repeat team name in the table
            .sort_values(by="Minutes", ascending=False)
            .reset_index(drop=True)
        )

        st.dataframe(
            team_df,
            use_container_width=True,
            hide_index=True,
            height=385,   # a bit smaller since it's per-team
        )
        st.divider()


# --- Main page logic (like Teams grid vs detail) ---
if st.session_state.selected_box_game_id is None:
    show_games_list()
else:
    show_boxscore_view()