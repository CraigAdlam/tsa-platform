import streamlit as st

st.set_page_config(
    page_title="Top Shelf Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

summary_page = st.Page("pages/1_Skater_Bios.py", title="Skater Bios")
bios_page = st.Page("pages/2_Skater_Summary.py", title="Skater Summary")
faceoff_pct_page = st.Page("pages/3_Skater_FaceoffPercentages.py", title="Faceoff Percentages")
faceoff_wins_page = st.Page("pages/4_Skater_FaceoffWins.py", title="Faceoff Wins")
goals_for_against_page = st.Page("pages/5_Skater_GoalsForAgainst.py", title="Goals For / Against")
penalties_page = st.Page("pages/6_Skater_Penalties.py", title="Penalties")
penalty_kill_page = st.Page("pages/7_Skater_PenaltyKill.py", title="Penalty Kill")
penalty_shots_page = st.Page("pages/8_Skater_PenaltyShots.py", title="Penalty Shots")
percentages_page = st.Page("pages/9_Skater_Percentages.py", title="Percentages")
powerplay_page = st.Page("pages/10_Skater_Powerplay.py", title="Power Play")
puck_possessions_page = st.Page("pages/11_Skater_PuckPossessions.py", title="Puck Possessions")
realtime_page = st.Page("pages/12_Skater_Realtime.py", title="Realtime")
scoring_per_game_page = st.Page("pages/13_Skater_ScoringPerGame.py", title="Scoring Per Game")

pg = st.navigation(
    [
        summary_page,
        bios_page,
        faceoff_pct_page,
        faceoff_wins_page,
        goals_for_against_page,
        penalties_page,
        penalty_kill_page,
        penalty_shots_page,
        percentages_page,
        powerplay_page,
        puck_possessions_page,
        realtime_page,
        scoring_per_game_page,
    ],
    position="sidebar",
    expanded=False,
)

pg.run()