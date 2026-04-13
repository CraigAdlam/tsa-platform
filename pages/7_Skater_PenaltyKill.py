from pathlib import Path
import streamlit as st
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "skater_penaltykill.csv"

@st.cache_data
def load_data(csv_path):
    if not csv_path.exists():
        st.error(f"CSV not found: {csv_path.name}")
        st.stop()

    file_size = csv_path.stat().st_size
    if file_size == 0:
        st.error(f"{csv_path.name} is empty (0 bytes).")
        st.stop()

    try:
        return pd.read_csv(csv_path)
    except pd.errors.EmptyDataError:
        st.error(f"{csv_path.name} contains no readable data.")
        st.stop()
    except Exception as e:
        st.error(f"Could not read {csv_path.name}: {e}")
        st.stop()

df = load_data(CSV_PATH)

desired_order = [
    "gameDate", "gameId", "skaterFullName", "playerId", "homeRoad",
    "teamAbbrev", "opponentTeamAbbrev", "positionCode", "gamesPlayed",
    "shGoals", "shAssists", "shPoints", "shPrimaryAssists",
    "shSecondaryAssists", "shShots", "shShootingPct", 
    "shIndividualSatFor", "shGoalsPer60", "ppGoalsAgainstPer60", 
    "shPrimaryAssistsPer60", "shSecondaryAssistsPer60","shPointsPer60", 
    "shShotsPer60", "shIndividualSatForPer60", "shTimeOnIce", 
    "shTimeOnIcePerGame", "shTimeOnIcePctPerGame", "lastName"
]

df = df[[col for col in desired_order if col in df.columns]]

if "gameDate" in df.columns:
    df["gameDate"] = pd.to_datetime(df["gameDate"], errors="coerce").dt.date

st.subheader("Skater Penalty Kill")

st.sidebar.header("Filters")
filtered_df = df.copy()

# ---- Date Filter ----
if "gameDate" in filtered_df.columns:
    min_date = filtered_df["gameDate"].min()
    max_date = filtered_df["gameDate"].max()

    if pd.notna(min_date) and pd.notna(max_date):
        date_mode = st.sidebar.radio(
            "Game Date Filter Type",
            ["Single date", "Date range"],
            index=0,
            key="pk_date_mode"
        )

        if date_mode == "Single date":
            selected_date = st.sidebar.date_input(
                "Game Date",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                key="pk_single_date"
            )

            filtered_df = filtered_df[
                filtered_df["gameDate"] == selected_date
            ]

        else:
            date_range = st.sidebar.date_input(
                "Game Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key="pk_date_range"
            )

            if len(date_range) == 2:
                start_date = date_range[0]
                end_date = date_range[1]

                filtered_df = filtered_df[
                    filtered_df["gameDate"].between(start_date, end_date)
                ]

# ---- Rows per page ----
page_size = st.sidebar.selectbox(
    "Rows per page",
    [10, 25, 50, 100],
    index=0,
    key="pk_page_size"
)

# ---- Team Filter ----
if "teamAbbrev" in filtered_df.columns:
    teams = sorted(filtered_df["teamAbbrev"].dropna().astype(str).unique().tolist())
    selected_teams = st.sidebar.multiselect(
        "Team",
        teams,
        key="pk_team_filter"
    )
    if selected_teams:
        filtered_df = filtered_df[
            filtered_df["teamAbbrev"].astype(str).isin(selected_teams)
        ]

# ---- Player Search ----
if "skaterFullName" in filtered_df.columns:
    search = st.sidebar.text_input("Search player", key="pk_search")
    if search:
        filtered_df = filtered_df[
            filtered_df["skaterFullName"].astype(str).str.contains(search, case=False, na=False)
        ]

# ---- Pagination ----
total_rows = len(filtered_df)
total_pages = max(1, (total_rows + page_size - 1) // page_size)

if "pk_page_num" not in st.session_state:
    st.session_state.pk_page_num = 1

if "pk_last_filter_count" not in st.session_state:
    st.session_state.pk_last_filter_count = total_rows

if st.session_state.pk_last_filter_count != total_rows:
    st.session_state.pk_page_num = 1
    st.session_state.pk_last_filter_count = total_rows

if st.session_state.pk_page_num > total_pages:
    st.session_state.pk_page_num = total_pages
if st.session_state.pk_page_num < 1:
    st.session_state.pk_page_num = 1

start_idx = (st.session_state.pk_page_num - 1) * page_size
end_idx = start_idx + page_size
page_df = filtered_df.iloc[start_idx:end_idx]

st.caption(f"Rows {start_idx + 1:,}–{min(end_idx, total_rows):,} of {total_rows:,}")

col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if st.button("◀ Back", disabled=st.session_state.pk_page_num <= 1, key="pk_back"):
        st.session_state.pk_page_num -= 1
        st.rerun()

with col2:
    st.markdown(
        f"<div style='text-align:center; padding-top:0.5rem;'>Page {st.session_state.pk_page_num} of {total_pages}</div>",
        unsafe_allow_html=True
    )

with col3:
    if st.button("Next ▶", disabled=st.session_state.pk_page_num >= total_pages, key="pk_next"):
        st.session_state.pk_page_num += 1
        st.rerun()

rows_on_page = len(page_df)
table_height = min(1000, max(220, rows_on_page * 35 + 40))

st.dataframe(
    page_df,
    width="stretch",
    hide_index=True,
    height=table_height
)

# ---- Download ----
st.markdown("### Download Options")

col1, col2 = st.columns(2)

with col1:
    st.download_button(
        "Filtered",
        filtered_df.to_csv(index=False),
        "filtered_skater_penaltykill.csv",
        "text/csv",
        key="pk_download_filtered"
    )

with col2:
    st.download_button(
        "Full Dataset",
        df.to_csv(index=False),
        "full_skater_penaltykill.csv",
        "text/csv",
        key="pk_download_full"
    )