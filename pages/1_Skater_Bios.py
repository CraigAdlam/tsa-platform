from pathlib import Path
import streamlit as st
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "skater_bios.csv"

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
    "skaterFullName", "playerId", "height", "weight", "birthDate",
    "birthCity", "birthStateProvinceCode", "birthCountryCode",
    "draftYear", "draftRound", "draftOverall", "isInHallOfFameYn",
    "currentTeamAbbrev", "currentTeamName", "shootsCatches",
    "positionCode", "gamesPlayed", "goals", "assists", "points",
    "nationalityCode", "lastName", "firstSeasonForGameType"
]

df = df[[col for col in desired_order if col in df.columns]]

if "birthDate" in df.columns:
    df["birthDate"] = pd.to_datetime(df["birthDate"], errors="coerce").dt.date

st.subheader("Skater Bios")

st.sidebar.header("Filters")
filtered_df = df.copy()

# ---- Rows per page ----
page_size = st.sidebar.selectbox("Rows per page", [10, 25, 50, 100], index=0, key="bios_page_size")

# ---- Team Filter ----
if "currentTeamAbbrev" in filtered_df.columns:
    teams = sorted(filtered_df["currentTeamAbbrev"].dropna().astype(str).unique().tolist())
    selected_teams = st.sidebar.multiselect("Team", teams, key="bios_team_filter")
    if selected_teams:
        filtered_df = filtered_df[
            filtered_df["currentTeamAbbrev"].astype(str).isin(selected_teams)
        ]

# ---- Skater Filter ----
if "skaterFullName" in filtered_df.columns:
    search = st.sidebar.text_input("Search player", key="bios_search")
    if search:
        filtered_df = filtered_df[
            filtered_df["skaterFullName"].astype(str).str.contains(search, case=False, na=False)
        ]

# ---- Pagination ----
total_rows = len(filtered_df)
total_pages = max(1, (total_rows + page_size - 1) // page_size)

if "bios_page_num" not in st.session_state:
    st.session_state.bios_page_num = 1

if "bios_last_filter_count" not in st.session_state:
    st.session_state.bios_last_filter_count = total_rows

if st.session_state.bios_last_filter_count != total_rows:
    st.session_state.bios_page_num = 1
    st.session_state.bios_last_filter_count = total_rows

if st.session_state.bios_page_num > total_pages:
    st.session_state.bios_page_num = total_pages
if st.session_state.bios_page_num < 1:
    st.session_state.bios_page_num = 1

start_idx = (st.session_state.bios_page_num - 1) * page_size
end_idx = start_idx + page_size
page_df = filtered_df.iloc[start_idx:end_idx]

st.caption(f"Rows {start_idx + 1:,}–{min(end_idx, total_rows):,} of {total_rows:,}")

col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if st.button("◀ Back", disabled=st.session_state.bios_page_num <= 1, key="bios_back"):
        st.session_state.bios_page_num -= 1
        st.rerun()

with col2:
    st.markdown(
        f"<div style='text-align:center; padding-top:0.5rem;'>Page {st.session_state.bios_page_num} of {total_pages}</div>",
        unsafe_allow_html=True
    )

with col3:
    if st.button("Next ▶", disabled=st.session_state.bios_page_num >= total_pages, key="bios_next"):
        st.session_state.bios_page_num += 1
        st.rerun()

rows_on_page = len(page_df)
table_height = min(1000, max(220, rows_on_page * 35 + 40))

st.dataframe(
    page_df,
    width="stretch",
    hide_index=True,
    height=table_height
)

col1, col2 = st.columns(2)

with col1:
    st.download_button(
        "⬇️ Filtered",
        filtered_df.to_csv(index=False),
        "filtered_skater_bios.csv",
        "text/csv",
        key="bios_download_filtered"
    )

with col2:
    st.download_button(
        "⬇️ Full",
        df.to_csv(index=False),
        "full_skater_bios.csv",
        "text/csv",
        key="bios_download_full"
    )