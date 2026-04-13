from pathlib import Path
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Top Shelf Analytics", layout="wide")

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "skater_summary.csv"

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
    "teamAbbrev", "opponentTeamAbbrev", "shootsCatches", "positionCode",
    "gamesPlayed", "goals", "assists", "points", "plusMinus",
    "penaltyMinutes", "pointsPerGame", "evGoals", "evPoints",
    "ppGoals", "ppPoints", "shGoals", "shPoints", "otGoals",
    "gameWinningGoals", "shots", "shootingPct",
    "timeOnIcePerGame", "faceoffWinPct", "lastName"
]

df = df[[col for col in desired_order if col in df.columns]]

if "gameDate" in df.columns:
    df["gameDate"] = pd.to_datetime(df["gameDate"], errors="coerce").dt.date

# st.title("Top Shelf Analytics")
st.subheader("Skater Summary")

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
            index=0
        )

        if date_mode == "Single date":
            selected_date = st.sidebar.date_input(
                "Game Date",
                value=max_date,
                min_value=min_date,
                max_value=max_date
            )

            selected_date = pd.to_datetime(selected_date).date()

            filtered_df = filtered_df[
                filtered_df["gameDate"] == selected_date
            ]

        else:
            date_range = st.sidebar.date_input(
                "Game Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )

            if len(date_range) == 2:
                start_date = date_range[0]
                end_date = date_range[1]

                filtered_df = filtered_df[
                    filtered_df["gameDate"].between(start_date, end_date)
                ]
                
# --- Row Limit OR Rows per page Filter ---
# row_limit = st.sidebar.selectbox("Rows to display", [25, 50, 100, 250, 500, 1000], index=2)
page_size = st.sidebar.selectbox("Rows per page", [10, 25, 50, 100], index=0)

# ---- Team Filter ----
if "teamAbbrev" in filtered_df.columns:
    teams = sorted(filtered_df["teamAbbrev"].dropna().astype(str).unique().tolist())
    selected_teams = st.sidebar.multiselect("Team", teams)
    if selected_teams:
        filtered_df = filtered_df[filtered_df["teamAbbrev"].astype(str).isin(selected_teams)]

# ---- Skater Filter ----
if "skaterFullName" in filtered_df.columns:
    search = st.sidebar.text_input("Search player")
    if search:
        filtered_df = filtered_df[
            filtered_df["skaterFullName"].astype(str).str.contains(search, case=False, na=False)
        ]

### --- WITHOUT PAGINATION ---------------------------------------------------------------------------------------------------
# st.write(f"Showing {min(len(filtered_df), row_limit):,} of {len(filtered_df):,} rows")

# Option 1: Scrollable grid with limited visible rows, but inner scroll bar
# st.dataframe(filtered_df.head(row_limit), use_container_width=True, hide_index=True) # USE THIS IF PREFER THE SCROLLABLE GRID WITH LIMITED VISIBLE ROWS (COMMENT OUT THE SECTION BELOW) ---

# Option 2: Increased table height (keeps performance + usability, but makes it feel much better on desktop)
# st.dataframe(
#     filtered_df.head(row_limit),
#     use_container_width=True,
#     hide_index=True,
#     height=600
# )

# Option 3: Dynamic heigh (best UX)
# rows_shown = min(len(filtered_df), row_limit)
# table_height = min(1000, rows_shown * 35 + 40)

# st.dataframe(
#     filtered_df.head(row_limit),
#     use_container_width=True,
#     hide_index=True,
#     height=table_height
# )

# Option 4: Remove scroll entirely
# st.dataframe(filtered_df.head(row_limit), use_container_width=True, hide_index=True)
### --- WITHOUT PAGINATION ---------------------------------------------------------------------------------------------------

### --- WITH PAGINATION ---------------------------------------------------------------------------------------------------
# ---- Pagination ----
total_rows = len(filtered_df)
total_pages = max(1, (total_rows + page_size - 1) // page_size)

if "page_num" not in st.session_state:
    st.session_state.page_num = 1

if "last_filter_count" not in st.session_state:
    st.session_state.last_filter_count = total_rows

# Reset to page 1 when filters change
if st.session_state.last_filter_count != total_rows:
    st.session_state.page_num = 1
    st.session_state.last_filter_count = total_rows

# Keep page number valid
if st.session_state.page_num > total_pages:
    st.session_state.page_num = total_pages
if st.session_state.page_num < 1:
    st.session_state.page_num = 1

start_idx = (st.session_state.page_num - 1) * page_size
end_idx = start_idx + page_size
page_df = filtered_df.iloc[start_idx:end_idx]

# st.write(f"Showing rows {start_idx + 1:,}–{min(end_idx, total_rows):,} of {total_rows:,}")
st.caption(f"Rows {start_idx + 1:,}–{min(end_idx, total_rows):,} of {total_rows:,}")

col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if st.button("◀ Back", disabled=st.session_state.page_num <= 1):
        st.session_state.page_num -= 1
        st.rerun()

with col2:
    st.markdown(
        f"<div style='text-align:center; padding-top:0.5rem;'>Page {st.session_state.page_num} of {total_pages}</div>",
        unsafe_allow_html=True
    )

with col3:
    if st.button("Next ▶", disabled=st.session_state.page_num >= total_pages):
        st.session_state.page_num += 1
        st.rerun()

# --- Option 1: Fixed height ---
# st.dataframe(
#     page_df,
#     width="stretch",
#     hide_index=True,
#     height=600
# )

# --- Option 2: Dynamic height ---
rows_on_page = len(page_df)
table_height = min(1000, max(220, rows_on_page * 35 + 40))

st.dataframe(
    page_df,
    width="stretch",
    hide_index=True,
    height=table_height
)
### --- WITH PAGINATION ---------------------------------------------------------------------------------------------------

col1, col2 = st.columns(2)

with col1:
    st.download_button(
        "Filtered",
        filtered_df.to_csv(index=False),
        "filtered_skater_data.csv",
        "text/csv",
        key="summary_download_filtered"
    )

with col2:
    st.download_button(
        "Full Dataset",
        df.to_csv(index=False),
        "full_skater_data.csv",
        "text/csv",
        key="summary_download_full"
    )