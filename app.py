import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Top Shelf Analytics", layout="wide")

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "skater_summary.csv"

@st.cache_data
def load_data():
    return pd.read_csv(CSV_PATH)

df = load_data()

st.title("Top Shelf Analytics")
st.subheader("Skater Summary")

# ---- Sidebar Filters ----
st.sidebar.header("Filters")

filtered_df = df.copy()

# Team filter
if "teamAbbrev" in filtered_df.columns:
    teams = sorted(filtered_df["teamAbbrev"].dropna().unique())
    selected_teams = st.sidebar.multiselect("Team", teams)
    if selected_teams:
        filtered_df = filtered_df[filtered_df["teamAbbrev"].isin(selected_teams)]

# Player search
if "skaterFullName" in filtered_df.columns:
    search = st.sidebar.text_input("Search player")
    if search:
        filtered_df = filtered_df[
            filtered_df["skaterFullName"].str.contains(search, case=False, na=False)
        ]

# Numeric filter example (shots)
if "summary_shots" in filtered_df.columns:
    min_shots = st.sidebar.slider(
        "Minimum shots", 
        int(filtered_df["summary_shots"].min()), 
        int(filtered_df["summary_shots"].max()), 
        0
    )
    filtered_df = filtered_df[filtered_df["summary_shots"] >= min_shots]

# Row limit
row_limit = st.sidebar.selectbox("Rows to display", [25, 50, 100, 250, 500, 1000], index=2)

st.write(f"Showing {min(len(filtered_df), row_limit):,} of {len(filtered_df):,} rows")

st.dataframe(filtered_df.head(row_limit), use_container_width=True, hide_index=True)

# ---- Download button ----
st.download_button(
    "Download filtered data",
    filtered_df.to_csv(index=False),
    "filtered_skater_data.csv",
    "text/csv"
)