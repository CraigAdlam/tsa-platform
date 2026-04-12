import streamlit as st
import pandas as pd

st.set_page_config(page_title="Top Shelf Analytics", layout="wide")

@st.cache_data
def load_data():
    return pd.read_csv("summary_skater.csv")

df = load_data()

st.title("Top Shelf Analytics")
st.subheader("Skater Summary")

st.write(f"Rows: {len(df):,}")

st.dataframe(df, use_container_width=True)