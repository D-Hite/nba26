import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Get the active Snowpark session (Streamlit-in-Snowflake)
session = get_active_session()

# SQL options for your fantasy queries
QUERIES = {
    "Last 10 Z-scores": """
        SELECT 
            RANK() OVER (ORDER BY totalZ DESC) AS player_rank,
            firstName,
            familyName,
            totalZ,
            minutesZScore,
            pointsZScore,
            assistsZScore,
            stealsZScore,
            blocksZScore,
            reboundsZScore,
            threesZScore,
            doubleDoubleZScore,
            efgPctZScore,
            ftPctZScore
        FROM fantasy__main.zScores_last10
        WHERE totalZ IS NOT NULL
          AND seasonId = nba.raw.current_season()
        ORDER BY totalZ DESC
    """,
    "Season-long Z-scores": """
        SELECT 
            RANK() OVER (ORDER BY totalZ DESC) AS player_rank,
            firstName,
            familyName,
            totalZ,
            minutesZScore,
            pointsZScore,
            assistsZScore,
            stealsZScore,
            blocksZScore,
            reboundsZScore,
            threesZScore,
            doubleDoubleZScore,
            efgPctZScore,
            ftPctZScore
        FROM fantasy__main.zScoresAverages
        WHERE totalZ IS NOT NULL
          AND seasonId = nba.raw.current_season()
        ORDER BY totalZ DESC
    """
}

@st.cache_data(show_spinner=False)
def load_data(query_key: str, limit: int) -> pd.DataFrame:
    """
    Run the selected fantasy z-scores query and return a pandas DataFrame.
    Limit rows for performance / UI.
    """
    query = QUERIES[query_key]
    df = (
        session.sql(query)
        .limit(limit)
        .to_pandas()
    )
    return df


def main():
    st.title("NBA Fantasy – Z-Score Rankings")

    # ---- Sidebar controls ----
    st.sidebar.header("Options")

    query_key = st.sidebar.selectbox(
        "Choose dataset",
        options=list(QUERIES.keys()),
        index=0,
    )

    row_limit = st.sidebar.slider(
        "Max rows to load",
        min_value=50,
        max_value=5000,
        step=50,
        value=500,
        help="Limit how many ranked players to pull from Snowflake.",
    )

    df = load_data(query_key, row_limit)

    if df.empty:
        st.warning("No rows returned for the current season.")
        return

    st.subheader(f"{query_key} ({len(df)} rows loaded)")

    # ---- Sorting controls ----
    display_df = df

    # Figure out a good default sort column
    cols = display_df.columns.tolist()
    sort_col_default = (
        "PLAYER_RANK"
        if "PLAYER_RANK" in cols
        else ("TOTALZ" if "TOTALZ" in cols else cols[0])
    )

    sort_col = st.selectbox(
        "Sort by column",
        options=cols,
        index=cols.index(sort_col_default),
    )

    sort_dir = st.radio(
        "Sort direction",
        options=["Descending", "Ascending"],
        horizontal=True,
    )
    ascending = sort_dir == "Ascending"

    sorted_df = display_df.sort_values(by=sort_col, ascending=ascending)

    # ---- Heatmap styling on stat columns ----
    # Choose which columns to heatmap: totalZ + any *ZScore columns
    stat_cols = [
        c for c in sorted_df.columns
        if c.upper() == "TOTALZ" or c.upper().endswith("ZSCORE")
    ]

    # Use a pandas Styler to apply a background gradient to those columns
    styled_df = sorted_df.style.background_gradient(
        subset=stat_cols,
        cmap="RdYlGn"  # you can swap this for another matplotlib colormap name
    )

    # ---- Show data with heatmap ----
    st.dataframe(
        styled_df,
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
