import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd

session = get_active_session()

WEEKLY_STATS_TABLE = "NBA.FANTASY__MAIN.WEEKLY_STATS"
WEEKLY_BREAKOUTS_TABLE = "NBA.FANTASY__MAIN.WEEKLY_BREAKOUTS"
SEASON_FINAL_TABLE = "NBA.FANTASY__MAIN.SEASON_FINAL"


# ----------------------------
# Helpers
# ----------------------------
def normalize_intish(v):
    if v is None:
        return None
    try:
        return int(str(v))
    except Exception:
        return v


def first_existing_column(df, candidates):
    for c in candidates:
        if c and c in df.columns:
            return c
    return None


def safe_player_name(row):
    fn = row.get("FIRSTNAME")
    ln = row.get("FAMILYNAME")
    if pd.notna(fn) and pd.notna(ln):
        return f"{fn} {ln}"
    return str(row.get("PERSONID", "Unknown"))


def fmt_num(x):
    if x is None or pd.isna(x):
        return ""
    try:
        x = float(x)
        if abs(x) >= 10:
            return f"{x:.0f}"
        if abs(x) >= 1:
            return f"{x:.2f}"
        return f"{x:.3f}"
    except Exception:
        return str(x)


def category_leader_row(df, label, sum_candidates, z_candidates, pick_by="z"):
    sum_col = first_existing_column(df, sum_candidates)
    z_col = first_existing_column(df, z_candidates)

    rank_col = z_col if (pick_by == "z" and z_col is not None) else sum_col
    if rank_col is None or df[rank_col].notna().sum() == 0:
        return {"Category": label, "Player": "N/A", "Sum/Avg": "", "Z-Score": ""}

    tmp = df.copy()
    tmp[rank_col] = pd.to_numeric(tmp[rank_col], errors="coerce")
    idx = tmp[rank_col].idxmax()
    row = df.loc[idx]

    return {
        "Category": label,
        "Player": safe_player_name(row),
        "Sum/Avg": fmt_num(row[sum_col]) if sum_col else "",
        "Z-Score": fmt_num(row[z_col]) if z_col else "",
    }


# ----------------------------
# Data fetch
# ----------------------------
@st.cache_data(show_spinner=False)
def get_weeks():
    df = (
        session.table(WEEKLY_STATS_TABLE)
        .select("SEASONID", "WEEKNUMBER", "WEEKSTARTDATE", "WEEKENDDATE", "SEASONWEEKID")
        .distinct()
        .sort(col("SEASONID"), col("WEEKNUMBER"))
        .to_pandas()
    )
    df.columns = [c.upper() for c in df.columns]

    df["SEASONID_N"] = df["SEASONID"].apply(normalize_intish)
    df["WEEKNUMBER_N"] = df["WEEKNUMBER"].apply(normalize_intish)

    def fmt_date(v):
        return str(v)[:10] if v is not None and str(v) != "NaT" else "N/A"

    df["LABEL"] = df.apply(
        lambda r: f"SZN {r['SEASONID']} | Week {r['WEEKNUMBER']} | {fmt_date(r['WEEKSTARTDATE'])} → {fmt_date(r['WEEKENDDATE'])}",
        axis=1,
    )
    return df


@st.cache_data(show_spinner=False)
def get_weekly_stats(season_id_raw, week_number_raw):
    season_id = normalize_intish(season_id_raw)
    week_number = normalize_intish(week_number_raw)

    df = (
        session.table(WEEKLY_STATS_TABLE)
        .filter((col("SEASONID") == season_id) | (col("SEASONID") == str(season_id)))
        .filter((col("WEEKNUMBER") == week_number) | (col("WEEKNUMBER") == str(week_number)))
        .to_pandas()
    )
    df.columns = [c.upper() for c in df.columns]
    return df


@st.cache_data(show_spinner=False)
def get_weekly_breakouts(season_id_raw, week_number_raw, week_start_date_raw=None):
    season_id = normalize_intish(season_id_raw)
    week_number = normalize_intish(week_number_raw)

    t = session.table(WEEKLY_BREAKOUTS_TABLE)

    cols = [c.upper() for c in t.columns]
    has_weeknum = "WEEKNUMBER" in cols
    has_weekstart = "WEEKSTARTDATE" in cols

    if has_weeknum:
        t = (
            t.filter((col("SEASONID") == season_id) | (col("SEASONID") == str(season_id)))
             .filter((col("WEEKNUMBER") == week_number) | (col("WEEKNUMBER") == str(week_number)))
        )
    elif has_weekstart and week_start_date_raw is not None:
        t = (
            t.filter((col("SEASONID") == season_id) | (col("SEASONID") == str(season_id)))
             .filter(col("WEEKSTARTDATE") == week_start_date_raw)
        )
    else:
        return pd.DataFrame()

    df = t.to_pandas()
    df.columns = [c.upper() for c in df.columns]
    return df


@st.cache_data(show_spinner=False)
def get_season_final(season_id_raw):
    season_id = normalize_intish(season_id_raw)
    df = (
        session.table(SEASON_FINAL_TABLE)
        .filter((col("SEASONID") == season_id) | (col("SEASONID") == str(season_id)))
        .to_pandas()
    )
    df.columns = [c.upper() for c in df.columns]
    return df


# ----------------------------
# UI
# ----------------------------
def main():
    st.title("📆 Weekly Fantasy Dashboard")
    st.caption("Weekly z-scores + breakout pickups (same selected week).")

    weeks_df = get_weeks()
    if weeks_df.empty:
        st.warning(f"No weekly data found in {WEEKLY_STATS_TABLE}.")
        return

    default_index = len(weeks_df) - 1
    selected_label = st.selectbox(
        "Select a week (Mon–Sun):",
        options=weeks_df["LABEL"].tolist(),
        index=default_index,
    )

    selected = weeks_df.loc[weeks_df["LABEL"] == selected_label].iloc[0]
    season_id = selected["SEASONID"]
    week_number = selected["WEEKNUMBER"]
    week_start = selected["WEEKSTARTDATE"]
    week_end = selected["WEEKENDDATE"]

    st.caption(
        f"Season **{season_id}**, Week **{week_number}** "
        f"({str(week_start)[:10]} → {str(week_end)[:10]})"
    )

    week_df = get_weekly_stats(season_id, week_number)
    if week_df.empty:
        st.warning("No WEEKLY_STATS rows for that week.")
        return

    # -------- Leaders --------
    st.subheader("🏅 Weekly Category Leaders")

    total_z_col = first_existing_column(week_df, ["TOTALZWEEK"])
    leaders = [
        category_leader_row(week_df, "Total Z", [total_z_col], [total_z_col], pick_by="z"),
        category_leader_row(week_df, "Points", ["POINTSSUM"], ["POINTSZSCOREWEEK"]),
        category_leader_row(week_df, "Rebounds", ["REBOUNDSSUM"], ["REBOUNDSZSCOREWEEK"]),
        category_leader_row(week_df, "Assists", ["ASSISTSSUM"], ["ASSISTSZSCOREWEEK"]),
        category_leader_row(week_df, "Steals", ["STEALSSUM"], ["STEALSZSCOREWEEK"]),
        category_leader_row(week_df, "Blocks", ["BLOCKSSUM"], ["BLOCKSZSCOREWEEK"]),
        category_leader_row(week_df, "Threes", ["THREESSUM"], ["THREESZSCOREWEEK"]),
        category_leader_row(
            week_df,
            "Double-Doubles",
            ["DOUBLEDOBLESUM", "DOUBLEDOBBLESUM", "DOUBLEDoublesum".upper()],
            ["DOUBLEDOBLEZSCOREWEEK", "DOUBLEDoublesZScoreWeek".upper()],
        ),
        category_leader_row(week_df, "eFG%", ["EFGPCTAVG"], ["EFGZSCOREWEEK", "EFG_ZSCOREWEEK", "EFGZSCORE"]),
        category_leader_row(week_df, "FT%", ["FTPCTAVG"], ["FTZSCOREWEEK", "FT_ZSCOREWEEK", "FTZSCORE"]),
    ]

    st.dataframe(pd.DataFrame(leaders), use_container_width=True, hide_index=True)

    # -------- Top players --------
    st.subheader("🔥 Top Players This Week (by TOTALZWEEK)")

    if total_z_col and total_z_col in week_df.columns:
        tmp = week_df.copy()
        tmp[total_z_col] = pd.to_numeric(tmp[total_z_col], errors="coerce")
        top_df = tmp.sort_values(total_z_col, ascending=False).head(25)
    else:
        sort_col = first_existing_column(week_df, ["POINTSSUM", "ASSISTSSUM", "REBOUNDSSUM"]) or week_df.columns[0]
        st.info(f"Couldn't find TOTALZWEEK; sorting by {sort_col} instead.")
        top_df = week_df.sort_values(sort_col, ascending=False).head(25)

    preferred_cols = [c for c in [
        "FIRSTNAME","FAMILYNAME","PERSONID","GAMESPLAYEDWEEK",
        "POINTSSUM","ASSISTSSUM","REBOUNDSSUM","STEALSSUM","BLOCKSSUM","THREESSUM","DOUBLEDOBLESUM",
        total_z_col
    ] if c and c in top_df.columns]

    st.dataframe(top_df[preferred_cols], use_container_width=True, hide_index=True)
    
    # -------- Breakouts (Selected Week) --------
    st.subheader("🚀 Breakout Pickups (Selected Week)")
    
    try:
        break_df = get_weekly_breakouts(season_id, week_number, week_start_date_raw=week_start)
    except Exception as e:
        break_df = pd.DataFrame()
        st.error(f"Failed reading breakouts table: {e}")
    
    if break_df.empty:
        st.info(f"No rows returned from {WEEKLY_BREAKOUTS_TABLE} for this week.")
    else:
        # Default sort: minutes delta desc
        if "MINUTES_DELTA_WEEK" in break_df.columns:
            break_df["MINUTES_DELTA_WEEK"] = pd.to_numeric(
                break_df["MINUTES_DELTA_WEEK"], errors="coerce"
            )
            break_df = break_df.sort_values("MINUTES_DELTA_WEEK", ascending=False)
    
        st.dataframe(
            break_df,
            use_container_width=True,
            hide_index=True
        )

    # -------- Full tables --------
    with st.expander("📊 Full WEEKLY_STATS (Selected Week)", expanded=False):
        st.dataframe(week_df, use_container_width=True, hide_index=True)

    if not break_df.empty:
        with st.expander("📈 Full WEEKLY_BREAKOUTS (Selected Week)", expanded=False):
            st.dataframe(break_df, use_container_width=True, hide_index=True)

    # -------- Season final (NEW) --------
    st.subheader("🏁 Season Final (Averages + Cumulative + Z-Scores)")

    season_final_df = get_season_final(season_id)
    if season_final_df.empty:
        st.warning(f"No rows found in {SEASON_FINAL_TABLE} for season {season_id}.")
        return
    # ---- Rank by season cumulative Z ----
    if "ZCUM_TOTAL" in season_final_df.columns:
        # ensure numeric
        season_final_df["ZCUM_TOTAL"] = pd.to_numeric(
            season_final_df["ZCUM_TOTAL"], errors="coerce"
        )
    
        season_final_df["CUMULATIVE_RANK"] = (
            season_final_df["ZCUM_TOTAL"]
            .rank(method="dense", ascending=False)
            .astype("Int64")
        )
    
        # optional: move rank column to front
        cols = ["CUMULATIVE_RANK"] + [
            c for c in season_final_df.columns if c != "CUMULATIVE_RANK"
        ]
        season_final_df = season_final_df[cols]
    if "ZAVG_TOTAL" in season_final_df.columns:
        # ensure numeric
        season_final_df["ZAVG_TOTAL"] = pd.to_numeric(
            season_final_df["ZAVG_TOTAL"], errors="coerce"
        )
    
        season_final_df["AVERAGE_RANK"] = (
            season_final_df["ZAVG_TOTAL"]
            .rank(method="dense", ascending=False)
            .astype("Int64")
        )
    
        # optional: move rank column to front
        cols = ["AVERAGE_RANK"] + [
            c for c in season_final_df.columns if c != "AVERAGE_RANK"
        ]
        season_final_df = season_final_df[cols]

    # sensible default sort (try zavg_total first)
    default_sort = "ZCUM_TOTAL" if "ZCUM_TOTAL" in season_final_df.columns else (
        "ZAVG_TOTAL" if "ZAVG_TOTAL" in season_final_df.columns else None
    )
    if default_sort:
        tmp = season_final_df.copy()
        tmp[default_sort] = pd.to_numeric(tmp[default_sort], errors="coerce")
        season_final_df = tmp.sort_values(default_sort, ascending=False)

    st.dataframe(season_final_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
