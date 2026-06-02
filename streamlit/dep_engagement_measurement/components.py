import streamlit as st
import pandas as pd
import altair as alt

from queries import load_user_breakdown


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.upper() for c in df.columns]
    return df


def trend_chart(df, y_col, color_col, title, color_map):
    df = df.copy()
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%b %d")
    return (
        alt.Chart(df)
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=60), strokeWidth=2.5)
        .encode(
            x=alt.X("DATE:O", title="Date", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y(f"{y_col}:Q", title="% of Total", scale=alt.Scale(domain=[0, 100]), axis=alt.Axis(values=[0,25,50,75,100])),
            color=alt.Color(
                f"{color_col}:N",
                scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())),
                legend=alt.Legend(orient="bottom"),
            ),
            tooltip=[
                alt.Tooltip("DATE:O", title="Date"),
                alt.Tooltip(f"{color_col}:N", title="Segment"),
                alt.Tooltip(f"{y_col}:Q", title="% Total", format=".1f"),
            ],
        )
        .properties(title=title, height=280)
    )


def dealer_table(df: pd.DataFrame, pct_cols: list[str]):
    col_config = {
        c: st.column_config.ProgressColumn(c, min_value=0, max_value=100, format="%.1f%%")
        for c in pct_cols
    }
    st.dataframe(df, use_container_width=True, hide_index=True, column_config=col_config)


def user_breakdown_section(tab_key: str, product_section: str, days: int, df_dealer: pd.DataFrame, session, engagement_only: bool):
    st.divider()
    st.subheader("User breakdown · drill into a dealer")

    options = [None] + [
        (int(row["DEALER_ID"]), row["DEALER_NAME"] or f"ID {row['DEALER_ID']}")
        for _, row in df_dealer.iterrows()
    ]
    selected = st.selectbox(
        "Select dealer",
        options=options,
        format_func=lambda x: "— select a dealer —" if x is None else f"{x[0]} · {x[1]}",
        key=f"dealer_input_{tab_key}",
    )
    if selected is None:
        return

    dealer_id = selected[0]
    with st.spinner("Loading user data…"):
        df_users = normalize_cols(load_user_breakdown(session, dealer_id, product_section, days, engagement_only))

    if df_users.empty:
        st.info("No users found for this dealer.")
        return

    st.dataframe(
        df_users.rename(columns={
            "USER_ID":      "User ID",
            "EMAIL":        "Email",
            "PAGE_VIEWS":   "Page Views",
            "INTERACTIONS": "Interactions",
        }),
        use_container_width=True,
        hide_index=True,
    )
