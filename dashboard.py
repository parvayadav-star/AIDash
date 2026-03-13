import os
import math
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
st.set_page_config(page_title="Call Analytics", layout="wide")

# ── Supabase client ───────────────────────────────────────────────────────────
def _sb():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except Exception:
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_KEY", "")
    return create_client(url, key)

# ── Status groupings ──────────────────────────────────────────────────────────
# call_in_queue  → not yet dialed
# call_placed    → dialed, ringing (no answer yet)
# could_not_connect → dialed, never answered
# completed      → answered & finished
# call_hangup    → ringing but hung up
# agent_errored / call_errored → system error

DIALED_STATUSES   = {"call_placed", "could_not_connect", "completed", "call_hangup",
                     "agent_errored", "call_errored"}
RINGING_STATUSES  = {"could_not_connect", "completed", "call_hangup"}   # rang but may not have been answered
ANSWERED_STATUSES = {"completed"}


def classify_number(num):
    s = str(num).strip() if pd.notna(num) else ""
    if not s or s in ("nan", "-"):
        return "missing"
    if "_" in s:
        return "has_underscore"
    if s.startswith("++"):
        return "double_plus"
    if not s.startswith("+"):
        return "no_plus"
    digits = s[1:]
    if not digits.isdigit():
        return "non_numeric"
    if len(digits) != 12:          # +91 + 10 digits = 12 digits after +
        return "invalid_length"
    return "valid"


@st.cache_data(ttl=300)   # auto-refresh every 5 minutes
def load_data():
    sb   = _sb()
    rows = []
    PAGE = 1000
    offset = 0
    while True:
        result = sb.table("calls").select("*").range(offset, offset + PAGE - 1).execute()
        rows.extend(result.data)
        if len(result.data) < PAGE:
            break
        offset += PAGE
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Map DB snake_case columns → original display names used throughout the app
    df = df.rename(columns={
        "number": "Number", "time": "Time", "use_case": "Use Case",
        "call_status": "Call Status", "duration": "Duration",
        "agent_number": "Agent Number", "recording_url": "Recording URL",
        "summary": "Analysis.summary", "user_sentiment": "Analysis.user_sentiment",
        "task_completion": "Analysis.task_completion", "issue_status": "Analysis.issue_status",
        "call_quality": "Analysis.call_quality", "status": "Analysis.status",
        "activity_status": "Analysis.activity_status", "activity_time": "Analysis.activity_time",
        "call_summary": "Analysis.call_summary", "long_hault_reason": "Analysis.long_hault_reason",
        "not_interested_reason": "Analysis.not_interested_reason",
        "notice_period": "Analysis.notice_period", "is_jaipur_based": "Analysis.is_jaipur_based",
        "can_operate_laptop": "Analysis.can_operate_laptop",
        "preferred_device": "Analysis.preferred_device",
        "can_handle_documents": "Analysis.can_handle_documents",
        "is_over_qualified": "Analysis.is_over_qualified", "is_misaligned": "Analysis.is_misaligned",
        "knows_excel_or_sheets": "Analysis.knows_excel_or_sheets",
        "preferred_date": "Analysis.preferred_date", "expected_salary": "Analysis.expected_salary",
        "status_for_next_round": "Analysis.status_for_next_round",
        "interview_preferred_date": "Analysis.interview_preferred_date",
        "current_salary": "Analysis.current_salary",
        "number_category": "Number Category",
    })
    df = df.drop(columns=["ingested_at"], errors="ignore")
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce", utc=True).dt.tz_localize(None)
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce")
    return df


df = load_data()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

date_min = df["Time"].min().date()
date_max = df["Time"].max().date()
date_range = st.sidebar.date_input(
    "Date range", value=(date_min, date_max), min_value=date_min, max_value=date_max
)

use_cases = sorted(df["Use Case"].dropna().unique().tolist())
selected_use_cases = st.sidebar.multiselect("Use Case", use_cases, default=use_cases)

statuses = sorted(df["Call Status"].dropna().unique().tolist())
selected_statuses = st.sidebar.multiselect("Call Status", statuses, default=statuses)

# Apply filters
mask = (
    (df["Time"].dt.date >= date_range[0])
    & (df["Time"].dt.date <= date_range[1])
    & (df["Use Case"].isin(selected_use_cases))
    & (df["Call Status"].isin(selected_statuses))
)
fdf = df[mask]

# ── Title ─────────────────────────────────────────────────────────────────────
st.title("Call Analytics Dashboard")
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
st.caption(f"Showing {len(fdf):,} of {len(df):,} records")

# ── KPI row ───────────────────────────────────────────────────────────────────
completed_df = fdf[fdf["Call Status"].isin(ANSWERED_STATUSES)]
dialed_df    = fdf[fdf["Call Status"].isin(DIALED_STATUSES)]
pick_rate    = len(completed_df) / len(dialed_df) * 100 if len(dialed_df) else 0
avg_dur      = completed_df["Duration"].mean() if len(completed_df) else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Calls", f"{len(fdf):,}")
k2.metric("Completed", f"{len(completed_df):,}")
k3.metric("Pick Rate (of dialed)", f"{pick_rate:.1f}%")
k4.metric("Avg Duration (completed)", f"{avg_dur:.0f}s")

st.divider()

# ── Section 1: Overview charts ────────────────────────────────────────────────
with st.expander("Overview Charts", expanded=True):
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Call Status Breakdown")
        sc = fdf["Call Status"].value_counts().reset_index()
        sc.columns = ["Status", "Count"]
        fig = px.bar(sc, x="Status", y="Count", color="Status", text="Count")
        fig.update_layout(showlegend=False, margin=dict(t=20))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Use Case Distribution")
        uc = fdf["Use Case"].value_counts().reset_index()
        uc.columns = ["Use Case", "Count"]
        fig2 = px.bar(uc, x="Count", y="Use Case", orientation="h", text="Count", color="Use Case")
        fig2.update_layout(showlegend=False, margin=dict(t=20), yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Calls Over Time")
    gran = st.radio("Granularity", ["Hourly", "Daily"], horizontal=True, key="time_gran")
    freq = "h" if gran == "Hourly" else "D"
    ts = fdf.set_index("Time").resample(freq).size().reset_index(name="Calls")
    fig3 = px.line(ts, x="Time", y="Calls", markers=True)
    fig3.update_layout(margin=dict(t=20))
    st.plotly_chart(fig3, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("User Sentiment (completed calls)")
        sent = completed_df["Analysis.user_sentiment"].dropna()
        if not sent.empty:
            sc2 = sent.value_counts().reset_index()
            sc2.columns = ["Sentiment", "Count"]
            cmap = {"Positive": "#2ecc71", "Negative": "#e74c3c", "Neutral": "#95a5a6"}
            fig4 = px.pie(sc2, names="Sentiment", values="Count",
                          color="Sentiment", color_discrete_map=cmap)
            fig4.update_layout(margin=dict(t=20))
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No sentiment data in current filter.")

    with col4:
        st.subheader("Task Completion (completed calls)")
        tc = completed_df["Analysis.task_completion"].dropna()
        if not tc.empty:
            tc2 = tc.value_counts().reset_index()
            tc2.columns = ["Completed", "Count"]
            fig5 = px.bar(tc2, x="Completed", y="Count", color="Completed", text="Count")
            fig5.update_layout(showlegend=False, margin=dict(t=20))
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.info("No task completion data in current filter.")

st.divider()

# ── Section 2: Agent Number Performance ──────────────────────────────────────
with st.expander("Agent Number Performance", expanded=True):
    st.subheader("Per-Agent Call Rates")
    st.caption(
        "Rates are calculated on **valid-number calls only** (invalid numbers excluded).  "
        "**Dial Rate** = attempted / valid calls  |  "
        "**Place Rate** = rang / attempted  |  "
        "**Pick Rate** = completed / rang"
    )

    # Use the full filtered df (ignore Call Status filter for this section so all statuses show)
    agent_mask = (
        (df["Time"].dt.date >= date_range[0])
        & (df["Time"].dt.date <= date_range[1])
        & (df["Use Case"].isin(selected_use_cases))
    )
    adf = df[agent_mask]

    def agent_stats(g):
        total    = len(g)
        invalid  = (g["Number Category"] != "valid").sum()
        v        = g[g["Number Category"] == "valid"]   # valid-number calls only
        dialed   = v["Call Status"].isin(DIALED_STATUSES).sum()
        ringing  = v["Call Status"].isin(RINGING_STATUSES).sum()
        answered = v["Call Status"].isin(ANSWERED_STATUSES).sum()
        avg_d    = v.loc[v["Call Status"].isin(ANSWERED_STATUSES), "Duration"].mean()
        return pd.Series({
            "Total Calls":        total,
            "Invalid Numbers":    invalid,
            "Valid Number Calls": len(v),
            "Dialed (valid)":     dialed,
            "Rang (valid)":       ringing,
            "Completed (valid)":  answered,
            "Dial Rate %":        round(dialed   / len(v)   * 100, 1) if len(v)   else 0,
            "Place Rate %":       round(ringing  / dialed   * 100, 1) if dialed   else 0,
            "Pick Rate %":        round(answered / ringing  * 100, 1) if ringing  else 0,
            "Avg Duration (s)":   round(avg_d, 1) if pd.notna(avg_d) else 0,
        })

    agent_table = adf.groupby("Agent Number").apply(agent_stats).reset_index()
    agent_table = agent_table.sort_values("Total Calls", ascending=False)
    st.dataframe(agent_table, use_container_width=True, hide_index=True)

    # Bar chart
    rate_col = st.selectbox("Visualise rate", ["Dial Rate %", "Place Rate %", "Pick Rate %"],
                            key="agent_rate_col")
    fig_a = px.bar(agent_table, x="Agent Number", y=rate_col, text=rate_col, color="Agent Number")
    fig_a.update_layout(showlegend=False, margin=dict(t=20), yaxis_range=[0, 100])
    st.plotly_chart(fig_a, use_container_width=True)

st.divider()

# ── Section 2b: Day-on-Day Rate Trends ───────────────────────────────────────
with st.expander("Day-on-Day Rate Trends", expanded=True):
    st.subheader("Daily Dial / Place / Pick Rates")
    st.caption("Valid-number calls only. Use the toggles to slice by Agent or Use Case.")

    trend_mask = (
        (df["Time"].dt.date >= date_range[0])
        & (df["Time"].dt.date <= date_range[1])
        & (df["Use Case"].isin(selected_use_cases))
        & (df["Number Category"] == "valid")
    )
    tdf = df[trend_mask].copy()
    tdf["Date"] = tdf["Time"].dt.date

    def daily_rates(g):
        dialed   = g["Call Status"].isin(DIALED_STATUSES).sum()
        ringing  = g["Call Status"].isin(RINGING_STATUSES).sum()
        answered = g["Call Status"].isin(ANSWERED_STATUSES).sum()
        total    = len(g)
        return pd.Series({
            "Dial Rate %":  round(dialed   / total   * 100, 1) if total   else None,
            "Place Rate %": round(ringing  / dialed  * 100, 1) if dialed  else None,
            "Pick Rate %":  round(answered / ringing * 100, 1) if ringing else None,
            "Calls":        total,
        })

    trend_by = st.radio("Break down by", ["Agent Number", "Use Case"],
                        horizontal=True, key="trend_by")
    rate_sel  = st.selectbox("Rate to plot", ["Dial Rate %", "Place Rate %", "Pick Rate %"],
                             key="trend_rate")

    daily = tdf.groupby(["Date", trend_by]).apply(daily_rates).reset_index()
    daily["Date"] = pd.to_datetime(daily["Date"])

    # Filter which agents/use-cases to show
    all_vals = sorted(daily[trend_by].dropna().unique().tolist())
    chosen_vals = st.multiselect(f"Select {trend_by}(s)", all_vals, default=all_vals,
                                 key="trend_vals")
    plot_data = daily[daily[trend_by].isin(chosen_vals)]

    if plot_data.empty:
        st.info("No data for the selected filters.")
    else:
        fig_t = px.line(
            plot_data, x="Date", y=rate_sel, color=trend_by,
            markers=True, hover_data=["Calls"],
            labels={rate_sel: rate_sel, "Date": "Date"},
        )
        fig_t.update_layout(margin=dict(t=20), yaxis_range=[0, 100],
                            yaxis_ticksuffix="%")
        st.plotly_chart(fig_t, use_container_width=True)

        # Underlying numbers table
        with st.expander("Show underlying daily numbers"):
            pivot_trend = plot_data.pivot_table(
                index="Date", columns=trend_by, values=rate_sel
            ).round(1)
            st.dataframe(pivot_trend, use_container_width=True)

st.divider()

# ── Section 3: User Number Quality ───────────────────────────────────────────
with st.expander("User Number Quality", expanded=True):
    st.subheader("Number Format Classification")

    CAT_COLORS = {
        "valid":           "#2ecc71",
        "double_plus":     "#e67e22",
        "no_plus":         "#3498db",
        "has_underscore":  "#e74c3c",
        "invalid_length":  "#9b59b6",
        "non_numeric":     "#95a5a6",
        "missing":         "#bdc3c7",
    }
    CAT_LABELS = {
        "valid":           "Valid",
        "double_plus":     "Double plus (++)",
        "no_plus":         "No plus prefix",
        "has_underscore":  "Has underscore",
        "invalid_length":  "Invalid digit count",
        "non_numeric":     "Non-numeric chars",
        "missing":         "Missing / blank",
    }

    cat_counts = (
        fdf["Number Category"]
        .map(CAT_LABELS)
        .value_counts()
        .reset_index()
    )
    cat_counts.columns = ["Category", "Count"]
    fig_nc = px.bar(cat_counts, x="Category", y="Count", color="Category", text="Count",
                    color_discrete_map={v: CAT_COLORS[k] for k, v in CAT_LABELS.items()})
    fig_nc.update_layout(showlegend=False, margin=dict(t=20))
    st.plotly_chart(fig_nc, use_container_width=True)

    # Detailed table per category
    st.markdown("**Sample records by category**")
    chosen_cat = st.selectbox(
        "Show records for category",
        options=list(CAT_LABELS.keys()),
        format_func=lambda k: CAT_LABELS[k],
        key="num_cat_sel"
    )
    preview_cols = ["Number", "Number Category", "Time", "Use Case", "Call Status",
                    "Agent Number", "Duration"]
    cat_preview = fdf[fdf["Number Category"] == chosen_cat][preview_cols]
    st.dataframe(cat_preview.head(200), use_container_width=True, hide_index=True)
    st.caption(f"{len(cat_preview):,} total records in this category")

st.divider()

# ── Section 4: Table Canvas (Pivot Builder) ───────────────────────────────────
with st.expander("Table Canvas — Build Any Table", expanded=True):
    st.subheader("Custom Pivot Table")
    st.caption("Select row dimension(s), an optional column dimension, and a metric to aggregate.")

    all_cols = [c for c in fdf.columns if fdf[c].nunique() < 500 or c == "Number Category"]
    cat_cols = [c for c in fdf.columns
                if fdf[c].dtype == object or c in ("Number Category",)]
    num_cols = ["Duration"] + [c for c in fdf.columns
                               if fdf[c].dtype in ("float64", "int64") and c != "Duration"]

    pc1, pc2, pc3 = st.columns(3)

    with pc1:
        row_dims = st.multiselect(
            "Row dimension(s)", cat_cols,
            default=["Agent Number"],
            key="pivot_rows"
        )
    with pc2:
        col_dim_opts = ["— none —"] + cat_cols
        col_dim = st.selectbox("Column dimension (optional)", col_dim_opts, key="pivot_col")
        col_dim = None if col_dim == "— none —" else col_dim
    with pc3:
        metric = st.selectbox(
            "Metric",
            ["Count"] + num_cols,
            key="pivot_metric"
        )
        agg_fn = "count"
        if metric != "Count":
            agg_fn = st.selectbox("Aggregation", ["sum", "mean", "min", "max", "median"],
                                  key="pivot_agg")

    if not row_dims:
        st.info("Select at least one row dimension.")
    else:
        try:
            if metric == "Count":
                if col_dim:
                    pivot = pd.crosstab(
                        [fdf[r] for r in row_dims],
                        fdf[col_dim],
                        margins=True, margins_name="Total"
                    )
                else:
                    pivot = fdf.groupby(row_dims).size().reset_index(name="Count")
            else:
                if col_dim:
                    pivot = fdf.pivot_table(
                        index=row_dims, columns=col_dim,
                        values=metric, aggfunc=agg_fn,
                        margins=True, margins_name="Total"
                    ).round(1)
                else:
                    pivot = fdf.groupby(row_dims)[metric].agg(agg_fn).reset_index()
                    pivot.columns = list(pivot.columns[:-1]) + [f"{agg_fn}({metric})"]
                    pivot = pivot.round(1)

            st.dataframe(pivot, use_container_width=True)
            st.caption(f"Rows shown: {len(pivot):,}")

            # Download button
            csv_bytes = pivot.to_csv().encode()
            st.download_button(
                "Download as CSV", data=csv_bytes,
                file_name="pivot_export.csv", mime="text/csv"
            )
        except Exception as e:
            st.error(f"Could not build table: {e}")

st.divider()

# ── Section 5: Nth Call Analysis ──────────────────────────────────────────────
with st.expander("Nth Call Analysis", expanded=True):
    st.subheader("Performance by Call Attempt Number")
    st.caption("Valid-number calls only, sorted by number → time. Each row = the nth time we called that number.")

    nth_base = fdf[fdf["Number Category"] == "valid"].copy()
    nth_base = nth_base.sort_values(["Number", "Time"])
    nth_base["call_number"] = nth_base.groupby("Number").cumcount() + 1

    tab_n1, tab_n2, tab_n3 = st.tabs(["Call Number Table", "Pickup Rate Trend", "Frequency Heatmap"])

    with tab_n1:
        nth_stats = (
            nth_base.groupby("call_number")
            .agg(
                total        =("call_number", "count"),
                picked_up    =("Call Status", lambda x: x.isin(ANSWERED_STATUSES).sum()),
                task_done    =("Analysis.task_completion", lambda x: (x == "true").sum()),
                neg_sentiment=("Analysis.user_sentiment", lambda x: x.str.lower().eq("negative").sum()),
            )
            .reset_index()
        )
        nth_stats["Pick Rate %"]        = (nth_stats["picked_up"] / nth_stats["total"] * 100).round(1)
        nth_stats["Task Done % (of picked)"] = (
            nth_stats["task_done"] / nth_stats["picked_up"].replace(0, np.nan) * 100
        ).fillna(0).round(1)
        nth_stats = nth_stats.rename(columns={
            "call_number": "Nth Call", "total": "Total Calls",
            "picked_up": "Picked Up", "task_done": "Task Done", "neg_sentiment": "Negative Sentiment",
        })
        st.dataframe(nth_stats, use_container_width=True, hide_index=True)
        st.download_button("Download", nth_stats.to_csv(index=False).encode(),
                           "nth_call.csv", "text/csv")

    with tab_n2:
        fig_nth = px.line(nth_stats, x="Nth Call", y="Pick Rate %",
                          markers=True, title="Pickup Rate by Call Attempt")
        fig_nth.update_layout(margin=dict(t=40), yaxis_range=[0, 100],
                              yaxis_ticksuffix="%")
        st.plotly_chart(fig_nth, use_container_width=True)

        # Also show task done rate trend
        fig_nth2 = px.line(nth_stats, x="Nth Call", y="Task Done % (of picked)",
                           markers=True, title="Task Completion Rate by Call Attempt",
                           color_discrete_sequence=["#2ecc71"])
        fig_nth2.update_layout(margin=dict(t=40), yaxis_range=[0, 100],
                               yaxis_ticksuffix="%")
        st.plotly_chart(fig_nth2, use_container_width=True)

    with tab_n3:
        st.caption("How many completed calls each user had vs total calls made to them.")
        user_summary = (
            nth_base.groupby("Number")
            .agg(
                total_calls    =("Number", "count"),
                completed_calls=("Call Status", lambda x: x.isin(ANSWERED_STATUSES).sum()),
            )
            .reset_index()
        )
        user_summary["total_bucket"]     = user_summary["total_calls"].clip(upper=10)
        user_summary["completed_bucket"] = user_summary["completed_calls"].clip(upper=10)

        freq = (
            user_summary.groupby(["total_bucket", "completed_bucket"])
            .size().reset_index(name="users")
        )
        pivot_h = freq.pivot(index="total_bucket", columns="completed_bucket", values="users").fillna(0)
        pct_h   = pivot_h.div(pivot_h.sum(axis=1), axis=0) * 100

        # Mask impossible cells (completed > total)
        mask = np.array([[c > r for c in pct_h.columns] for r in pct_h.index])
        pct_masked = pct_h.where(~mask)

        fig_hm = go.Figure(go.Heatmap(
            z=pct_masked.values,
            x=[f"{int(c)}+" if c == 10 else str(int(c)) for c in pct_masked.columns],
            y=[f"{int(r)}+" if r == 10 else str(int(r)) for r in pct_masked.index],
            colorscale="YlGnBu",
            text=pct_masked.values.round(1),
            texttemplate="%{text:.1f}%",
            colorbar=dict(title="%"),
        ))
        fig_hm.update_layout(
            title="% of Users by Total Calls vs Calls Picked Up",
            xaxis_title="Calls Picked Up",
            yaxis_title="Total Calls Made to Number",
            height=500, margin=dict(t=40),
        )
        st.plotly_chart(fig_hm, use_container_width=True)

st.divider()

# ── Section 6: Daily Deviation Analysis ───────────────────────────────────────
with st.expander("Daily Deviation Analysis", expanded=True):
    st.subheader("Day-on-Day Deviations from Average")
    st.caption(
        "Shows how each dimension's daily rate deviates from its own overall average. "
        "Green = above average, Red = below average. Valid-number calls only."
    )

    dev_base = df[
        (df["Time"].dt.date >= date_range[0])
        & (df["Time"].dt.date <= date_range[1])
        & (df["Use Case"].isin(selected_use_cases))
        & (df["Number Category"] == "valid")
    ].copy()
    dev_base["Date"] = dev_base["Time"].dt.date

    dev_dim    = st.radio("Break down by", ["Use Case", "Agent Number"],
                          horizontal=True, key="dev_dim")
    dev_metric = st.selectbox("Metric", ["Pick Rate %", "Place Rate %", "Dial Rate %"],
                              key="dev_metric")

    def compute_rates(g):
        total   = len(g)
        dialed  = g["Call Status"].isin(DIALED_STATUSES).sum()
        ringing = g["Call Status"].isin(RINGING_STATUSES).sum()
        answered= g["Call Status"].isin(ANSWERED_STATUSES).sum()
        return pd.Series({
            "Dial Rate %":  round(dialed   / total   * 100, 1) if total   else None,
            "Place Rate %": round(ringing  / dialed  * 100, 1) if dialed  else None,
            "Pick Rate %":  round(answered / ringing * 100, 1) if ringing else None,
            "Calls": total,
        })

    daily_dev = dev_base.groupby(["Date", dev_dim]).apply(compute_rates).reset_index()
    daily_dev["Date"] = pd.to_datetime(daily_dev["Date"])

    # Compute overall average per dimension value
    overall_avg = daily_dev.groupby(dev_dim)[dev_metric].mean()
    daily_dev["Overall Avg"] = daily_dev[dev_dim].map(overall_avg)
    daily_dev["Deviation"]   = (daily_dev[dev_metric] - daily_dev["Overall Avg"]).round(1)

    # Filter dimension values
    all_dim_vals = sorted(daily_dev[dev_dim].dropna().unique().tolist())
    chosen_dims  = st.multiselect(f"Select {dev_dim}(s)", all_dim_vals, default=all_dim_vals,
                                  key="dev_vals")
    plot_dev = daily_dev[daily_dev[dev_dim].isin(chosen_dims)]

    if plot_dev.empty:
        st.info("No data.")
    else:
        # Line chart: actual rate vs overall avg per dimension
        fig_dev = px.line(plot_dev, x="Date", y=dev_metric, color=dev_dim,
                          markers=True, hover_data=["Calls", "Overall Avg", "Deviation"])
        fig_dev.update_layout(margin=dict(t=20), yaxis_range=[0, 100],
                              yaxis_ticksuffix="%")
        st.plotly_chart(fig_dev, use_container_width=True)

        # Deviation pivot table with colour
        dev_pivot = plot_dev.pivot_table(
            index="Date", columns=dev_dim, values="Deviation"
        ).round(1)
        dev_pivot.index = dev_pivot.index.astype(str)

        st.markdown("**Deviation from each dimension's own average (percentage points)**")
        st.dataframe(
            dev_pivot.style.background_gradient(cmap="RdYlGn", axis=None, vmin=-20, vmax=20),
            use_container_width=True,
        )

        st.download_button(
            "Download deviation table",
            data=dev_pivot.to_csv().encode(),
            file_name="daily_deviation.csv", mime="text/csv"
        )
