#!/usr/bin/env python3
"""
Loan Collection Dashboard - Live DB Edition
============================================
Reads from finguard_oto_april PostgreSQL database in real-time.
Auto-refreshes every 60 seconds.

Usage:
    python dashboard_app.py [--port PORT] [--debug] [--days DAYS]

Dependencies:
    pip install -r requirements.txt
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np

import dash
from dash import html, dcc, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from db import get_activities_df, get_db_status
from transforms import compute_metrics

# ─── Configuration ────────────────────────────────────────────────────────────
DEFAULT_DAYS = int(os.environ.get("DEFAULT_DAYS", 7))
REFRESH_INTERVAL_MS = int(os.environ.get("REFRESH_INTERVAL_MS", 60_000))  # 60 seconds

CATEGORY_COLORS = {
    "Financial Hardship": "#FF4444",
    "Family Emergency": "#9B59B6",
    "Health Issues": "#FF8C00",
    "Dispute": "#FFD700",
    "Payment Commitment": "#2ECC71",
    "Wrong Number": "#95A5A6",
    "Busy/No Answer": "#3498DB",
    "General/Uncategorized": "#BDC3C7",
}

SENTIMENT_COLORS = {"positive": "#2ECC71", "neutral": "#95A5A6", "negative": "#E74C3C"}

CHANNEL_COLORS = {
    "AI Call": "#3498DB",
    "AI ASSISTANT": "#9B59B6",
    "SMS": "#1ABC9C",
    "Whatsapp": "#27AE60",
    "Manual Call": "#E67E22",
    "API Call": "#95A5A6",
}

PRIORITY_COLORS = {"High": "#E74C3C", "Medium": "#F39C12", "Low": "#95A5A6"}

DARK_CARD = {"backgroundColor": "#1e2130", "border": "1px solid #2d3250"}
DARK_TABLE_HEADER = {"backgroundColor": "#1a1a2e", "color": "#e0e0e0", "fontWeight": "bold", "fontSize": "12px"}
DARK_TABLE_CELL = {
    "backgroundColor": "#16213e", "color": "#e0e0e0", "fontSize": "12px",
    "padding": "8px", "textAlign": "left", "maxWidth": "200px",
    "overflow": "hidden", "textOverflow": "ellipsis",
}


# ─── Data loader (combines DB + transforms) ───────────────────────────────────
def get_data(days: int = DEFAULT_DAYS) -> dict:
    """Fetch + process data. DB result cached 60s; transform runs on each call."""
    df = get_activities_df(days)
    return compute_metrics(df)


# ─── App setup ────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="Loan Collection Dashboard",
)
server = app.server  # expose for gunicorn


# ─── Helpers ──────────────────────────────────────────────────────────────────
def kpi_card(title: str, value: str, subtitle: str = "", color: str = "primary") -> dbc.Card:
    return dbc.Card([
        dbc.CardBody([
            html.H6(title, className="text-muted mb-2",
                    style={"fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "0.5px"}),
            html.H2(str(value), className=f"text-{color} mb-1", style={"fontWeight": "700"}),
            html.Small(subtitle, className="text-muted") if subtitle else None,
        ])
    ], className="shadow-sm h-100", style={"borderLeft": f"3px solid var(--bs-{color})", **DARK_CARD})


def empty_fig(message: str = "No data available") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#666"))
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", height=300)
    return fig


# ─── Layout ───────────────────────────────────────────────────────────────────
app.layout = dbc.Container([
    # Auto-refresh interval
    dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),

    # Days selector store
    dcc.Store(id="days-store", data=DEFAULT_DAYS),

    # ── Header ──
    dbc.Row([
        dbc.Col([
            html.H2("Loan Collection Dashboard", className="mb-0 fw-bold"),
            html.P(id="header-subtitle", className="text-muted small mb-0",
                   children="Loading..."),
        ], width=8),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Select(
                        id="days-select",
                        options=[
                            {"label": "Last 7 days", "value": 7},
                            {"label": "Last 14 days", "value": 14},
                            {"label": "Last 30 days", "value": 30},
                        ],
                        value=DEFAULT_DAYS,
                        size="sm",
                        style={"backgroundColor": "#2d3250", "color": "#fff", "border": "1px solid #444"},
                    ),
                ], width="auto"),
                dbc.Col([
                    dbc.Badge(id="live-badge", children="● Loading", color="warning", className="p-2"),
                ], width="auto", className="d-flex align-items-center"),
            ], className="justify-content-end g-2"),
            html.Small(id="last-updated", className="text-muted d-block text-end mt-1"),
        ], width=4),
    ], className="mb-4 pt-3"),

    # ── Tabs ──
    dbc.Tabs([

        # ── TAB 1: Executive Overview ──
        dbc.Tab(label="Executive Overview", tab_id="tab-executive", children=[
            html.Div(className="py-3", children=[
                dbc.Row(id="kpi-row", className="mb-3"),
                dbc.Row([
                    dbc.Col([dcc.Graph(id="daily-trend")], lg=8, className="mb-3"),
                    dbc.Col([dcc.Graph(id="channel-pie")], lg=4, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([dcc.Graph(id="channel-bar")], lg=6, className="mb-3"),
                    dbc.Col([dcc.Graph(id="portfolio-gauge")], lg=6, className="mb-3"),
                ]),
            ])
        ]),

        # ── TAB 2: Operations ──
        dbc.Tab(label="Operations", tab_id="tab-ops", children=[
            html.Div(className="py-3", children=[
                dbc.Row([
                    dbc.Col([dcc.Graph(id="disposition-chart")], lg=7, className="mb-3"),
                    dbc.Col([dcc.Graph(id="flow-chart")], lg=5, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([dcc.Graph(id="hourly-chart")], lg=12, className="mb-3"),
                ]),
            ])
        ]),

        # ── TAB 3: Customer Intelligence ──
        dbc.Tab(label="Customer Intelligence", tab_id="tab-customer", id="tab-customer-label", children=[
            html.Div(className="py-3", children=[
                dbc.Row([
                    dbc.Col([dcc.Graph(id="category-chart")], lg=4, className="mb-3"),
                    dbc.Col([dcc.Graph(id="sentiment-chart")], lg=4, className="mb-3"),
                    dbc.Col([dcc.Graph(id="scatter-chart")], lg=4, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Row([
                            dbc.Col([html.H5("Escalation Queue", className="mb-0")]),
                            dbc.Col([
                                dbc.Select(
                                    id="priority-filter",
                                    options=[
                                        {"label": "All Priorities", "value": "All"},
                                        {"label": "High only", "value": "High"},
                                        {"label": "Medium only", "value": "Medium"},
                                    ],
                                    value="All",
                                    size="sm",
                                    style={"backgroundColor": "#2d3250", "color": "#fff",
                                           "border": "1px solid #444", "maxWidth": "150px"},
                                )
                            ], className="text-end"),
                        ], className="mb-3 align-items-center"),
                        dash_table.DataTable(
                            id="account-table",
                            columns=[
                                {"name": "Priority", "id": "priority"},
                                {"name": "Account ID", "id": "account_id"},
                                {"name": "Category", "id": "primary_category"},
                                {"name": "Touchpoints", "id": "touchpoints"},
                                {"name": "Connected", "id": "connected"},
                                {"name": "Sentiment", "id": "last_sentiment"},
                                {"name": "Last Contact", "id": "last_contact"},
                                {"name": "Summary", "id": "last_summary"},
                            ],
                            data=[],
                            page_size=15,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            style_header=DARK_TABLE_HEADER,
                            style_cell=DARK_TABLE_CELL,
                            style_data_conditional=[
                                {"if": {"filter_query": '{priority} = "High"'},
                                 "backgroundColor": "rgba(231, 76, 60, 0.1)",
                                 "borderLeft": "3px solid #E74C3C"},
                                {"if": {"filter_query": '{priority} = "Medium"'},
                                 "backgroundColor": "rgba(243, 156, 18, 0.05)",
                                 "borderLeft": "3px solid #F39C12"},
                            ],
                        ),
                    ], lg=12, className="mb-3"),
                ]),
            ])
        ]),

        # ── TAB 4: Analytics ──
        dbc.Tab(label="Analytics & Insights", tab_id="tab-analytics", children=[
            html.Div(className="py-3", children=[
                dbc.Row([
                    dbc.Col([dcc.Graph(id="ptp-chart")], lg=6, className="mb-3"),
                    dbc.Col([dcc.Graph(id="provider-chart")], lg=6, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([dcc.Graph(id="contact-chart")], lg=6, className="mb-3"),
                    dbc.Col([dcc.Graph(id="diversity-chart")], lg=6, className="mb-3"),
                ]),
            ])
        ]),

    ], className="mb-3"),

], fluid=True, className="px-4")


# ─── Callbacks ────────────────────────────────────────────────────────────────

@app.callback(
    Output("days-store", "data"),
    Input("days-select", "value"),
)
def update_days_store(val):
    return int(val)


@app.callback(
    [Output("header-subtitle", "children"),
     Output("last-updated", "children"),
     Output("live-badge", "children"),
     Output("live-badge", "color")],
    [Input("refresh-interval", "n_intervals"),
     Input("days-store", "data")],
)
def update_header(n, days):
    data = get_data(days)
    kpis = data["kpis"]
    date_range = f"Last {days} days"
    subtitle = (
        f"{kpis['total_accounts']:,} Accounts · {date_range} · "
        f"{kpis['total_activities']:,} Activities · "
        f"{kpis['high_priority_accounts']} flagged"
    )
    updated = f"Updated: {data['last_updated']}"
    return subtitle, updated, "● Live", "success"


@app.callback(
    Output("kpi-row", "children"),
    [Input("refresh-interval", "n_intervals"),
     Input("days-store", "data")],
)
def update_kpis(n, days):
    data = get_data(days)
    kpis = data["kpis"]
    total = kpis["total_accounts"]
    reached = kpis["accounts_reached"]
    pct = f"{reached / total * 100:.0f}%" if total else "0%"
    return [
        dbc.Col(kpi_card("Total Activities", f"{kpis['total_activities']:,}",
                         f"Last {days} days", "info"), lg=3, md=6, className="mb-3"),
        dbc.Col(kpi_card("Connection Rate", f"{kpis['connection_rate']}%",
                         "AI calls connected · target >15%", "danger"), lg=3, md=6, className="mb-3"),
        dbc.Col(kpi_card("Portfolio Reached", f"{reached}/{total}",
                         f"{pct} of accounts", "warning"), lg=3, md=6, className="mb-3"),
        dbc.Col(kpi_card("Payment Outcomes", f"{kpis['total_payments']:,}",
                         "Paid + Agreed + Claimed", "success"), lg=3, md=6, className="mb-3"),
    ]


@app.callback(
    [Output("daily-trend", "figure"),
     Output("channel-pie", "figure"),
     Output("channel-bar", "figure"),
     Output("portfolio-gauge", "figure")],
    [Input("refresh-interval", "n_intervals"),
     Input("days-store", "data")],
)
def update_executive(n, days):
    data = get_data(days)
    df = data["df"]
    ai_calls = data["ai_calls"]
    kpis = data["kpis"]

    if df.empty:
        return [empty_fig()] * 4

    # Daily trend
    daily = df.groupby("date").size().reset_index(name="activities")
    daily_conn = (
        ai_calls[ai_calls["disposition"] == "Connected"]
        .groupby("date").size().reset_index(name="connected")
    )
    daily = daily.merge(daily_conn, on="date", how="left").fillna(0)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Bar(x=daily["date"], y=daily["activities"],
                               name="Activities", marker_color="#6366f1", opacity=0.7))
    fig_trend.add_trace(go.Scatter(x=daily["date"], y=daily["connected"],
                                   name="Connected", line=dict(color="#10b981", width=3),
                                   mode="lines+markers", yaxis="y2"))
    fig_trend.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        title="Daily Activity Trend", height=350, margin=dict(l=40, r=40, t=40, b=40),
        yaxis=dict(title="Activities"),
        yaxis2=dict(title="Connected", overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.12),
        barmode="overlay",
    )

    # Channel pie
    ch_data = df.groupby("channel").size().reset_index(name="count")
    fig_pie = px.pie(ch_data, values="count", names="channel", hole=0.5,
                     color="channel", color_discrete_map=CHANNEL_COLORS)
    fig_pie.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                          title="Channel Mix", height=350, margin=dict(l=20, r=20, t=40, b=20))

    # Channel success rates
    ch_rates = pd.DataFrame([
        {"channel": "AI Call", "rate": kpis["connection_rate"], "label": "Connection %"},
        {"channel": "SMS", "rate": kpis["sms_delivery_rate"], "label": "Delivery %"},
        {"channel": "Whatsapp", "rate": kpis["whatsapp_delivery_rate"], "label": "Delivery %"},
    ])
    fig_bar = px.bar(ch_rates, x="rate", y="channel", orientation="h",
                     color="channel", color_discrete_map=CHANNEL_COLORS, text="rate")
    fig_bar.update_traces(texttemplate="%{text}%", textposition="outside")
    fig_bar.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                          title="Channel Success Rates (%)", height=300,
                          showlegend=False, xaxis_title="Rate %", xaxis_range=[0, 110])

    # Portfolio gauge
    total = kpis["total_accounts"]
    reached = kpis["accounts_reached"]
    pct = (reached / total * 100) if total else 0
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=pct,
        number={"suffix": "%"},
        title={"text": "Portfolio Reached"},
        delta={"reference": 60, "suffix": "% vs 60% target"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#f59e0b"},
            "steps": [
                {"range": [0, 30], "color": "rgba(239,68,68,0.2)"},
                {"range": [30, 60], "color": "rgba(245,158,11,0.2)"},
                {"range": [60, 100], "color": "rgba(16,185,129,0.2)"},
            ],
            "threshold": {"line": {"color": "#10b981", "width": 3}, "thickness": 0.8, "value": 60},
        },
    ))
    fig_gauge.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=300)

    return fig_trend, fig_pie, fig_bar, fig_gauge


@app.callback(
    [Output("disposition-chart", "figure"),
     Output("flow-chart", "figure"),
     Output("hourly-chart", "figure")],
    [Input("refresh-interval", "n_intervals"),
     Input("days-store", "data")],
)
def update_operations(n, days):
    data = get_data(days)
    df = data["df"]
    ai_calls = data["ai_calls"]

    if df.empty:
        return [empty_fig()] * 3

    # Disposition breakdown (AI calls)
    disp = ai_calls["disposition"].replace("", pd.NA).dropna().value_counts().head(12).reset_index()
    disp.columns = ["disposition", "count"]
    fig_disp = px.bar(disp, x="count", y="disposition", orientation="h",
                      color="count", color_continuous_scale="Viridis", text="count")
    fig_disp.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig_disp.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                           title="AI Call Disposition Breakdown", height=420,
                           yaxis=dict(autorange="reversed"), coloraxis_showscale=False)

    # Flow performance
    flow_df = df[df["flow"].notna()].copy()
    if not flow_df.empty:
        flow = flow_df.groupby("flow").agg(
            volume=("id", "count"),
            success=("disposition", lambda x: x.isin(["Payment Paid", "Agree To Pay", "Connected"]).sum())
        ).reset_index()
        flow["rate"] = (flow["success"] / flow["volume"] * 100).round(2)
        fig_flow = px.bar(flow, x="flow", y="rate", text="rate",
                          color="rate", color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                          hover_data={"volume": True})
        fig_flow.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_flow.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                               title="Flow Performance (Success Rate %)", height=420,
                               yaxis_title="Rate %", coloraxis_showscale=False)
    else:
        fig_flow = empty_fig("No flow data")

    # Hourly distribution
    hourly = df.groupby("hour").size().reset_index(name="count")
    hourly_conn = (
        ai_calls[ai_calls["disposition"] == "Connected"]
        .groupby("hour").size().reset_index(name="connected")
    )
    hourly = hourly.merge(hourly_conn, on="hour", how="left").fillna(0)
    fig_hourly = make_subplots(specs=[[{"secondary_y": True}]])
    fig_hourly.add_trace(
        go.Bar(x=hourly["hour"], y=hourly["count"], name="All Activities",
               marker_color="#6366f1", opacity=0.6), secondary_y=False
    )
    fig_hourly.add_trace(
        go.Scatter(x=hourly["hour"], y=hourly["connected"], name="AI Connected",
                   line=dict(color="#10b981", width=3), mode="lines+markers"), secondary_y=True
    )
    fig_hourly.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        title="Hourly Activity Distribution (IST)", height=350,
        xaxis=dict(title="Hour of Day", tickmode="linear", tick0=0, dtick=1),
    )
    fig_hourly.update_yaxes(title_text="Activity Volume", secondary_y=False)
    fig_hourly.update_yaxes(title_text="AI Connections", secondary_y=True)

    return fig_disp, fig_flow, fig_hourly


@app.callback(
    [Output("category-chart", "figure"),
     Output("sentiment-chart", "figure"),
     Output("scatter-chart", "figure"),
     Output("account-table", "data")],
    [Input("refresh-interval", "n_intervals"),
     Input("days-store", "data"),
     Input("priority-filter", "value")],
)
def update_customer(n, days, priority_filter):
    data = get_data(days)
    accounts = data["account_data"]
    ai_calls = data["ai_calls"]

    # Category distribution (exclude General)
    cat_dist = {}
    for acc in accounts:
        for c in acc.get("categories", []):
            if c != "General/Uncategorized":
                cat_dist[c] = cat_dist.get(c, 0) + 1

    if cat_dist:
        cat_df = pd.DataFrame(list(cat_dist.items()), columns=["category", "count"])
        fig_cat = px.pie(cat_df, values="count", names="category", hole=0.4,
                         color="category", color_discrete_map=CATEGORY_COLORS)
        fig_cat.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                              title="Customer Segments", height=350)
    else:
        fig_cat = empty_fig("No categorized accounts")

    # Sentiment
    if not ai_calls.empty:
        sent = ai_calls["sentiment"].replace("", pd.NA).dropna().str.lower().value_counts().reset_index()
        sent.columns = ["sentiment", "count"]
        fig_sent = px.bar(sent, x="sentiment", y="count", color="sentiment",
                          color_discrete_map=SENTIMENT_COLORS, text="count")
        fig_sent.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig_sent.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                               title="Sentiment Distribution", height=350, showlegend=False)
    else:
        fig_sent = empty_fig("No sentiment data")

    # Scatter: touchpoints vs connections
    if accounts:
        scatter_df = pd.DataFrame(accounts)[["account_id", "touchpoints", "connected", "priority", "primary_category"]]
        fig_scatter = px.scatter(
            scatter_df, x="touchpoints", y="connected",
            color="priority", color_discrete_map=PRIORITY_COLORS,
            hover_data=["account_id", "primary_category"],
            size_max=12, opacity=0.7,
        )
        fig_scatter.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                  title="Touchpoints vs Connections (by Priority)", height=350)
    else:
        fig_scatter = empty_fig()

    # Table data with optional priority filter
    filtered = accounts
    if priority_filter != "All":
        filtered = [a for a in accounts if a["priority"] == priority_filter]

    table_rows = [
        {k: v for k, v in a.items() if k not in ("categories",)}
        for a in filtered
    ]

    return fig_cat, fig_sent, fig_scatter, table_rows


@app.callback(
    [Output("ptp-chart", "figure"),
     Output("provider-chart", "figure"),
     Output("contact-chart", "figure"),
     Output("diversity-chart", "figure")],
    [Input("refresh-interval", "n_intervals"),
     Input("days-store", "data")],
)
def update_analytics(n, days):
    data = get_data(days)
    df = data["df"]
    ai_calls = data["ai_calls"]

    if df.empty:
        return [empty_fig()] * 4

    # PTP Funnel (from live data)
    ptp_stages = [
        {"stage": "PTP Commitments", "value": int(df["ptp"].notna().sum())},
        {"stage": "Agreed to Pay", "value": int((df["disposition"] == "Agree To Pay").sum())},
        {"stage": "Payments Made", "value": int(df["disposition"].isin(["Payment Paid", "Paid"]).sum())},
        {"stage": "PTP Broken Cases", "value": int((df["flow"] == "PTP Broken").sum())},
    ]
    ptp_df = pd.DataFrame(ptp_stages)
    fig_ptp = px.funnel(ptp_df, x="value", y="stage", color="stage",
                        color_discrete_sequence=["#6366f1", "#06b6d4", "#10b981", "#ef4444"])
    fig_ptp.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                          title="PTP Pipeline (Live)", height=350, showlegend=False)

    # Provider performance
    prov = ai_calls[ai_calls["provider"].notna()].groupby("provider").agg(
        volume=("id", "count"),
        connected=("disposition", lambda x: (x == "Connected").sum())
    ).reset_index()
    prov["rate"] = (prov["connected"] / prov["volume"] * 100).round(2)
    fig_prov = go.Figure()
    fig_prov.add_trace(go.Bar(x=prov["provider"], y=prov["volume"], name="Volume",
                              marker_color="#6366f1", text=prov["volume"],
                              texttemplate="%{text:,}", textposition="outside"))
    fig_prov.add_trace(go.Bar(x=prov["provider"], y=prov["connected"], name="Connected",
                              marker_color="#10b981", text=prov["connected"],
                              texttemplate="%{text:,}", textposition="outside"))
    fig_prov.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                           title="Provider Performance (AI Calls)", height=350, barmode="group")

    # Contact type performance
    contact_df = ai_calls[ai_calls["contact_number_choice"].notna()].groupby("contact_number_choice").agg(
        volume=("id", "count"),
        connected=("disposition", lambda x: (x == "Connected").sum())
    ).reset_index()
    contact_df["rate"] = (contact_df["connected"] / contact_df["volume"] * 100).round(2)
    contact_df["label"] = contact_df["contact_number_choice"].str.replace("_", " ").str.title()
    fig_contact = px.bar(contact_df, x="rate", y="label", orientation="h", text="rate",
                         color="rate", color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"])
    fig_contact.update_traces(texttemplate="%{text}%", textposition="outside")
    fig_contact.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                              title="Contact Type Connection Rates", height=350,
                              yaxis_title="", coloraxis_showscale=False)

    # Multi-channel diversity
    diversity = df.groupby("account_id")["channel"].nunique().value_counts().reset_index()
    diversity.columns = ["channels", "accounts"]
    diversity = diversity.sort_values("channels")
    diversity["label"] = diversity["channels"].astype(str) + " Channel(s)"
    fig_div = px.pie(diversity, values="accounts", names="label", hole=0.4)
    fig_div.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                          title="Multi-Channel Penetration", height=350)

    return fig_ptp, fig_prov, fig_contact, fig_div


# ─── Entry Point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Loan Collection Dashboard (Live DB)")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    args = parser.parse_args()

    # Quick connectivity check
    print("Checking database connection...")
    status = get_db_status()
    if not status["ok"]:
        print(f"ERROR: Cannot connect to database — {status.get('error')}")
        sys.exit(1)
    print(f"Connected. Total rows in DB: {status['total_rows']:,}")

    print(f"Pre-loading last {args.days} days of data...")
    data = get_data(args.days)
    kpis = data["kpis"]
    print(f"Loaded {kpis['total_activities']:,} activities across {kpis['total_accounts']:,} accounts.")
    print(f"High priority accounts: {kpis['high_priority_accounts']}")

    print(f"\n{'='*50}")
    print(f"  Dashboard running at http://127.0.0.1:{args.port}")
    print(f"  Auto-refreshes every {REFRESH_INTERVAL_MS // 1000}s")
    print(f"{'='*50}\n")

    app.run(debug=args.debug, port=args.port, host="0.0.0.0")
