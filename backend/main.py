"""
backend/main.py — FastAPI backend for Loan Collection Dashboard
===============================================================
Returns the same JSON structure as loan-dashboard's data.js,
populated from live finguard_oto_april PostgreSQL database.

Logic (matching loan-dashboard exactly):
  - Connection rate  : task_status = 'Connected' (not disposition)
  - AI calls         : channel = 'AI Call' only  (AI ASSISTANT excluded)
  - Main channels    : AI Call, SMS, Whatsapp, Manual Call
  - Scheduled        : eta_ist hour
  - Executed         : modified_ist hour, status = 'done', not rescheduled
  - PTPs             : 'Agree To Pay' only (Unacceptable excluded)
  - Cache            : 5 minutes (CACHE_SECONDS env var)
"""

import os
import re
import time
import numpy as np
import pandas as pd
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://readonly:readonly@otolmsstagedbinstance.cttxlpcdrmsq.ap-south-1.rds.amazonaws.com:5432/finguard_oto_april",
)
CACHE_SECONDS = int(os.environ.get("CACHE_SECONDS", 300))
STATIC_DIR = Path(__file__).parent / "static"

# ── DB engine ─────────────────────────────────────────────────────────────────
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            DB_URL,
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=5,
            connect_args={"connect_timeout": 15},
        )
    return _engine

# ── Cache bucket (invalidates every CACHE_SECONDS) ────────────────────────────
def _bucket() -> int:
    return int(time.time() / CACHE_SECONDS)

# ── Category keyword patterns (regex, case-insensitive) ───────────────────────
KEYWORDS = {
    "Financial Hardship":  r"financial hardship|no money|salary|lost job|business loss|financial difficulty|financial problem",
    "Family Emergency":    r"death|demise|funeral|family emergency|passed away|mourning|bereavement",
    "Health Issues":       r"\bhealth\b|hospital|medical|surgery|illness|\bsick\b|treatment|accident",
    "Dispute":             r"dispute|not taken loan|already paid|settled|discrepancy|denied|not my loan|fraud",
    "Payment Commitment":  r"will pay|promise to pay|agree to pay|payment by|paying on|pay by|pay on|pay after",
    "Wrong Number":        r"wrong number",
    "Busy/No Answer":      r"voicemail|not available|no answer|\bbusy\b",
}
HIGH_CATS  = {"Financial Hardship", "Family Emergency", "Health Issues", "Dispute"}
SKIP_STAT  = {"not-connected", "Not Connected", "RescheduledToNextDay", ""}
MAIN_CH    = {"AI Call", "SMS", "Whatsapp", "Manual Call"}

# ── Raw SQL fetch (lru_cached per bucket) ─────────────────────────────────────
@lru_cache(maxsize=4)
def _build_response(bucket: int) -> dict:
    """Fetch raw data, compute all metrics, return full JSON response."""
    query = """
        SELECT
            t.id,
            t.account_id,
            t.channel,
            t.disposition,
            t.task_status,
            t.status,
            t.flow,
            t.provider,
            t.contact_number_choice,
            t.sentiment,
            t.summary,
            t.is_priority_task,
            t.ptp,
            (t.eta          AT TIME ZONE 'Asia/Kolkata') AS eta_ist,
            (t.processed_at AT TIME ZONE 'Asia/Kolkata') AS processed_at_ist,
            (t.created      AT TIME ZONE 'Asia/Kolkata') AS created_ist,
            DATE(t.created  AT TIME ZONE 'Asia/Kolkata') AS date
        FROM activity_taskactivity t
        ORDER BY t.created ASC
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(text(query), conn)

    df["date"]            = df["date"].astype(str)
    df["summary"]         = df["summary"].fillna("")
    df["disposition"]     = df["disposition"].fillna("")
    df["task_status"]     = df["task_status"].fillna("")
    df["sentiment"]       = df["sentiment"].fillna("")
    # Derive date strings for cross-day lookups
    df["eta_date"]        = df["eta_ist"].dt.date.apply(
                                lambda x: x.isoformat() if pd.notna(x) else "")
    df["processed_at_date"] = df["processed_at_ist"].dt.date.apply(
                                lambda x: x.isoformat() if pd.notna(x) else "")

    dates = sorted(df["date"].unique().tolist())

    # "all" = aggregate across every date
    all_metrics = _compute_metrics(df, full_df=df, target_date=None)

    # Daily summary lives in all_metrics always (regardless of selected date)
    daily_summary = []
    for d in dates:
        ddf = df[df["date"] == d]
        ai_d = ddf[ddf["channel"] == "AI Call"]
        daily_summary.append({
            "date": d,
            "activities": int(len(ddf[ddf["channel"].isin(MAIN_CH)])),
            "connected":  int((ai_d["task_status"] == "Connected").sum()),
        })
    all_metrics["daily_summary"] = daily_summary

    # Per-date slices — pass full_df so cross-day scheduled activities can be resolved
    by_date = {d: _compute_metrics(df[df["date"] == d], full_df=df, target_date=d)
               for d in dates}

    return {"dates": dates, "all": all_metrics, "by_date": by_date}


def get_response() -> dict:
    return _build_response(_bucket())


# ── Metrics for one dataframe slice ──────────────────────────────────────────
def _compute_metrics(df: pd.DataFrame,
                     full_df: pd.DataFrame = None,
                     target_date: str = None) -> dict:
    ai       = df[df["channel"] == "AI Call"].copy()
    sms      = df[df["channel"] == "SMS"]
    wa       = df[df["channel"] == "Whatsapp"]
    main_ch  = df[df["channel"].isin(MAIN_CH)]

    ai_total     = len(ai)
    ai_conn      = int((ai["task_status"] == "Connected").sum())
    accs_total   = int(df["account_id"].nunique())
    accs_reached = int(ai[ai["task_status"] == "Connected"]["account_id"].nunique())

    sms_del = int(sms["task_status"].isin(["Delivered", "Sent", "SENT - No DLR"]).sum()) if len(sms) else 0
    wa_del  = int(
        ((wa["task_status"] == "Sent") | ((wa["task_status"] == "") & (wa["status"] == "done"))).sum()
    ) if len(wa) else 0

    kpis = {
        "total_activities": int(len(main_ch)),
        "ai_calls":         int(ai_total),
        "sms":              int(len(sms)),
        "whatsapp":         int(len(wa)),
        "connection_rate":  round(ai_conn / ai_total * 100, 2) if ai_total else 0,
        "ai_connected":     ai_conn,
        "accounts_total":   accs_total,
        "accounts_reached": accs_reached,
        "ptps_total":       int(df["ptp"].notna().sum()),
        "agree_to_pay":     int((df["disposition"] == "Agree To Pay").sum()),
        "sms_rate":         round(sms_del / len(sms) * 100, 2) if len(sms) else 0,
        "wa_rate":          round(wa_del  / len(wa)  * 100, 2) if len(wa)  else 0,
    }

    return {
        "kpis":                 kpis,
        "hourly_trend":         _hourly_trend(df),
        "hourly_exec":          _hourly_exec(full_df if full_df is not None else df, target_date),
        "case_analysis":        _case_analysis(ai, accs_total),
        "disposition":          _disposition(ai),
        "category_distribution":_categories(ai),
        "sentiment":            _sentiment(ai),
        "contacts":             _contacts(ai),
        "accounts":             _accounts(df, ai),
        "ptp_funnel":           _ptp_funnel(df),
        "providers":            _providers(ai),
        "channel_diversity":    _channel_diversity(df),
    }


# ── Hourly trend (Chart 1 — scheduled by eta_ist, executed by eta_ist+status=done)
# NOTE: processed_at is set by a batch finalization job hours after the call fires,
# so it does NOT represent when the call happened. eta_ist is the correct execution
# timestamp — it reflects when the system actually placed the call.
def _hourly_trend(df: pd.DataFrame) -> list:
    buckets: dict = {}

    def add(h, key, n):
        if h not in buckets:
            buckets[h] = {"hour": int(h), "s_ai": 0, "s_wa": 0, "s_sms": 0, "e_ai": 0, "e_wa": 0, "e_sms": 0}
        buckets[h][key] += int(n)

    for k, ch in [("ai", "AI Call"), ("wa", "Whatsapp"), ("sms", "SMS")]:
        cdf = df[df["channel"] == ch]
        # Scheduled: all activities by eta hour
        for h, n in cdf["eta_ist"].dropna().dt.hour.value_counts().items():
            add(h, f"s_{k}", n)
        # Executed: status=done by eta hour (eta = when the call was actually placed)
        exec_df = cdf[(cdf["status"] == "done") & (~cdf["task_status"].isin(SKIP_STAT))]
        for h, n in exec_df["eta_ist"].dropna().dt.hour.value_counts().items():
            add(h, f"e_{k}", n)

    return sorted(buckets.values(), key=lambda x: x["hour"])


# ── Hourly exec (Chart 2 — cross-day aware, both scheduled and executed use eta_date)
# Scheduled: activities whose eta_date == view_date (includes next-day carry-overs)
# Executed : status=done whose eta_date == view_date (eta = actual call time)
# Activities created after 7:30 PM have eta on the next day → appear as scheduled
# for that next day, never leak into the current day's executed bars.
def _hourly_exec(full_df: pd.DataFrame, target_date: str = None) -> list:
    buckets: dict = {}

    def add(h, key, n):
        if h not in buckets:
            buckets[h] = {"hour": int(h), "s_ai": 0, "s_wa": 0, "s_sms": 0, "ai": 0, "wa": 0, "sms": 0}
        buckets[h][key] += int(n)

    # Scheduled: activities whose eta falls on target_date (cross-day aware)
    sched = full_df[full_df["eta_date"] == target_date] if target_date else full_df
    for k, ch in [("ai", "AI Call"), ("wa", "Whatsapp"), ("sms", "SMS")]:
        for h, n in sched[sched["channel"] == ch]["eta_ist"].dropna().dt.hour.value_counts().items():
            add(h, f"s_{k}", n)

    # Executed: status=done whose eta falls on target_date (eta = when call was placed)
    done = full_df[full_df["status"] == "done"]
    if target_date:
        done = done[done["eta_date"] == target_date]
    for k, ch in [("ai", "AI Call"), ("wa", "Whatsapp"), ("sms", "SMS")]:
        for h, n in done[done["channel"] == ch]["eta_ist"].dropna().dt.hour.value_counts().items():
            add(h, k, n)

    return sorted(buckets.values(), key=lambda x: x["hour"])


# ── Case analysis ─────────────────────────────────────────────────────────────
def _case_analysis(ai: pd.DataFrame, total_accounts: int) -> dict:
    ai_accs   = set(ai["account_id"].dropna().astype(int))
    conn_accs = set(ai[ai["task_status"] == "Connected"]["account_id"].dropna().astype(int))
    nc_accs   = set(ai[ai["task_status"] != "Connected"]["account_id"].dropna().astype(int))
    return {
        "attempted":           int(len(ai_accs)),
        "connected":           int(len(conn_accs)),
        "connected_with_more": int(len(conn_accs & nc_accs)),
        "no_success":          int(len(ai_accs - conn_accs)),
        "no_ai_scheduled":     int(total_accounts - len(ai_accs)),
    }


# ── Disposition breakdown ─────────────────────────────────────────────────────
def _disposition(ai: pd.DataFrame) -> dict:
    counts = ai["disposition"].replace("", pd.NA).dropna().value_counts()
    return {str(k): int(v) for k, v in counts.items()}


# ── Category distribution (vectorized) ───────────────────────────────────────
def _add_cat_cols(ai: pd.DataFrame) -> pd.DataFrame:
    s = ai["summary"].str.lower()
    d = ai["disposition"]
    for cat, pat in KEYWORDS.items():
        col = f"_c_{cat}"
        if cat == "Wrong Number":
            ai[col] = (d == "Wrong Number") | s.str.contains(pat, regex=True, na=False)
        else:
            ai[col] = s.str.contains(pat, regex=True, na=False)
    return ai

def _row_cats(row, cat_cols):
    cats = [c.replace("_c_", "") for c in cat_cols if row[c]]
    return cats if cats else ["General/Uncategorized"]

def _categories(ai: pd.DataFrame) -> dict:
    if ai.empty:
        return {}
    ai = _add_cat_cols(ai.copy())
    cat_cols = [f"_c_{c}" for c in KEYWORDS]
    dist: dict = {}
    for cats in ai[cat_cols].apply(lambda r: _row_cats(r, cat_cols), axis=1):
        for cat in cats:
            dist[cat] = dist.get(cat, 0) + 1
    return dist


# ── Sentiment ─────────────────────────────────────────────────────────────────
def _sentiment(ai: pd.DataFrame) -> dict:
    counts = ai["sentiment"].replace("", pd.NA).dropna().str.lower().value_counts()
    return {str(k): int(v) for k, v in counts.items()}


# ── Contact strategy ──────────────────────────────────────────────────────────
def _contacts(ai: pd.DataFrame) -> list:
    filtered = ai[ai["contact_number_choice"].notna() & (ai["contact_number_choice"] != "")]
    result = []
    for name, g in filtered.groupby("contact_number_choice"):
        vol  = len(g)
        conn = int((g["task_status"] == "Connected").sum())
        result.append({"type": str(name), "volume": int(vol), "connected": conn,
                       "rate": round(conn / vol * 100, 2) if vol else 0})
    return sorted(result, key=lambda x: -x["volume"])


# ── Accounts / escalation queue ───────────────────────────────────────────────
def _accounts(df: pd.DataFrame, ai: pd.DataFrame) -> list:
    touchpoints = df.groupby("account_id").size().rename("touchpoints")
    channels = (df[df["channel"].isin(MAIN_CH)]
                .groupby("account_id")["channel"].nunique().rename("channels"))
    connected   = (ai[ai["task_status"] == "Connected"]
                   .groupby("account_id").size().rename("connected"))
    wrong_nums  = (ai[ai["disposition"] == "Wrong Number"]
                   .groupby("account_id").size().rename("wrong_numbers"))

    acc = touchpoints.to_frame().join([channels, connected, wrong_nums], how="left")
    acc[["connected", "wrong_numbers", "channels"]] = (
        acc[["connected", "wrong_numbers", "channels"]].fillna(0).astype(int))

    # Vectorised categories per account
    ai_cat   = _add_cat_cols(ai.copy())
    cat_cols = [f"_c_{c}" for c in KEYWORDS]
    ai_cat["cats"] = ai_cat[cat_cols].apply(lambda r: _row_cats(r, cat_cols), axis=1)

    def agg_cats(s):
        out: set = set()
        for c in s:
            out.update(c)
        return list(out) if out else ["General/Uncategorized"]

    if not ai_cat.empty:
        acct_cats = ai_cat.groupby("account_id")["cats"].apply(agg_cats)
        acc = acc.join(acct_cats.rename("categories"), how="left")

    acc["categories"] = acc["categories"].apply(
        lambda x: x if isinstance(x, list) else ["General/Uncategorized"])
    acc["primary_category"] = acc["categories"].apply(lambda x: x[0])

    def priority(row):
        cats = set(row["categories"])
        if cats & HIGH_CATS:                            return "High"
        if row["touchpoints"] > 40 and row["connected"] == 0: return "High"
        if "Payment Commitment" in cats:               return "Medium"
        if row["touchpoints"] > 20:                    return "Medium"
        return "Low"

    acc["priority"]     = acc.apply(priority, axis=1)
    acc["last_contact"] = (df.groupby("account_id")["created_ist"].max()
                           .dt.strftime("%Y-%m-%d %H:%M"))

    if not ai_cat.empty:
        last_sent = (ai_cat[ai_cat["sentiment"] != ""]
                     .groupby("account_id")["sentiment"].last().rename("last_sentiment"))
        last_summ = (ai_cat[ai_cat["summary"] != ""]
                     .groupby("account_id")["summary"].last().rename("last_summary"))
        acc = acc.join(last_sent, how="left").join(last_summ, how="left")

    acc["last_sentiment"] = acc.get("last_sentiment", pd.Series(dtype=str)).fillna("N/A")
    acc["last_summary"]   = acc.get("last_summary",   pd.Series(dtype=str)).fillna("").str[:150]
    acc["_rank"]  = acc["priority"].map({"High": 3, "Medium": 2, "Low": 1})
    acc = acc.sort_values(["_rank", "touchpoints"], ascending=[False, False]).drop(columns=["_rank"])

    rows = acc.reset_index().to_dict("records")
    return [{
        "account_id":      int(r["account_id"]),
        "touchpoints":     int(r["touchpoints"]),
        "channels":        int(r.get("channels", 1)),
        "connected":       int(r["connected"]),
        "wrong_numbers":   int(r["wrong_numbers"]),
        "categories":      list(r["categories"]),
        "primary_category":str(r["primary_category"]),
        "priority":        str(r["priority"]),
        "last_contact":    str(r.get("last_contact", "")),
        "last_sentiment":  str(r.get("last_sentiment", "N/A")),
        "last_summary":    str(r.get("last_summary", ""))[:150],
    } for r in rows]


# ── PTP funnel ────────────────────────────────────────────────────────────────
def _ptp_funnel(df: pd.DataFrame) -> dict:
    return {
        "commitments": int(df["ptp"].notna().sum()),
        "agreed":      int((df["disposition"] == "Agree To Pay").sum()),
        "paid":        int(df["disposition"].isin(["Payment Paid", "Paid"]).sum()),
    }


# ── Provider performance ──────────────────────────────────────────────────────
def _providers(ai: pd.DataFrame) -> list:
    ai_p = ai[ai["provider"].notna() & (ai["provider"] != "")]
    result = []
    for prov, g in ai_p.groupby("provider"):
        done = g[g["status"] == "done"]
        made = int(len(done))
        conn = int((done["task_status"] == "Connected").sum())
        result.append({"display_name": str(prov), "calls_made": made, "connected": conn,
                       "rate": round(conn / made * 100, 2) if made else 0})
    return sorted(result, key=lambda x: -x["calls_made"])


# ── Channel diversity ─────────────────────────────────────────────────────────
def _channel_diversity(df: pd.DataFrame) -> dict:
    div = (df[df["channel"].isin(MAIN_CH)]
           .groupby("account_id")["channel"].nunique()
           .value_counts().to_dict())
    return {str(k): int(v) for k, v in div.items()}


# ── FastAPI app ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Warming data cache…")
    try:
        _build_response(_bucket())
        print("Cache ready.")
    except Exception as e:
        print(f"Cache warm failed (will retry on first request): {e}")
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/data")
def api_data():
    return get_response()


@app.get("/api/health")
def api_health():
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected",
                "next_refresh_in": CACHE_SECONDS - (int(time.time()) % CACHE_SECONDS)}
    except Exception as e:
        return {"status": "error", "db": str(e)}


# Serve React static build (must be last — catches all non-API routes)
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        return FileResponse(str(STATIC_DIR / "index.html"))
