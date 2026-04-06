"""
transforms.py - Business logic: categorization, KPIs, account scoring
----------------------------------------------------------------------
Takes a raw activities DataFrame (from db.get_activities_df) and produces
structured metrics used by dashboard callbacks.
All operations are vectorized (no per-row Python loops for large sets).
"""

import pandas as pd
import numpy as np
from datetime import datetime

# ─── Constants ────────────────────────────────────────────────────────────────
KEYWORDS_MAP = {
    "Financial Hardship": [
        "financial hardship", "no money", "salary", "lost job",
        "business loss", "financial difficulty", "financial problem",
    ],
    "Family Emergency": [
        "death", "demise", "funeral", "family emergency",
        "passed away", "mourning", "bereavement",
    ],
    "Health Issues": [
        "health", "hospital", "medical", "surgery",
        "illness", "sick", "treatment", "accident",
    ],
    "Dispute": [
        "dispute", "not taken loan", "already paid", "settled",
        "discrepancy", "denied", "not my loan", "fraud",
    ],
    "Payment Commitment": [
        "will pay", "promise to pay", "agree to pay",
        "payment by", "paying on", "pay by", "pay on", "pay after",
    ],
}

BUSY_KEYWORDS = ["voicemail", "not available", "no answer", "busy"]

PAYMENT_DISPOSITIONS = {"Payment Paid", "Agree To Pay", "Paid", "Will Pay", "Payment Claimed"}
CONNECTED_DISPOSITIONS = {"Connected"}
SMS_DELIVERED_STATUSES = {"Sent", "Delivered", "SENT - No DLR"}
WA_DELIVERED_STATUSES = {"Sent"}

PRIORITY_ORDER = {"High": 3, "Medium": 2, "Low": 1}


# ─── Row-level categorization ─────────────────────────────────────────────────
def _cats_from_text(summary: str, disposition: str) -> list:
    """Derive category tags from one activity's summary + disposition."""
    cats = []
    s = summary.lower()
    for cat, kws in KEYWORDS_MAP.items():
        if any(kw in s for kw in kws):
            cats.append(cat)
    if disposition == "Wrong Number" or "wrong number" in s:
        cats.append("Wrong Number")
    if any(kw in s for kw in BUSY_KEYWORDS):
        cats.append("Busy/No Answer")
    return cats if cats else ["General/Uncategorized"]


# ─── Main transform ───────────────────────────────────────────────────────────
def compute_metrics(df: pd.DataFrame) -> dict:
    """
    Compute all dashboard metrics from a raw activities DataFrame.

    Returns:
        df          - enriched activities DataFrame (date, hour columns added)
        ai_calls    - filtered to AI Call + AI ASSISTANT channels
        kpis        - dict of top-level KPIs
        account_data - list of per-account dicts, sorted by priority
        last_updated - ISO timestamp of computation
    """
    if df.empty:
        return _empty_result()

    # ── Enrich timestamps ──
    df = df.copy()
    df["created"] = pd.to_datetime(df["created"])
    df["date"] = df["created"].dt.date.astype(str)
    df["hour"] = df["created"].dt.hour

    ai_calls = df[df["channel"].isin(["AI Call", "AI ASSISTANT"])].copy()

    # ── Categorize AI call rows ──
    ai_calls["summary"] = ai_calls["summary"].fillna("")
    ai_calls["disposition"] = ai_calls["disposition"].fillna("")
    ai_calls["cats"] = ai_calls.apply(
        lambda r: _cats_from_text(r["summary"], r["disposition"]), axis=1
    )

    # ── Per-account aggregations (vectorized) ──
    account_data = _build_account_data(df, ai_calls)

    # ── KPIs ──
    ai_total = len(ai_calls)
    ai_connected = (ai_calls["disposition"] == "Connected").sum()

    sms_df = df[df["channel"] == "SMS"]
    wa_df = df[df["channel"] == "Whatsapp"]

    sms_delivered = sms_df["task_status"].isin(SMS_DELIVERED_STATUSES).sum()
    wa_delivered = wa_df["task_status"].isin(WA_DELIVERED_STATUSES).sum()

    kpis = {
        "total_activities": len(df),
        "total_accounts": df["account_id"].nunique(),
        "connection_rate": round(ai_connected / ai_total * 100, 2) if ai_total else 0,
        "accounts_reached": ai_calls[
            ai_calls["disposition"] == "Connected"
        ]["account_id"].nunique(),
        "total_payments": df["disposition"].isin(PAYMENT_DISPOSITIONS).sum(),
        "sms_delivery_rate": round(sms_delivered / len(sms_df) * 100, 2) if len(sms_df) else 0,
        "whatsapp_delivery_rate": round(wa_delivered / len(wa_df) * 100, 2) if len(wa_df) else 0,
        "wrong_number_rate": round(
            (ai_calls["disposition"] == "Wrong Number").sum() / ai_total * 100, 2
        ) if ai_total else 0,
        "high_priority_accounts": sum(1 for a in account_data if a["priority"] == "High"),
        "ptp_count": int(df["ptp"].notna().sum()),
        "priority_task_count": int(df["is_priority_task"].sum()),
    }

    return {
        "df": df,
        "ai_calls": ai_calls,
        "kpis": kpis,
        "account_data": account_data,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _build_account_data(df: pd.DataFrame, ai_calls: pd.DataFrame) -> list:
    """Build per-account summary using vectorized groupby operations."""
    # Touchpoints and channel diversity
    touchpoints = df.groupby("account_id").size().rename("touchpoints")
    channel_div = df.groupby("account_id")["channel"].nunique().rename("channels")

    # Connected and wrong number counts (AI calls only)
    connected = (
        ai_calls[ai_calls["disposition"] == "Connected"]
        .groupby("account_id").size().rename("connected")
    )
    wrong_nums = (
        ai_calls[ai_calls["disposition"] == "Wrong Number"]
        .groupby("account_id").size().rename("wrong_numbers")
    )

    # Merge base stats
    acc = touchpoints.to_frame().join([channel_div, connected, wrong_nums], how="left")
    acc[["connected", "wrong_numbers"]] = acc[["connected", "wrong_numbers"]].fillna(0).astype(int)

    # Category tags per account
    def agg_cats(series):
        all_cats = set()
        for cats in series:
            all_cats.update(cats)
        return list(all_cats) if all_cats else ["General/Uncategorized"]

    if not ai_calls.empty and "cats" in ai_calls.columns:
        account_cats = ai_calls.groupby("account_id")["cats"].apply(agg_cats)
        acc = acc.join(account_cats.rename("categories"), how="left")
        acc["categories"] = acc["categories"].apply(
            lambda x: x if isinstance(x, list) else ["General/Uncategorized"]
        )
    else:
        acc["categories"] = [["General/Uncategorized"]] * len(acc)

    acc["primary_category"] = acc["categories"].apply(lambda x: x[0])

    # Priority scoring
    high_cats = {"Financial Hardship", "Family Emergency", "Health Issues", "Dispute"}

    def compute_priority(row):
        cats = set(row["categories"])
        if cats & high_cats:
            return "High"
        if row["touchpoints"] > 40 and row["connected"] == 0:
            return "High"
        if "Payment Commitment" in cats:
            return "Medium"
        if row["touchpoints"] > 20:
            return "Medium"
        return "Low"

    acc["priority"] = acc.apply(compute_priority, axis=1)

    # Last contact timestamp
    last_contact = (
        df.groupby("account_id")["created"].max().dt.strftime("%Y-%m-%d %H:%M")
    )
    acc = acc.join(last_contact.rename("last_contact"), how="left")

    # Last sentiment and summary from AI calls
    if not ai_calls.empty:
        last_sent = (
            ai_calls[ai_calls["sentiment"].notna()]
            .groupby("account_id")["sentiment"].last()
            .rename("last_sentiment")
        )
        last_summ = (
            ai_calls[ai_calls["summary"].notna() & (ai_calls["summary"] != "")]
            .groupby("account_id")["summary"].last()
            .rename("last_summary")
        )
        acc = acc.join(last_sent, how="left").join(last_summ, how="left")

    acc["last_sentiment"] = acc.get("last_sentiment", pd.Series(dtype=str)).fillna("N/A")
    acc["last_summary"] = acc.get("last_summary", pd.Series(dtype=str)).fillna("No summary available")
    acc["last_summary"] = acc["last_summary"].str[:150]

    # Sort: High > Medium > Low, then by most touchpoints
    acc["_priority_rank"] = acc["priority"].map(PRIORITY_ORDER)
    acc = acc.sort_values(["_priority_rank", "touchpoints"], ascending=[False, False])
    acc = acc.drop(columns=["_priority_rank"])

    records = acc.reset_index().rename(columns={"index": "account_id"}).to_dict("records")
    # Ensure account_id is int and categories is serializable
    for r in records:
        r["account_id"] = int(r["account_id"])
        r["categories"] = list(r["categories"]) if isinstance(r["categories"], (list, set)) else ["General/Uncategorized"]

    return records


def _empty_result() -> dict:
    return {
        "df": pd.DataFrame(),
        "ai_calls": pd.DataFrame(),
        "kpis": {
            "total_activities": 0, "total_accounts": 0, "connection_rate": 0,
            "accounts_reached": 0, "total_payments": 0, "sms_delivery_rate": 0,
            "whatsapp_delivery_rate": 0, "wrong_number_rate": 0,
            "high_priority_accounts": 0, "ptp_count": 0, "priority_task_count": 0,
        },
        "account_data": [],
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
