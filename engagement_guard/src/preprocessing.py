# ============================================================
#  src/preprocessing.py
#  Core EWMA logic — math, scoring, and database read/write.
#  This is the heart of the system.
# ============================================================

import math
import logging
from datetime import date, timedelta
from typing import Optional

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.config import EWMA_ALPHA, EWMA_K, EWMA_MIN_DAYS
from src.db_connection import get_connection

logger = logging.getLogger(__name__)


# ── EWMA Math ─────────────────────────────────────────────────────────────────

def update_ewma(
    prev_mean: float,
    prev_var:  float,
    new_value: float,
    alpha:     float
) -> tuple:
    """
    One-step EWMA update. Returns (new_mean, new_variance).

    Mean formula    : E_t = alpha * X_t + (1 - alpha) * E_{t-1}
    Variance formula: V_t = (1 - alpha) * (V_{t-1} + alpha * (X_t - E_{t-1})^2)
    """
    new_mean = alpha * new_value + (1.0 - alpha) * prev_mean
    new_var  = (1.0 - alpha) * (prev_var + alpha * (new_value - prev_mean) ** 2)
    return new_mean, new_var


def classify(
    actual:    float,
    ewma_mean: float,
    ewma_std:  float,
    k:         float
) -> tuple:
    """
    Compare actual count against the EWMA band.

    Returns (label, deviation_score).
    deviation = (actual - mean) / std

    If std is too small (not enough variance yet), return Normal.
    """
    if ewma_std < 1e-9:
        return "Normal", 0.0

    deviation = (actual - ewma_mean) / ewma_std

    if deviation < -k:
        label = "Drop"
    elif deviation > k:
        label = "Rising"
    else:
        label = "Normal"

    return label, round(deviation, 4)


# ── Series computation ────────────────────────────────────────────────────────

def compute_ewma_series(
    counts:        list,
    alpha:         float = EWMA_ALPHA,
    k:             float = EWMA_K,
    min_days:      int   = EWMA_MIN_DAYS,
    initial_state: Optional[dict] = None
) -> list:
    """
    Process a sorted list of (feedback_date, feedback_count) tuples
    for one company and return a scored result for every day.

    If initial_state is provided (loaded from ewma_state table),
    the EWMA continues from where it left off instead of restarting.

    Each result dict contains:
        feedback_date, actual_count, ewma_mean, ewma_std,
        deviation, label, n_days
    """
    if not counts:
        return []

    results = []

    # ── Initialise state ──────────────────────────────────────
    if initial_state and initial_state.get("n_days", 0) >= min_days:
        # Continue from saved state — no need to replay history
        ewma_mean = initial_state["ewma_mean"]
        ewma_var  = initial_state["ewma_var"]
        n_days    = initial_state["n_days"]
    else:
        # Cold start — seed with the very first value
        first_date, first_count = counts[0]
        ewma_mean = float(first_count)
        ewma_var  = 0.0
        n_days    = 1
        counts    = counts[1:]  # first row used as seed, skip it in loop

    # ── Process each day ──────────────────────────────────────
    for feedback_date, actual_count in counts:
        ewma_mean, ewma_var = update_ewma(
            ewma_mean, ewma_var, float(actual_count), alpha
        )
        n_days  += 1
        ewma_std = math.sqrt(ewma_var)

        # Only score after we have enough history
        if n_days >= min_days:
            label, deviation = classify(float(actual_count), ewma_mean, ewma_std, k)
        else:
            label, deviation = "Normal", 0.0

        results.append({
            "feedback_date": feedback_date,
            "actual_count":  actual_count,
            "ewma_mean":     round(ewma_mean, 4),
            "ewma_std":      round(ewma_std,  4),
            "ewma_var":      round(ewma_var,  6),
            "deviation":     deviation,
            "label":         label,
            "n_days":        n_days,
        })

    return results


# ── Database helpers ──────────────────────────────────────────────────────────

def load_recent_counts(
    company_id:   int,
    lookback_days: int = 90
) -> list:
    """
    Fetch the last N days of daily_feedback for one company.
    Returns a sorted list of (feedback_date, feedback_count) tuples.
    """
    since = (date.today() - timedelta(days=lookback_days)).isoformat()

    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT feedback_date, feedback_count
        FROM   daily_feedback
        WHERE  company_id    = %s
          AND  feedback_date >= %s
        ORDER BY feedback_date ASC
        """,
        (company_id, since)
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [(row["feedback_date"], row["feedback_count"]) for row in rows]


def load_ewma_state(company_id: int) -> Optional[dict]:
    """
    Load the saved EWMA state for a company from ewma_state table.
    Returns None if no state exists yet (first run).
    """
    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT ewma_mean, ewma_var, n_days
        FROM   ewma_state
        WHERE  company_id = %s
        """,
        (company_id,)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    return row if row else None


def save_ewma_state(company_id: int, result: dict) -> None:
    """
    Upsert the latest EWMA state for a company back into ewma_state.
    This is what lets the system remember between daily runs.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO ewma_state
            (company_id, ewma_mean, ewma_var, last_updated, n_days)
        VALUES
            (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ewma_mean    = VALUES(ewma_mean),
            ewma_var     = VALUES(ewma_var),
            last_updated = VALUES(last_updated),
            n_days       = VALUES(n_days)
        """,
        (
            company_id,
            result["ewma_mean"],
            result["ewma_var"],
            result["feedback_date"],
            result["n_days"],
        )
    )
    cursor.close()
    conn.close()


def upsert_alert(company_id: int, result: dict) -> None:
    """
    Write a single scored day into engagement_alerts.
    Uses INSERT ... ON DUPLICATE KEY UPDATE so re-runs are safe.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO engagement_alerts
            (company_id, alert_date, alert_type, actual_count,
             ewma_mean, ewma_std, deviation)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            alert_type   = VALUES(alert_type),
            actual_count = VALUES(actual_count),
            ewma_mean    = VALUES(ewma_mean),
            ewma_std     = VALUES(ewma_std),
            deviation    = VALUES(deviation)
        """,
        (
            company_id,
            result["feedback_date"],
            result["label"],
            result["actual_count"],
            result["ewma_mean"],
            result["ewma_std"],
            result["deviation"],
        )
    )
    cursor.close()
    conn.close()


# ── Main pipeline function ────────────────────────────────────────────────────

def run_preprocessing(
    alpha: float = EWMA_ALPHA,
    k:     float = EWMA_K
) -> dict:
    """
    Run the full EWMA scoring pipeline across all companies.

    For each company:
      1. Load recent counts from daily_feedback
      2. Load saved EWMA state (if any)
      3. Compute EWMA series
      4. Write alerts to engagement_alerts
      5. Save updated state to ewma_state

    Returns a summary dict used by train.py.
    """
    logger.info(
        "Starting EWMA preprocessing — alpha=%.2f  K=%.1f", alpha, k
    )

    summary = {
        "companies_scored": 0,
        "alerts_raised":    0,
        "drops":            0,
        "rising":           0,
    }

    # Load all companies
    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT company_id, company_name FROM companies")
    companies = cursor.fetchall()
    cursor.close()
    conn.close()

    for company in companies:
        cid  = company["company_id"]
        name = company["company_name"]

        counts = load_recent_counts(cid)

        if len(counts) < 2:
            logger.debug(
                "Skipping %s — not enough data (%d rows)", name, len(counts)
            )
            continue

        state   = load_ewma_state(cid)
        results = compute_ewma_series(
            counts, alpha=alpha, k=k, initial_state=state
        )

        if not results:
            continue

        # Write every scored day to alerts table
        for r in results:
            upsert_alert(cid, r)

        # Save the latest EWMA state so tomorrow's run can continue
        save_ewma_state(cid, results[-1])

        drops  = sum(1 for r in results if r["label"] == "Drop")
        rising = sum(1 for r in results if r["label"] == "Rising")

        summary["companies_scored"] += 1
        summary["alerts_raised"]    += drops + rising
        summary["drops"]            += drops
        summary["rising"]           += rising

        logger.info(
            "  %-20s — %3d days scored | %2d drops | %2d rising",
            name, len(results), drops, rising
        )

    logger.info("Preprocessing complete — %s", summary)
    return summary


if __name__ == "__main__":
    # Run this file directly to test EWMA scoring:
    # python src/preprocessing.py
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s %(levelname)s %(message)s"
    )
    result = run_preprocessing()
    print("\nSummary:", result)