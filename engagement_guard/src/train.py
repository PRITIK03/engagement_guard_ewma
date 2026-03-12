# ============================================================
#  src/train.py
#  Runs the full EWMA pipeline and logs the run to
#  model_registry. No ML model is trained here — this file
#  just orchestrates preprocessing and records what happened.
# ============================================================

import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.config import EWMA_ALPHA, EWMA_K, EWMA_MIN_DAYS
from src.db_connection import get_connection
from src.preprocessing import run_preprocessing

logger = logging.getLogger(__name__)


def log_run_to_registry(
    alpha:            float,
    k:                float,
    min_days:         int,
    companies_scored: int,
    alerts_raised:    int,
    notes:            str = ""
) -> int:
    """
    Insert one row into model_registry for this pipeline run.
    Returns the new run_id.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO model_registry
            (alpha, k_threshold, min_days,
             companies_scored, alerts_raised, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (alpha, k, min_days, companies_scored, alerts_raised, notes)
    )
    run_id = cursor.lastrowid
    cursor.close()
    conn.close()

    return run_id


def clear_ewma_state() -> None:
    """
    Wipe the ewma_state table so the pipeline re-scores
    everything from scratch on next run.
    Only call this if you want a full reset.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ewma_state")
    cursor.close()
    conn.close()
    logger.info("ewma_state cleared — next run will rescore from scratch.")


def run_training(
    alpha:    float = EWMA_ALPHA,
    k:        float = EWMA_K,
    min_days: int   = EWMA_MIN_DAYS,
    notes:    str   = "",
    reset:    bool  = False
) -> dict:
    """
    Main entry point. Called by scheduler, run.py, and the API.

    Steps:
      1. Optionally clear saved state (if reset=True)
      2. Run EWMA preprocessing across all companies
      3. Log the run to model_registry
      4. Return a summary dict

    Parameters
    ----------
    alpha    : EWMA smoothing factor
    k        : std deviation threshold for alerts
    min_days : minimum days before scoring starts
    notes    : optional note saved to model_registry
    reset    : if True, clears ewma_state before running
               (forces full rescore from raw data)
    """
    logger.info("=" * 55)
    logger.info("EngagementGuard EWMA pipeline starting")
    logger.info("alpha=%.2f | K=%.1f | min_days=%d", alpha, k, min_days)
    logger.info("=" * 55)

    if reset:
        logger.info("Reset flag is True — clearing saved EWMA state.")
        clear_ewma_state()

    # Run the full EWMA scoring
    summary = run_preprocessing(alpha=alpha, k=k)

    # Build note if none provided
    if not notes:
        notes = (
            f"EWMA pipeline run — "
            f"{summary['drops']} drops, "
            f"{summary['rising']} rising across "
            f"{summary['companies_scored']} companies"
        )

    # Log to registry
    run_id = log_run_to_registry(
        alpha            = alpha,
        k                = k,
        min_days         = min_days,
        companies_scored = summary["companies_scored"],
        alerts_raised    = summary["alerts_raised"],
        notes            = notes
    )

    result = {
        "run_id":            run_id,
        "alpha":             alpha,
        "k_threshold":       k,
        "min_days":          min_days,
        "companies_scored":  summary["companies_scored"],
        "alerts_raised":     summary["alerts_raised"],
        "drops":             summary["drops"],
        "rising":            summary["rising"],
    }

    logger.info("Pipeline complete — run_id=%d | %s", run_id, result)
    return result


if __name__ == "__main__":
    # Run this file directly to test the full pipeline:
    # python src/train.py
    #
    # To force a full rescore from scratch:
    # pass reset=True below

    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s %(levelname)s %(message)s"
    )

    result = run_training(reset=True)

    print("\n" + "=" * 40)
    print("Pipeline Run Summary")
    print("=" * 40)
    for key, val in result.items():
        print(f"  {key:<20} : {val}")