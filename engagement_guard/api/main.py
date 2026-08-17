# ============================================================
#  engagement_guard/api/main.py
#  FastAPI endpoints for Engagement Guard EWMA
#  - Query alerts and engagement metrics
#  - Get recommendations when engagement drops
#  - Manage EWMA state and trigger manual runs
# ============================================================

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.config import API_HOST, API_PORT
from src.db_connection import get_connection
from src.train import run_training
from src.recommendations import (
    get_recommendations_for_company,
    get_engagement_severity,
    get_trend_direction
)

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Engagement Guard EWMA API",
    description="Monitor user engagement and detect anomalies using EWMA",
    version="1.0.0"
)

# ── Health Check ───────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ── Engagement Alerts Endpoints ────────────────────────────────────────────

@app.get("/api/v1/alerts")
def get_alerts(
    company_id: Optional[int] = Query(None, description="Filter by company ID"),
    alert_type: Optional[str] = Query(None, description="Filter by alert type: Drop, Rising, Normal"),
    days: int = Query(30, description="Look back N days"),
    limit: int = Query(100, description="Max results to return")
):
    """
    Get recent engagement alerts across companies or for a specific company.
    
    Returns:
        - alert_date: date of the alert
        - alert_type: Drop / Rising / Normal
        - company_id, company_name
        - actual_count: real engagement metric
        - ewma_mean, ewma_std: statistical bounds
        - deviation: how many std deviations off the mean
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        since = (date.today() - timedelta(days=days)).isoformat()
        
        # Build query
        query = """
            SELECT 
                a.company_id,
                c.company_name,
                a.alert_date,
                a.alert_type,
                a.actual_count,
                a.ewma_mean,
                a.ewma_std,
                a.deviation
            FROM engagement_alerts a
            JOIN companies c ON a.company_id = c.company_id
            WHERE a.alert_date >= %s
        """
        params = [since]
        
        if company_id:
            query += " AND a.company_id = %s"
            params.append(company_id)
        
        if alert_type and alert_type in ["Drop", "Rising", "Normal"]:
            query += " AND a.alert_type = %s"
            params.append(alert_type)
        
        query += " ORDER BY a.alert_date DESC, a.company_id ASC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "count": len(rows),
            "filters": {
                "days": days,
                "company_id": company_id,
                "alert_type": alert_type
            },
            "alerts": rows
        }
    
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/alerts/{company_id}")
def get_company_alerts(
    company_id: int,
    days: int = Query(90, description="Look back N days"),
    limit: int = Query(100, description="Max results")
):
    """
    Get all alerts for a specific company with full details.
    Includes engagement trend analysis.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        since = (date.today() - timedelta(days=days)).isoformat()
        
        # Get company info
        cursor.execute(
            "SELECT company_id, company_name FROM companies WHERE company_id = %s",
            (company_id,)
        )
        company = cursor.fetchone()
        
        if not company:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Get alerts
        cursor.execute(
            """
            SELECT 
                alert_date,
                alert_type,
                actual_count,
                ewma_mean,
                ewma_std,
                deviation
            FROM engagement_alerts
            WHERE company_id = %s AND alert_date >= %s
            ORDER BY alert_date DESC
            LIMIT %s
            """,
            (company_id, since, limit)
        )
        alerts = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Calculate statistics
        if alerts:
            drop_count = sum(1 for a in alerts if a["alert_type"] == "Drop")
            rising_count = sum(1 for a in alerts if a["alert_type"] == "Rising")
            avg_engagement = sum(a["actual_count"] for a in alerts) / len(alerts)
        else:
            drop_count = rising_count = avg_engagement = 0
        
        return {
            "company": company,
            "period": {"days": days, "since": since},
            "summary": {
                "total_alerts": len(alerts),
                "drops": drop_count,
                "rising": rising_count,
                "avg_engagement": round(avg_engagement, 2)
            },
            "alerts": alerts
        }
    
    except Exception as e:
        logger.error(f"Error fetching company alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Engagement Trend Endpoints ─────────────────────────────────────────────

@app.get("/api/v1/trends/{company_id}")
def get_engagement_trend(
    company_id: int,
    days: int = Query(90, description="Trend window in days")
):
    """
    Get engagement trend over time for a company.
    Includes actual engagement counts and EWMA bounds.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        since = (date.today() - timedelta(days=days)).isoformat()
        
        cursor.execute(
            """
            SELECT 
                alert_date as date,
                actual_count,
                ewma_mean,
                ewma_std,
                alert_type
            FROM engagement_alerts
            WHERE company_id = %s AND alert_date >= %s
            ORDER BY alert_date ASC
            """,
            (company_id, since)
        )
        
        trend_data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not trend_data:
            raise HTTPException(status_code=404, detail="No trend data found")
        
        # Calculate trend direction
        if len(trend_data) > 1:
            first_count = trend_data[0]["actual_count"]
            last_count = trend_data[-1]["actual_count"]
            trend_direction = get_trend_direction(first_count, last_count)
        else:
            trend_direction = "stable"
        
        return {
            "company_id": company_id,
            "period": {"days": days, "since": since},
            "trend_direction": trend_direction,
            "data_points": len(trend_data),
            "trend": trend_data
        }
    
    except Exception as e:
        logger.error(f"Error fetching trend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Recommendations Endpoints ──────────────────────────────────────────────

@app.get("/api/v1/recommendations/{company_id}")
def get_recommendations(
    company_id: int,
    recent_days: int = Query(7, description="Analyze last N days")
):
    """
    Get AI-powered recommendations for a company based on recent engagement.
    
    Returns:
        - severity: critical, warning, healthy
        - recommendations: list of actionable suggestions
        - confidence: score (0-1)
        - reason: explanation of the recommendation
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get company
        cursor.execute(
            "SELECT company_id, company_name FROM companies WHERE company_id = %s",
            (company_id,)
        )
        company = cursor.fetchone()
        
        if not company:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Get recent alerts
        since = (date.today() - timedelta(days=recent_days)).isoformat()
        
        cursor.execute(
            """
            SELECT 
                alert_date,
                alert_type,
                actual_count,
                ewma_mean,
                deviation
            FROM engagement_alerts
            WHERE company_id = %s AND alert_date >= %s
            ORDER BY alert_date DESC
            """,
            (company_id, since)
        )
        
        recent_alerts = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not recent_alerts:
            return {
                "company": company,
                "period": {"days": recent_days},
                "severity": "healthy",
                "recommendations": [
                    "Keep monitoring engagement patterns",
                    "Continue with current engagement strategy"
                ],
                "confidence": 0.8,
                "reason": "No recent alerts — engagement is stable"
            }
        
        # Get recommendations based on recent patterns
        recommendations = get_recommendations_for_company(recent_alerts, company)
        severity = get_engagement_severity(recent_alerts)
        
        return {
            "company": company,
            "period": {"days": recent_days},
            "severity": severity,
            "recommendations": recommendations["suggestions"],
            "confidence": recommendations["confidence"],
            "reason": recommendations["reason"]
        }
    
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── EWMA State & Pipeline Endpoints ────────────────────────────────────────

@app.get("/api/v1/state/{company_id}")
def get_ewma_state(company_id: int):
    """
    Get the current saved EWMA state for a company.
    Shows the running mean, variance, and number of days processed.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            """
            SELECT 
                company_id,
                ewma_mean,
                ewma_var,
                n_days,
                last_updated
            FROM ewma_state
            WHERE company_id = %s
            """,
            (company_id,)
        )
        
        state = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not state:
            raise HTTPException(status_code=404, detail="No EWMA state found for company")
        
        return {
            "company_id": state["company_id"],
            "ewma_mean": round(state["ewma_mean"], 4),
            "ewma_variance": round(state["ewma_var"], 6),
            "days_processed": state["n_days"],
            "last_updated": state["last_updated"].isoformat() if state["last_updated"] else None
        }
    
    except Exception as e:
        logger.error(f"Error fetching EWMA state: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/pipeline/run")
def trigger_pipeline_run(
    reset: bool = Query(False, description="Clear EWMA state and rescore from scratch"),
    notes: str = Query("", description="Optional notes about this run")
):
    """
    Manually trigger the EWMA pipeline run.
    Rescores all companies and updates alerts.
    
    Parameters:
        - reset: If True, clears saved EWMA state (forces full rescore)
        - notes: Optional description of why this run was triggered
    """
    try:
        logger.info(f"Pipeline run triggered manually. Reset={reset}, Notes={notes}")
        
        result = run_training(reset=reset, notes=notes)
        
        return {
            "status": "success",
            "run_id": result["run_id"],
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "companies_scored": result["companies_scored"],
                "alerts_raised": result["alerts_raised"],
                "drops": result["drops"],
                "rising": result["rising"]
            }
        }
    
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Model Registry Endpoints ───────────────────────────────────────────────

@app.get("/api/v1/model-registry")
def get_model_runs(
    limit: int = Query(50, description="Number of recent runs to return"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    Get history of all pipeline runs with their parameters and results.
    Useful for auditing and comparing EWMA parameter effectiveness.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            """
            SELECT 
                run_id,
                alpha,
                k_threshold,
                min_days,
                companies_scored,
                alerts_raised,
                notes,
                created_at
            FROM model_registry
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )
        
        runs = cursor.fetchall()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as count FROM model_registry")
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "runs": runs
        }
    
    except Exception as e:
        logger.error(f"Error fetching model registry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Company Management Endpoints ───────────────────────────────────────────

@app.get("/api/v1/companies")
def list_companies(
    limit: int = Query(100, description="Max results"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    List all monitored companies.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            """
            SELECT 
                c.company_id,
                c.company_name,
                COUNT(CASE WHEN a.alert_type = 'Drop' THEN 1 END) as recent_drops,
                COUNT(CASE WHEN a.alert_type = 'Rising' THEN 1 END) as recent_rising,
                MAX(a.alert_date) as last_alert_date
            FROM companies c
            LEFT JOIN engagement_alerts a ON c.company_id = a.company_id
                AND a.alert_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY c.company_id, c.company_name
            ORDER BY c.company_name
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )
        
        companies = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as count FROM companies")
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "companies": companies
        }
    
    except Exception as e:
        logger.error(f"Error listing companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Anomaly Detection Endpoints ────────────────────────────────────────────

@app.get("/api/v1/anomalies")
def get_critical_anomalies(
    severity_threshold: float = Query(2.5, description="Min deviation to flag as critical"),
    days: int = Query(7, description="Look back N days"),
    limit: int = Query(50, description="Max results")
):
    """
    Get all critical anomalies across all companies.
    Sorted by severity (highest deviation first).
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        since = (date.today() - timedelta(days=days)).isoformat()
        
        cursor.execute(
            """
            SELECT 
                c.company_name,
                a.company_id,
                a.alert_date,
                a.alert_type,
                a.actual_count,
                a.ewma_mean,
                a.ewma_std,
                a.deviation,
                ABS(a.deviation) as severity
            FROM engagement_alerts a
            JOIN companies c ON a.company_id = c.company_id
            WHERE a.alert_date >= %s
                AND ABS(a.deviation) >= %s
                AND a.alert_type IN ('Drop', 'Rising')
            ORDER BY ABS(a.deviation) DESC
            LIMIT %s
            """,
            (since, severity_threshold, limit)
        )
        
        anomalies = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "filters": {
                "severity_threshold": severity_threshold,
                "days": days
            },
            "count": len(anomalies),
            "anomalies": anomalies
        }
    
    except Exception as e:
        logger.error(f"Error fetching anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Dashboard Summary Endpoint ────────────────────────────────────────────

@app.get("/api/v1/dashboard/summary")
def get_dashboard_summary():
    """
    Get a high-level dashboard summary across all companies.
    Shows overall health, critical alerts, and recent trends.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        seven_days_ago = (date.today() - timedelta(days=7)).isoformat()
        
        # Total companies
        cursor.execute("SELECT COUNT(*) as count FROM companies")
        total_companies = cursor.fetchone()["count"]
        
        # Recent drops
        cursor.execute(
            """
            SELECT COUNT(*) as count FROM engagement_alerts
            WHERE alert_type = 'Drop' AND alert_date >= %s
            """,
            (seven_days_ago,)
        )
        recent_drops = cursor.fetchone()["count"]
        
        # Recent rising
        cursor.execute(
            """
            SELECT COUNT(*) as count FROM engagement_alerts
            WHERE alert_type = 'Rising' AND alert_date >= %s
            """,
            (seven_days_ago,)
        )
        recent_rising = cursor.fetchone()["count"]
        
        # Average deviation (recent)
        cursor.execute(
            """
            SELECT AVG(ABS(deviation)) as avg_deviation
            FROM engagement_alerts
            WHERE alert_date >= %s AND alert_type != 'Normal'
            """,
            (seven_days_ago,)
        )
        avg_deviation = cursor.fetchone()["avg_deviation"]
        
        # Companies with recent drops
        cursor.execute(
            """
            SELECT DISTINCT company_id, company_name
            FROM engagement_alerts a
            JOIN companies c ON a.company_id = c.company_id
            WHERE a.alert_type = 'Drop' AND a.alert_date >= %s
            """,
            (seven_days_ago,)
        )
        at_risk_companies = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overview": {
                "total_companies": total_companies,
                "companies_at_risk": len(at_risk_companies)
            },
            "last_7_days": {
                "drops": recent_drops,
                "rising": recent_rising,
                "avg_anomaly_deviation": round(avg_deviation or 0, 2)
            },
            "at_risk_companies": at_risk_companies
        }
    
    except Exception as e:
        logger.error(f"Error generating dashboard summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Startup ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    
    logger.info(f"Starting Engagement Guard API on {API_HOST}:{API_PORT}")
    uvicorn.run(app, host=API_HOST, port=API_PORT)
