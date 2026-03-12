# ============================================================
#  EngagementGuard EWMA — Central Configuration
#  All settings live here. Change DB credentials below.
# ============================================================

# ── MySQL Database ────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 3306
DB_NAME     = "engagement_guard_ewma"
DB_USER     = "root"
DB_PASSWORD = "1234"   # <-- change this

# ── EWMA Parameters ───────────────────────────────────────────
# Alpha: how fast the mean reacts to new data (0.01 to 0.99)
# Lower = smoother and slower. Higher = faster but noisier.
EWMA_ALPHA    = 0.15

# K: how many std deviations away from mean triggers an alert
# 2.5 is a good balance between sensitivity and noise
EWMA_K        = 2.5

# Minimum days of history needed before a company gets scored
# Before this, every day is labelled Normal
EWMA_MIN_DAYS = 7

# ── Scheduler ─────────────────────────────────────────────────
# Pipeline runs automatically every day at this time (24hr format)
SCHEDULE_HOUR   = 2    # 2:00 AM
SCHEDULE_MINUTE = 0

# ── API ───────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8000

# ── Logging ───────────────────────────────────────────────────
LOG_FILE  = "logs/engagement_guard.log"
LOG_LEVEL = "INFO"