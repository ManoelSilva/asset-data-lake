# SQL query constants and functions for MotherDuckLakeService

def CREATE_B3_HIST_FROM_DF():
    return "CREATE TABLE IF NOT EXISTS b3_hist AS SELECT * FROM df"


CREATE_B3_FEATURED_FROM_DF = "CREATE TABLE IF NOT EXISTS b3_featured AS SELECT * FROM df"
CREATE_B3_HIST_FROM_B3_DATA = "CREATE TABLE IF NOT EXISTS b3_hist AS SELECT * FROM b3_data LIMIT 0"
INSERT_OR_REPLACE_B3_HIST = "INSERT OR REPLACE INTO b3_hist SELECT * FROM b3_data"
SELECT_ALL_B3_HIST = "SELECT * FROM b3_hist"
SELECT_B3_HIST_COUNT = "SELECT COUNT(*) FROM b3_hist"
SELECT_B3_HIST_MAX_DATE = "SELECT MAX(date) FROM b3_hist"
SELECT_B3_HIST_MIN_DATE = "SELECT MIN(date) FROM b3_hist"


def primary_query(ticker, target_date, days_back):
    return f"""
    SELECT * FROM b3_hist 
    WHERE TRIM(ticker) = '{ticker}' 
    AND date <= CAST('{target_date}' AS DATE)
    AND date >= CAST('{target_date}' AS DATE) - INTERVAL {days_back} DAY
    ORDER BY date ASC
    """


def fallback_query(ticker, target_date, missing):
    return f"""
    SELECT * FROM (
        SELECT * FROM b3_hist
        WHERE TRIM(ticker) = '{ticker}'
        AND date < CAST('{target_date}' AS DATE)
        ORDER BY date DESC
        LIMIT {missing}
    ) t
    ORDER BY date ASC
    """


def fetch_latest_asset_row_query(ticker):
    return f"""
    SELECT * FROM b3_hist
    WHERE TRIM(ticker) = '{ticker.strip().upper()}'
    ORDER BY date DESC
    LIMIT 1
    """
