import logging
from datetime import date, timedelta

import requests

from src.service.db.md_lake import MotherDuckLakeService


class BusinessDayService:
    def __init__(self, md: MotherDuckLakeService):
        self._md = md

    def get_last_business_day(self, target: date = None) -> date | None:
        """
        Returns the last business day in Brazil before the given date (default = today).
        Business day = not Saturday/Sunday and not a national holiday.
        Fallback: If BrasilAPI fails, returns the last available date from DuckDB.
        """
        if target is None:
            target = date.today()
        try:
            # collect holidays for current and previous year
            years = {target.year, target.year - 1}
            holidays = set()
            for y in years:
                url = f"https://brasilapi.com.br/api/feriados/v1/{y}"
                logging.info(f"Fetching holidays for year {y}")
                resp = requests.get(url)
                resp.raise_for_status()
                for item in resp.json():
                    holidays.add(date.fromisoformat(item["date"]))
            # step backwards until a valid business day is found
            dt = target - timedelta(days=1)
            while dt.weekday() >= 5 or dt in holidays:  # weekend or holiday
                dt -= timedelta(days=1)
            return dt
        except Exception as e:
            logging.error(f"Failed to fetch holidays from BrasilAPI, falling back to DuckDB: {e}")
            # fallback: get last available date from DuckDB
            last_date = self._md.get_last_available_date()
            if last_date:
                logging.info(f"Fallback: returning last available date from DuckDB: {last_date}")
                return last_date
            else:
                logging.error("No fallback date available from DuckDB.")
                return None
