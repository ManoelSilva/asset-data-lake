import logging

import duckdb
import pandas as pd

from b3.parser import B3HistFileParser
from b3.transformer import B3Transformer


class MotherDuckLakeService(object):
    def __init__(self):
        self._b3_parser = B3HistFileParser(file_path='assets/COTAHIST_M082025.txt')
        self._md = duckdb.connect('md:b3')

    def create_b3_lake(self):
        df = self._b3_parser.parse_b3_hist_quota()
        self._md.execute("CREATE TABLE IF NOT EXISTS b3_hist AS SELECT * FROM df")

    def create_b3_featured_lake(self):
        logging.info(f"Creating B3 featured lake..")
        df = B3Transformer.transform_b3_hist_quota(self._md.execute("SELECT * FROM b3_hist").df())
        self._md.execute("CREATE TABLE IF NOT EXISTS b3_featured AS SELECT * FROM df")

    def fetch_asset_with_historical_context(self, single_asset_data: pd.DataFrame, days_back: int = 30) -> pd.DataFrame:
        """
        Fetches historical data for a single asset to complement it with enough context
        for the transform_b3_hist_quota method to calculate all features.

        Args:
            single_asset_data: DataFrame with a single row containing asset data
            days_back: Number of days of historical data to fetch (default 30 to ensure all features can be calculated)

        Returns:
            DataFrame with the single asset data plus historical context
        """
        if single_asset_data.empty:
            logging.warning("Single asset data is empty")
            return single_asset_data

        if len(single_asset_data) > 1:
            logging.warning("Single asset data contains more than one row, taking the first one")
            single_asset_data = single_asset_data.iloc[[0]]

        # Extract asset information
        ticker = str(single_asset_data['ticker'].iloc[0]).strip()
        target_date = single_asset_data['date'].iloc[0]

        logging.info(f"Fetching historical context for ticker {ticker} on date {target_date}")

        # Query historical data from the lake
        # We need data from (target_date - days_back) to target_date for the specific ticker
        # If the window is sparse, we will fallback to fetch the most recent older rows
        # before target_date until we have enough history.
        primary_query = f"""
        SELECT * FROM b3_hist 
        WHERE TRIM(ticker) = '{ticker}' 
        AND date <= CAST('{target_date}' AS DATE)
        AND date >= CAST('{target_date}' AS DATE) - INTERVAL {days_back} DAY
        ORDER BY date ASC
        """

        try:
            historical_data = self._md.execute(primary_query).df()

            # Remove the single asset date if it exists to avoid duplicates later
            historical_data = historical_data[historical_data['date'] != target_date]

            # Determine required history size for features (26 total incl. target row)
            required_rows = 26
            required_hist_rows = max(1, required_rows - 1)

            # Fallback: fetch most recent older rows before target_date if window is sparse
            if len(historical_data) < required_hist_rows:
                missing = required_hist_rows - len(historical_data)
                fallback_query = f"""
                SELECT * FROM (
                    SELECT * FROM b3_hist
                    WHERE TRIM(ticker) = '{ticker}'
                    AND date < CAST('{target_date}' AS DATE)
                    ORDER BY date DESC
                    LIMIT {missing}
                ) t
                ORDER BY date ASC
                """
                older_data = self._md.execute(fallback_query).df()
                if not older_data.empty:
                    historical_data = pd.concat([older_data, historical_data], ignore_index=True)

            if historical_data.empty:
                logging.warning(f"No historical data found for ticker {ticker}")
                return single_asset_data

            if len(historical_data) < required_hist_rows:
                logging.warning(
                    f"Limited historical data for {ticker}: {len(historical_data)} rows (recommended >= {required_hist_rows})")

            # Combine and sort
            combined_data = pd.concat([historical_data, single_asset_data], ignore_index=True)
            combined_data = combined_data.sort_values(['ticker', 'date']).reset_index(drop=True)

            logging.info(
                f"Successfully combined {len(historical_data)} historical records with single asset data for {ticker}")
            return combined_data

        except Exception as e:
            logging.error(f"Error fetching historical data for {ticker}: {str(e)}")
            return single_asset_data

    def transform_single_asset_with_context(self, single_asset_data: pd.DataFrame, days_back: int = 30) -> pd.DataFrame:
        """
        Convenience method that fetches historical context and applies transformation to a single asset.
        
        Args:
            single_asset_data: DataFrame with a single row containing asset data
            days_back: Number of days of historical data to fetch
            
        Returns:
            Transformed DataFrame with features calculated
        """
        # Get historical context
        asset_with_context = self.fetch_asset_with_historical_context(single_asset_data, days_back)

        # Apply transformation
        transformed_data = B3Transformer.transform_b3_hist_quota(asset_with_context)

        # Return only the row for the original date if it exists
        if not single_asset_data.empty:
            target_date = single_asset_data['date'].iloc[0]
            result = transformed_data[transformed_data['date'] == target_date]
            if not result.empty:
                return result

        # If no exact match, return the last row (most recent)
        return transformed_data.tail(1) if not transformed_data.empty else pd.DataFrame()

    def delete_lake(self):
        pass
