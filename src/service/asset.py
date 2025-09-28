from service.md_lake import MotherDuckLakeService
from service.scrapper import B3ScrapperService


class AssetService:
    def __init__(self):
        self._scrapper = B3ScrapperService()
        self._md_lake = MotherDuckLakeService()

    def get_asset(self, ticker: str, target_date: str = None):
        """
        Get a single asset with all features calculated using historical context from the data lake.
        
        Args:
            ticker: Asset ticker symbol
            target_date: Specific date to get data for (YYYY-MM-DD format). If None, gets latest available.
            
        Returns:
            Dictionary with transformed asset data including all engineered features
        """
        # First, get the single asset data from scrapper
        b3_data = self._scrapper.fetch_data()
        asset_data = b3_data[b3_data['ticker'].str.strip() == ticker.upper()]

        if asset_data.empty:
            return None

        # If target_date is specified, filter to that date
        if target_date:
            asset_data = asset_data[asset_data['date'] == target_date]
            if asset_data.empty:
                return None
        else:
            # Get the most recent data
            asset_data = asset_data.tail(1)

        # Use the new method to get historical context and transform
        transformed_data = self._md_lake.transform_single_asset_with_context(asset_data)

        if transformed_data.empty:
            return None

        # Convert to dict for JSON serialization
        return transformed_data.to_dict('records')[0]
