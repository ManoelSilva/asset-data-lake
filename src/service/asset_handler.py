from service.db.asset import AssetService


class AssetApiHandler:
    def __init__(self, asset_service: AssetService):
        self._asset_service = asset_service

    def get_asset(self, ticker):
        # Business logic, validation, and payload manipulation for get_asset
        if not ticker or not isinstance(ticker, str):
            return {'error': 'Invalid ticker', 'message': 'Ticker must be a non-empty string'}, 400
        asset_data = self._asset_service.get_asset(ticker)
        if asset_data is None:
            return {'error': 'Asset not found', 'ticker': ticker}, 404
        return {'ticker': ticker, 'data': asset_data}, 200

    def get_asset_history(self, ticker, days, end_date=None):
        if not ticker or not isinstance(ticker, str):
            return {'error': 'Invalid ticker', 'message': 'Ticker must be a non-empty string'}, 400
        
        try:
            days = int(days)
            if days <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return {'error': 'Invalid days parameter', 'message': 'Days must be a positive integer'}, 400
            
        data = self._asset_service.get_historical_data(ticker, days, end_date)
        
        # Structure the response as expected by the client:
        # returns data list directly or dict with data key.
        # The user snippet shows handling:
        # if isinstance(data, list): return data, None
        # elif isinstance(data, dict): ...
        
        # We will return a dict with a 'data' key containing the list
        return {'ticker': ticker, 'data': data}, 200

    def list_assets(self, search_term, page, page_size):
        # Business logic, validation, and payload manipulation for list_assets
        if page < 1:
            return {'error': 'Invalid page number', 'message': 'Page must be greater than 0'}, 400
        if page_size < 1 or page_size > 100:
            return {'error': 'Invalid page size', 'message': 'Page size must be between 1 and 100'}, 400
        if search_term and len(search_term) < 3:
            return {'error': 'Invalid search term', 'message': 'Search term must have at least 3 characters'}, 400
        try:
            result = self._asset_service.list_assets(
                search_term=search_term if search_term else None,
                page=page,
                page_size=page_size
            )
            return result, 200
        except Exception as e:
            return {'error': 'Internal server error', 'message': str(e)}, 500
