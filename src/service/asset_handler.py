from src.service.db.asset import AssetService


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
