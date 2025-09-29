from service.md_lake import MotherDuckLakeService

try:
    from service.scrapper import B3ScrapperService

    SCRAPPER_AVAILABLE = True
except ImportError:
    SCRAPPER_AVAILABLE = False
    B3ScrapperService = None


class AssetService:
    def __init__(self):
        if SCRAPPER_AVAILABLE:
            self._scrapper = B3ScrapperService()
        else:
            self._scrapper = None
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
        if not SCRAPPER_AVAILABLE:
            raise Exception("Scrapper service is not available. Cannot fetch asset data.")

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

    def list_assets(self, search_term: str = None, page: int = 1, page_size: int = 20):
        """
        List available assets from b3_featured table with search and pagination.
        
        Args:
            search_term: Search term to filter assets (minimum 3 characters)
            page: Page number (1-based)
            page_size: Number of items per page
            
        Returns:
            Dictionary with paginated asset list and metadata
        """
        try:
            # Connect to the data lake
            md_lake = self._md_lake

            # Build the base query
            base_query = """
                         SELECT DISTINCT ticker, company
                         FROM b3_featured
                         WHERE ticker IS NOT NULL
                           AND company IS NOT NULL \
                         """

            # Add search filter if provided and has at least 3 characters
            if search_term and len(search_term.strip()) >= 3:
                search_term = search_term.strip().upper()
                base_query += f" AND (UPPER(ticker) LIKE '%{search_term}%' OR UPPER(company) LIKE '%{search_term}%')"

            # Add ordering
            base_query += " ORDER BY ticker ASC"

            # Calculate offset for pagination
            offset = (page - 1) * page_size

            # Get total count for pagination metadata
            count_query = f"SELECT COUNT(*) as total FROM ({base_query}) as filtered_assets"
            total_count = md_lake._md.execute(count_query).fetchone()[0]

            # Get paginated results
            paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"
            results = md_lake._md.execute(paginated_query).df()

            # Convert to list of dictionaries
            assets = results.to_dict('records')

            # Calculate pagination metadata
            total_pages = (total_count + page_size - 1) // page_size
            has_next = page < total_pages
            has_prev = page > 1

            return {
                'assets': assets,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'has_next': has_next,
                    'has_prev': has_prev
                },
                'search_term': search_term
            }

        except Exception as e:
            raise Exception(f"Error listing assets: {str(e)}")
