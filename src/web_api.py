import os

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint

from service.asset_handler import AssetApiHandler
from service.business_day import BusinessDayService
from service.db.asset import AssetService
from service.db.md_lake import MotherDuckLakeService
from service.scrapper import B3ScrapperService

load_dotenv()

app = Flask(__name__)
CORS(app)
md_lake = MotherDuckLakeService()
b3_scrapper = B3ScrapperService(BusinessDayService(md_lake))
asset_handler = AssetApiHandler(AssetService(md_lake, b3_scrapper))

SWAGGER_URL = '/swagger'
API_URL = '/swagger.yaml'

"""
Swagger UI is available at /swagger
The OpenAPI YAML spec is served at /swagger.yaml
"""
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Asset Data Lake API"
    }
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


@app.route('/swagger.yaml')
def swagger_yaml():
    """
    Serve the OpenAPI (Swagger) YAML specification for the API.
    This is used by the Swagger UI at /swagger.
    """
    yaml_path = os.path.join(os.path.dirname(__file__), 'swagger.yaml')
    with open(yaml_path, 'r') as f:
        swagger_spec = f.read()
    return app.response_class(swagger_spec, mimetype='application/yaml')


@app.route('/asset/<ticker>', methods=['GET'])
def get_asset(ticker):
    """
    Get asset information by ticker symbol.
    
    Args:
        ticker (str): The ticker symbol to search for
        
    Returns:
        JSON response with asset data or error message
    """
    response, status = asset_handler.get_asset(ticker)
    return jsonify(response), status


@app.route('/assets', methods=['GET'])
def list_assets():
    """
    List available assets from b3_featured table with search and pagination.
    
    Query Parameters:
        search (str): Search term to filter assets (minimum 3 characters)
        page (int): Page number (default: 1)
        page_size (int): Number of items per page (default: 20, max: 100)
        
    Returns:
        JSON response with paginated asset list and metadata
    """
    try:
        # Get query parameters
        search_term = request.args.get('search', '').strip()
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        response, status = asset_handler.list_assets(search_term, page, page_size)

        return jsonify(response), status

    except ValueError as e:
        return jsonify({
            'error': 'Invalid parameter',
            'message': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/scheduled/b3-data-update', methods=['POST'])
def update_b3_data():
    """
    Scheduled job endpoint to fetch B3 data and update the b3_hist table.
    
    This endpoint:
    1. Fetches the latest B3 historical data using B3ScrapperService
    2. Updates the b3_hist table in the data lake with the new data
    3. Returns status information about the operation
    
    Returns:
        JSON response with operation status and statistics
    """
    try:
        # Fetch data from B3 source
        app.logger.info("Starting B3 data fetch...")
        b3_data = b3_scrapper.fetch_data()

        if b3_data is None or b3_data.empty:
            return jsonify({
                'error': 'No data retrieved',
                'message': 'B3ScrapperService returned empty data'
            }), 400

        app.logger.info(f"Fetched {len(b3_data)} records from B3 source")

        md_lake.update_b3_hist_table(b3_data)
        stats = md_lake.get_b3_hist_stats()

        app.logger.info(f"Successfully updated b3_hist table. Total records: {stats['total_records']}")

        return jsonify({
            'status': 'success',
            'message': 'B3 data successfully updated',
            'statistics': stats
        }), 200

    except Exception as e:
        app.logger.error(f"Error updating B3 data: {str(e)}")
        return jsonify({
            'error': 'Failed to update B3 data',
            'message': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5002)
