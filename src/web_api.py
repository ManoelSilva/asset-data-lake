from dotenv import load_dotenv
from flask import Flask, jsonify

from service.asset import AssetService

load_dotenv()

app = Flask(__name__)
asset_service = AssetService()


@app.route('/asset/<ticker>', methods=['GET'])
def get_asset(ticker):
    """
    Get asset information by ticker symbol.
    
    Args:
        ticker (str): The ticker symbol to search for
        
    Returns:
        JSON response with asset data or error message
    """
    try:
        asset_data = asset_service.get_asset(ticker)

        if asset_data is None:
            return jsonify({
                'error': 'Asset not found',
                'ticker': ticker
            }), 404

        return jsonify({
            'ticker': ticker,
            'data': asset_data
        }), 200

    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5002)
