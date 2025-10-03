[Leia em português](README.pt-br.md)

# Asset Data Lake

Asset Data Lake is a Python project for parsing, transforming, and creating a feature-rich data lake from B3 (Brazilian
Stock Exchange) historical data. It leverages DuckDB, pandas, and other modern data tools to enable efficient analytics
and feature engineering for financial assets.

## Features

- Parse B3 historical files (COTAHIST)
- Transform raw data into engineered features for ML/analytics
- Store and query data using DuckDB (MotherDuck)
- Easily extensible for new data sources and transformations

## Project Structure

```
asset-data-lake/
├── src/
│   ├── b3/
│   │   ├── parser.py         # Parses B3 historical files
│   │   ├── transformer.py    # Feature engineering for B3 data
│   ├── md_lake.py            # MotherDuckLakeService: manages DuckDB lake
│   ├── lake_creator_app.py   # CLI entrypoint for creating lakes
├── requirements.txt          # Python dependencies
```

## Quickstart

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
2. **Prepare B3 data**
    - Place COTAHIST files in `src/b3/assets/` (e.g., `COTAHIST_A2025.txt`)
3. **Run the lake creator**
   ```bash
   python src/lake_creator_app.py
   ```
   This will parse and transform the B3 data, creating DuckDB tables for raw and featured data.

## Main Components

- `B3HistFileParser` (`src/b3/parser.py`): Parses fixed-width B3 historical files into pandas DataFrames.
- `B3Transformer` (`src/b3/transformer.py`): Engineers features (returns, volatility, momentum, etc.) from raw data.
- `MotherDuckLakeService` (`src/md_lake.py`): Manages DuckDB connection and table creation.
- `LakeCreatorApp` (`src/lake_creator_app.py`): CLI app to orchestrate the process.

## Example Data

Sample B3 files are provided in `src/b3/assets/` for testing and development.

## Requirements

- Python 3.8+
- pandas, numpy, pyarrow, requests, boto3, duckdb, python-dotenv

## REST API

The project includes a Flask-based REST API for accessing the data lake:

### Main Endpoints

- `GET /asset/<ticker>` - Get asset information by ticker symbol
- `GET /assets` - List available assets with search and pagination
  - Query parameters: `search`, `page`, `page_size`
- `POST /scheduled/b3-data-update` - Update B3 data from source
- `GET /health` - Health check endpoint
- `GET /swagger` - Interactive API documentation (Swagger UI)
- `GET /swagger.yaml` - OpenAPI specification

### API Usage Examples

```python
import requests

# Get asset information
response = requests.get("http://localhost:5002/asset/PETR4")
asset_data = response.json()

# Search assets
response = requests.get("http://localhost:5002/assets?search=PETR&page=1&page_size=10")
assets = response.json()

# Update B3 data
response = requests.post("http://localhost:5002/scheduled/b3-data-update")
update_status = response.json()
```

## Environment Configuration

### Required Environment Variables

```bash
export MOTHERDUCK_TOKEN="your_motherduck_token_here"
export environment="AWS"  # or "LOCAL" for local development
```

### Local Development Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**
   ```bash
   export MOTHERDUCK_TOKEN="your_token"
   export environment="LOCAL"
   ```

3. **Run the API server**
   ```bash
   python src/web_api.py
   ```

## Data Sources and Formats

### B3 Historical Data (COTAHIST)
- **Format**: Fixed-width text files
- **Source**: B3 (Brazilian Stock Exchange)
- **Update Frequency**: Daily
- **File Naming**: `COTAHIST_AYYYY.txt` (annual), `COTAHIST_MMMYYYY.TXT` (monthly)

### Data Processing Pipeline
1. **Parsing**: Fixed-width files → pandas DataFrames
2. **Transformation**: Feature engineering (returns, volatility, momentum)
3. **Storage**: DuckDB tables (raw and featured data)
4. **API Access**: REST endpoints for data retrieval

## Service Deployment

### Production Deployment (AWS EC2)

The service is designed to run as a systemd service on AWS EC2:

```bash
# Deploy using the provided script
sudo MOTHERDUCK_TOKEN=your_token EC2_HOST=your_ip bash deploy_asset_data_lake.sh
```

### Service Management

```bash
# Check service status
sudo systemctl status asset-data-lake

# View logs
sudo journalctl -u asset-data-lake -f

# Restart service
sudo systemctl restart asset-data-lake
```

## Data Lake Schema

### Tables

- **b3_hist**: Raw B3 historical data
- **b3_featured**: Processed data with engineered features
- **Asset metadata**: Company information and ticker mappings

### Features Engineered

- **Price-based**: Returns, volatility, moving averages
- **Volume-based**: Volume trends, liquidity metrics
- **Technical indicators**: RSI, MACD, Bollinger Bands
- **Market indicators**: Sector performance, market cap

## Extending

- Add new parsers/transformers for other asset classes in `src/`
- Modify `MotherDuckLakeService` to support new tables or analytics
- Extend the API with new endpoints for custom analytics
- Add data validation and quality checks

## License

MIT License

---
[Read in Portuguese](README.pt-br.md)
