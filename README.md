[Leia em português](README.pt-br.md)

# Asset Data Lake

Asset Data Lake is a Python project for parsing, transforming, and creating a feature-rich data lake from B3 (Brazilian Stock Exchange) historical data. It leverages DuckDB, pandas, and other modern data tools to enable efficient analytics and feature engineering for financial assets.

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

## Extending
- Add new parsers/transformers for other asset classes in `src/`
- Modify `MotherDuckLakeService` to support new tables or analytics

## License
MIT License

---
[Read in Portuguese](README.pt-br.md)
