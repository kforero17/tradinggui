# Stock Market Analysis Tool

A Python-based tool for tracking and analyzing stock market data, focusing on S&P 500 and NASDAQ listed companies. The tool fetches historical and fundamental data to calculate various momentum and valuation metrics.

## Features

- Loads and deduplicates tickers from S&P 500 and NASDAQ
- Fetches historical and fundamental data using Yahoo Finance API
- Calculates momentum metrics (MA, EMA, price movements)
- Calculates valuation metrics (PE, PB, EBITDA/EV ratios)
- Stores data in SQLite database for analysis
- Implements rate limiting and error handling
- Provides logging and monitoring capabilities

## Project Structure

```
TradingGui/
├── data/               # Data storage
│   ├── raw/           # Raw input files
│   └── processed/     # Processed data and database
├── src/               # Source code
│   ├── config/        # Configuration
│   ├── data/          # Data handling
│   ├── analysis/      # Analysis modules
│   └── utils/         # Utilities
└── tests/             # Unit tests
```

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and configure your settings
5. Place your S&P 500 and NASDAQ CSV files in `data/raw/`

## Usage

Run the main script:
```bash
python -m src.main
```

## Development

- Run tests: `pytest`
- Check code style: `flake8 src tests`
- Type checking: `mypy src`

## License

MIT License 