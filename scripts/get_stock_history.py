import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

from seed_utils import append_seed_rows, current_snapshot_ts


STOCK_HISTORY_COLUMNS = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'Symbol']


def normalize_stock_history(stock_data, symbol):
    """Flattens per-symbol history into the stable CSV schema used by seeds."""
    if stock_data.empty:
        return pd.DataFrame(columns=STOCK_HISTORY_COLUMNS)

    normalized = stock_data.copy()

    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = normalized.columns.get_level_values(0)

    normalized = normalized.reset_index()

    if 'Date' not in normalized.columns and len(normalized.columns) > 0:
        normalized = normalized.rename(columns={normalized.columns[0]: 'Date'})

    normalized['Symbol'] = symbol

    for column in STOCK_HISTORY_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    return normalized[STOCK_HISTORY_COLUMNS]

def read_symbols_from_file(file_path):
    """Reads stock symbols from a file."""
    try:
        with open(file_path, 'r') as file:
            symbols = file.read().splitlines()
        return symbols
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

def validate_symbol(symbol):
    """Validates a symbol using yfinance."""
    try:
        stock = yf.Ticker(symbol)
        if stock.history(period='1d').empty:
            return False
        return True
    except Exception as e:
        print(f"Error validating symbol {symbol}: {e}")
        return False

def fetch_stock_data(symbol, start_date, end_date):
    """Fetches stock data for a valid symbol."""
    try:
        return yf.download(symbol, start=start_date, end=end_date)
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

def main():
    # Define the date range (last 360 days)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=360)).strftime('%Y-%m-%d')

    # Read the stock symbols from the file
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, 'symbols.txt')
    symbols = read_symbols_from_file(file_path)

    if not symbols:
        print("No symbols found in file.")
        return

    all_data = []

    # Validate and fetch stock data for each symbol
    for symbol in symbols:
        if validate_symbol(symbol):
            print(f"Fetching data for: {symbol}")
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            stock_data = normalize_stock_history(stock_data, symbol)
            if not stock_data.empty:
                all_data.append(stock_data)
        else:
            print(f"Invalid symbol: {symbol}")

    if all_data:
        # Concatenate all stock data into one DataFrame
        result_df = pd.concat(all_data, ignore_index=True)
        snapshot_ts = current_snapshot_ts()
        result_df['snapshot_ts'] = snapshot_ts

        seed_path = os.path.join(script_dir, '..', 'seeds', 'ticker_history.csv')
        added_rows = append_seed_rows(seed_path, result_df, sort_columns=['Symbol', 'Date'])
        print(f"Stock history snapshot {snapshot_ts} merged into {seed_path} ({added_rows} new rows)")
    else:
        print("No valid stock data found to save.")

if __name__ == "__main__":
    main()
