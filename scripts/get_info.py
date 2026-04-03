import yfinance as yf
import pandas as pd
import os

from seed_utils import append_seed_rows, current_snapshot_ts


def drop_case_insensitive_duplicate_columns(df):
    """Keeps the first occurrence of case-insensitive duplicate columns."""
    seen = set()
    keep = []

    for column in df.columns:
        key = str(column).lower()
        if key in seen:
            continue
        seen.add(key)
        keep.append(column)

    return df.loc[:, keep]

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

def fetch_stock_info(symbol):
    """Fetches stock information (info endpoint) for a valid symbol."""
    try:
        stock = yf.Ticker(symbol)
        return stock.info
    except Exception as e:
        print(f"Error fetching info for {symbol}: {e}")
        return {}

def main():
    # Read the stock symbols from the file
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, 'symbols.txt')
    symbols = read_symbols_from_file(file_path)

    if not symbols:
        print("No symbols found in file.")
        return

    all_info = []

    # Validate and fetch stock info for each symbol
    for symbol in symbols:
        if validate_symbol(symbol):
            print(f"Fetching info for: {symbol}")
            stock_info = fetch_stock_info(symbol)
            if stock_info:
                stock_info['symbol'] = symbol
                all_info.append(stock_info)
        else:
            print(f"Invalid symbol: {symbol}")

    if all_info:
        # Create a DataFrame from the info data
        info_df = pd.DataFrame(all_info)
        info_df = drop_case_insensitive_duplicate_columns(info_df)

        if 'Symbol' in info_df.columns:
            info_df = info_df.rename(columns={'Symbol': 'symbol'})

        snapshot_ts = current_snapshot_ts()
        info_df['snapshot_ts'] = snapshot_ts

        seed_path = os.path.join(script_dir, '..', 'seeds', 'ticker_info.csv')
        added_rows = append_seed_rows(seed_path, info_df, sort_columns=['symbol'])
        print(f"Info snapshot {snapshot_ts} merged into {seed_path} ({added_rows} new rows)")
    else:
        print("No valid stock info found to save.")

if __name__ == "__main__":
    main()
