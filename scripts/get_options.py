import yfinance as yf
import pandas as pd
import os

from seed_utils import append_seed_rows, current_snapshot_ts


OPTION_COLUMNS = [
    'contractSymbol',
    'lastTradeDate',
    'strike',
    'lastPrice',
    'bid',
    'ask',
    'change',
    'percentChange',
    'volume',
    'openInterest',
    'impliedVolatility',
    'inTheMoney',
    'contractSize',
    'currency',
    'Type',
    'Expiration',
    'Symbol',
]


def normalize_option_columns(df):
    """Projects option data into a stable, seed-friendly column set."""
    normalized = df.copy()

    for column in OPTION_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    return normalized[OPTION_COLUMNS]

def read_symbols_from_file(file_path):
    """Reads stock symbols from a file."""
    try:
        with open(file_path, 'r') as file:
            symbols = file.read().splitlines()
        return symbols
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

def fetch_option_data(symbol):
    """Fetches option chain data for a valid symbol."""
    try:
        stock = yf.Ticker(symbol)
        # Get all expiration dates for options
        exp_dates = stock.options
        
        if not exp_dates:
            print(f"No option data available for {symbol}")
            return pd.DataFrame()

        # Fetch option data for each expiration date
        all_options_data = []
        for exp in exp_dates:
            print(f"Fetching options for {symbol} expiring on {exp}")
            options = stock.option_chain(exp)
            
            # Combine calls and puts with expiration date
            calls = options.calls.copy()
            calls['Type'] = 'Call'
            puts = options.puts.copy()
            puts['Type'] = 'Put'
            
            options_data = pd.concat([calls, puts])
            options_data['Expiration'] = exp
            options_data['Symbol'] = symbol
            
            all_options_data.append(options_data)
        
        return pd.concat(all_options_data)
    
    except Exception as e:
        print(f"Error fetching option data for {symbol}: {e}")
        return pd.DataFrame()

def main():
    # Read the stock symbols from the file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    symbols_file_path = os.path.join(script_dir, 'symbols.txt')
    symbols = read_symbols_from_file(symbols_file_path)

    if not symbols:
        print("No symbols found in file.")
        return

    all_option_data = []

    # Fetch option data for each symbol
    for symbol in symbols:
        print(f"Fetching option data for: {symbol}")
        option_data = fetch_option_data(symbol)
        if not option_data.empty:
            all_option_data.append(option_data)
        else:
            print(f"No valid option data for {symbol}")

    if all_option_data:
        # Concatenate all option data into one DataFrame
        result_df = pd.concat(all_option_data, ignore_index=True)
        result_df = normalize_option_columns(result_df)
        result_df['snapshot_ts'] = current_snapshot_ts()

        seed_path = os.path.join(script_dir, '..', 'seeds', 'option_history.csv')
        added_rows = append_seed_rows(
            seed_path,
            result_df,
            sort_columns=['Symbol', 'Expiration', 'contractSymbol'],
        )
        print(
            f"Option snapshot {result_df['snapshot_ts'].iat[0]} merged into {seed_path} "
            f"({added_rows} new rows)"
        )
    else:
        print("No valid option data found to save.")

if __name__ == "__main__":
    main()
