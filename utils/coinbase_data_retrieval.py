import coinbase.wallet.client as coinbase_client
from datetime import datetime, timezone, timedelta
from datetime_utils import parse_unix_timestamp_in_ms_to_datetime as parse_unix_time
import pandas as pd
import requests

def get_historic_prices_as_df(symbol, start_time, end_time, granularity):
    # Coinbase Pro API endpoint
    url = "https://api.exchange.coinbase.com/products/{}/candles".format(symbol)

    # Query parameters
    params = {
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
        "granularity": granularity,  # 1 hour
    }

    # Make the request
    response = requests.get(url, params=params)

    # Parse the data
    if response.status_code == 200:
        data = response.json()
        # Each entry [timestamp, low, high, open, close, volume]
        df = pd.DataFrame(data, columns=['Time', 'Low', 'High', 'Open', 'Close', 'Volume'])
        df['Time'] = df['Time'].apply(lambda x: parse_unix_time(x*1000.0))
        return df
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def auth_client():
    try:
        import config_local as config
    except ImportError:
        raise ImportError("Local settings file (config_local.py) not found. Please create one.")

    # Create a config_local.py file and specify values for your api key and secret, BINANCE_API_KEY, and BINANCE_API_SECRET
    api_key = config.COINBASE_KEY
    api_secret = config.COINBASE_SECRET

    return coinbase_client.Client(api_key, api_secret)

if __name__ == "__main__":
    start_date = datetime(2015, 1,1).replace(tzinfo=timezone.utc)
    end_date = (datetime.now() - timedelta(days=1)).replace(tzinfo=timezone.utc)
    step_size = 99

    date = start_date
    all_prices = pd.DataFrame()
    while date < end_date:
        start_time = date
        end_time = min(end_date, (date + timedelta(days=step_size-1)))
        prices = get_historic_prices_as_df('BTC-USD', start_time, end_time, 86400) # granularity = 1-day
        prices['Time'] = prices['Time'].apply(lambda x: x.date())

        all_prices = pd.concat([all_prices, prices], ignore_index=True)
        date += timedelta(days=step_size)

    all_prices.sort_values(by='Time', inplace=True)
    all_prices.to_csv("../data/coinbase_prices.csv")

    print(all_prices)