from binance.client import Client
from datetime import datetime, timezone
from datetime_utils import parse_unix_timestamp_in_ms_to_datetime as parse_unix_time
import pandas as pd

def create_client(api_key, api_secret):
    # Initialize Binance Client
    return Client(api_key, api_secret)

def get_ticker(client, symbol):
    return client.get_symbol_ticker(symbol=symbol)

class HistoricData:
    def __init__(self, client, symbol, interval, start_time, end_time):
        self.client = client
        self.symbol = symbol
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

    def retrieve_data_as_list_of_dicts(self):
        data = self._get_historic_data(self.symbol, self.interval, self.start_time, self.end_time)

        return [
            {
                "open_time": parse_unix_time(entry[0]),
                "open": float(entry[1]),
                "high": float(entry[2]),
                "low": float(entry[3]),
                "close": float(entry[4]),
                "volume": float(entry[5]),
                "close_time": parse_unix_time(entry[6]),
                "quote_asset_volume": float(entry[7]),
                "number_of_trades": int(entry[8]),
                "taker_buy_base_asset_volume": float(entry[9]),
                "taker_buy_quote_asset_volume": float(entry[10])
            }
            for entry in data
        ]

    def retrieve_data_as_df(self):
        data = self._get_historic_data(self.symbol, self.interval, self.start_time, self.end_time)
        df = pd.DataFrame(data,
                            columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume',
                                     'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'])

        df['Open Time'] = df['Open Time'].apply(lambda x: parse_unix_time(x))
        df['Close Time'] = df['Close Time'].apply(lambda x: parse_unix_time(x))
        df['Close Time'] = pd.to_datetime(df['Close Time'], utc=True)

        return df

    def _get_historic_data(self, symbol, interval, start_time, end_time):
        '''
        Data Format
        [
          [
            1499040000000,      // Kline open time
            "0.01634790",       // Open price
            "0.80000000",       // High price
            "0.01575800",       // Low price
            "0.01577100",       // Close price
            "148976.11427815",  // Volume
            1499644799999,      // Kline Close time
            "2434.19055334",    // Quote asset volume
            308,                // Number of trades
            "1756.87402397",    // Taker buy base asset volume
            "28.46694368",      // Taker buy quote asset volume
            "0"                 // Unused field, ignore.
          ]
        ]
        '''

        return self.client.get_historical_klines(symbol, interval, start_time, end_time, limit=1000)

if __name__ == "__main__":
    try:
        import config_local as config
    except ImportError:
        raise ImportError("Local settings file (config_local.py) not found. Please create one.")

    # Create a config_local.py file and specify values for your api key and secret, BINANCE_API_KEY, and BINANCE_API_SECRET
    api_key = config.BINANCE_API_KEY
    api_secret = config.BINANCE_API_SECRET

    client = create_client(api_key, api_secret)

    all_data = pd.DataFrame()
    for year in range(2017, 2025):
        # first date with btcusdt data is 20170817
        start_time = datetime(year, 8, 17).replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        end_time = datetime(year+1, 8, 17).replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        historic_data = HistoricData(client, "BTCUSDT", "1d", start_time, end_time)
        data = historic_data.retrieve_data_as_df()
        all_data = pd.concat([all_data, data], ignore_index=True)

    all_data.to_csv("../data/btcusdt_data.csv")
