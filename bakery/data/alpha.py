import json
from pymongo import MongoClient
import time
from tqdm import tqdm
import requests
import io
import pandas as pd
from datetime import datetime

class Alpha():
    """ 
    
    """
    def __init__(self, path):
        with open(path, "rb") as f:
            config = json.loads(f.read().decode())
            self.api_key = config["alpha"]["api_key"]
            self.limit = config["alpha"]["limit"]
            self.endpoint = config["alpha"]["endpoint"]
            self.client = MongoClient(config["mongo"]["conn_str"])

    def extract_active_list(self):
        """ 
        
        """
        params = {
            "function": "LISTING_STATUS",
            "apikey": self.api_key
        }
        return requests.get(self.endpoint, params)
    
    def extract_daily(self, symbol):
        """ 
        
        """
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "outputsize": "full",
            "datatype": "json",
            "apikey": self.api_key
        }
        try:
            r = requests.get(self.endpoint, params)
            r_dict = r.json()["Time Series (Daily)"]
        except requests.exceptions.RequestException as re:
            print(f"Request error for symbol {symbol}\n{re}")
        except KeyError as ke:
            print(f"Key error for symbol {symbol}\n{ke}\nAvailable keys: {r.json().keys()}, likely caused by rate limiting.")
        return r_dict
    
    def extract_hourly(self, symbol, year, month):
        """ 
        
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "60min",
            "extended_hours": "true",
            "outputsize": "full",
            "month": f"{year}-{month:02}",
            "apikey": self.api_key
        }
        try:
            r = requests.get(self.endpoint, params)
            r_dict = r.json()["Time Series (60min)"]
        except requests.exceptions.RequestException as re:
            print(f"Request error for symbol {symbol} and year-month {year_month}\n{re}")
        except KeyError as ke:
            print(f"Key error for symbol {symbol} and year-month {year_month}\n{ke}\nAvailable keys: {r.json().keys()}, likely caused by rate limiting.")
        return r_dict
    
    @staticmethod
    def transform_active_list(response):
        """
        
        """
        data = io.BytesIO(response.content)
        df = pd.read_csv(data)
        df.drop(columns="delistingDate", inplace=True)
        df.rename(columns={"assetType": "asset_type", "ipoDate": "ipo_dt"}, inplace=True)
        df["ipo_dt"] = pd.to_datetime(df["ipo_dt"])
        df["refresh_dttm"]= datetime.today()
        return df
    
    @staticmethod
    def transform_daily(symbol, response):
        """ 
        
        """
        df = pd.DataFrame(response).T
        df["dttm"] = df.index
        df.reset_index(drop=True, inplace=True)
        df["symbol"] = symbol
        df.rename(columns={"1. open": "open", 
                           "2. high": "high", 
                           "3. low": "low", 
                           "4. close": "close", 
                           "5. adjusted close": "adj_close", 
                           "6. volume": "volume",
                           "7. dividend amount": "div_amt",
                           "8. split coefficient": "split_coef"},
                  inplace=True)
        float_cols = ["open", "high", "low", "close", "adj_close", "volume", "div_amt", "split_coef"]
        df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
        df["dttm"] = pd.to_datetime(df["dttm"])
        return df
    
    @staticmethod
    def transform_hourly(symbol, response):
        """ 
        
        """
        df = pd.DataFrame(response).T
        df["dttm"] = df.index
        df.reset_index(drop=True, inplace=True)
        df["symbol"] = symbol
        df.rename(columns={"1. open": "open", 
                           "2. high": "high", 
                           "3. low": "low", 
                           "4. close": "close", 
                           "5. volume": "volume"},
                  inplace=True)
        float_cols = ["open", "high", "low", "close", "volume"]
        df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
        df["dttm"] = pd.to_datetime(df["dttm"])
        return df

    def load_dataframe(self, df, collection):
        """ 
        
        """
        rows = df.to_dict("records")
        coll = self.client["bakery"][collection]
        result = coll.insert_many(rows)
        return result
    
    def etl_active_list(self):
        """ 
        
        """
        r = self.extract_active_list()
        df = self.transform_active_list(r)
        result = self.load_dataframe(df, "alpha_active_list")
        return result
    
    def etl_daily(self, symbols):
        """ 
        
        """
        total_requests = 0
        t0 = time.time()
        t = tqdm(symbols, position=0, desc="Symbols", leave=True)
        for symbol in t:
            result = self.load_dataframe(
                self.transform_daily(
                    symbol,
                    self.extract_daily(symbol)
                    ),
                "alpha_daily"
                )
            total_requests += 1
            total_seconds = time.time() - t0
            requests_per_minute = total_requests / (total_seconds / 60)
            t.set_description(f"Symbols (req / s: {requests_per_minute:.2f})")
            t.refresh()
            while (requests_per_minute > self.limit):
                time.sleep(1)
                total_seconds = time.time() - t0
                requests_per_minute = total_requests / (total_seconds / 60)
    
    def etl_hourly(self, symbols, years, months):
        """
        
        """
        total_requests = 0
        t0 = time.time()
        for symbol in tqdm(symbols, position=0, desc="Symbols", leave=True):
            for year in tqdm(years, position=1, desc="Years", leave=False):
                t3 = tqdm(months, position=2, desc="Months", leave=False)
                for month in t3:
                    result = self.load_dataframe(
                        self.transform_hourly(
                            symbol, 
                            self.extract_hourly(symbol, year, month)
                            ),
                        "alpha_hourly"
                        )
                    total_requests += 1
                    total_seconds = time.time() - t0
                    requests_per_minute = total_requests / (total_seconds / 60)
                    t3.set_description(f"Months (req / s: {requests_per_minute:.2f})")
                    t3.refresh()
                    while (requests_per_minute > self.limit):
                        time.sleep(1)
                        total_seconds = time.time() - t0
                        requests_per_minute = total_requests / (total_seconds / 60)