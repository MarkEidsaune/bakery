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
            print(f"Request error for symbol {symbol}")
            r_dict = None
        except KeyError as ke:
            print(f"Key error for symbol {symbol}")
            r_dict = None
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
            print(f"Request error for symbol {symbol} and year-month {year}-{month:02}\n{re}")
            r_dict = None
        except KeyError as ke:
            print(f"Key error for symbol {symbol} and year-month {year}-{month:02}\n{ke}\nAvailable keys: {r.json().keys()}, likely caused by rate limiting.")
            r_dict = None
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
        if df.shape[0] > 0: # Truncate collection
            coll = self.client["bakery"]["alpha_active_list"]
            coll.delete_many({})
        result = self.load_dataframe(df, "alpha_active_list")
        return result
    
    def get_active_list_symbols(self):
        """
        
        """
        coll = self.client["bakery"]["alpha_active_list"]
        symbols = [s["symbol"] for s in list(coll.find({}, {"symbol": 1, "_id": 0}))]
        return symbols
    
    def etl_daily(self, symbols):
        """ 
        
        """
        missed_symbols = []
        total_requests = 0
        t0 = time.time()
        t = tqdm(symbols, position=0, desc="Symbols", leave=False)
        for symbol in t:
            r = self.extract_daily(symbol)
            if r:
                df = self.transform_daily(symbol, r)
                result = self.load_dataframe(df, "alpha_daily")
            else:
                missed_symbols.append(symbol)
            total_requests += 1
            total_seconds = time.time() - t0
            requests_per_minute = total_requests / (total_seconds / 60)
            t.set_description(f"Symbols (req / s: {requests_per_minute:.2f})")
            t.refresh()
            while (requests_per_minute > self.limit - 2):
                time.sleep(1)
                total_seconds = time.time() - t0
                requests_per_minute = total_requests / (total_seconds / 60)
        return missed_symbols
    
    
    def etl_hourly(self, symbols, years, months):
        """
        
        """
        missed_symbols = []
        total_requests = 0
        t0 = time.time()
        for symbol in tqdm(symbols, position=0, desc="Symbols", leave=False):
            for year in tqdm(years, position=1, desc="Years", leave=False):
                t3 = tqdm(months, position=2, desc="Months", leave=False)
                for month in t3:
                    r = self.extract_hourly(symbol, year, month)
                    if r:
                        df = self.transform_hourly(symbol, r)
                        _ = self.load_dataframe(df, "alpha_hourly")
                    else:
                        missed_symbols.append(symbol)
                    total_requests += 1
                    total_seconds = time.time() - t0
                    requests_per_minute = total_requests / (total_seconds / 60)
                    t3.set_description(f"Months (req / s: {requests_per_minute:.2f})")
                    t3.refresh()
                    while (requests_per_minute > self.limit - 2):
                        time.sleep(1)
                        total_seconds = time.time() - t0
                        requests_per_minute = total_requests / (total_seconds / 60)
        return missed_symbols