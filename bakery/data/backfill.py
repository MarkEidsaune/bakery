import argparse
import os
import json
from pymongo import MongoClient
import pandas as pd
from alpha import Alpha

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--refresh-active-list", action="store_true", help="Default = false, include this flag to refresh the active list prior to backfilling")
parser.add_argument("--resolution", default="daily", type=str, help="Temporal resolution of historical equity data")
args = parser.parse_args()
print(vars(args))

# Init mongo client and alpha class
path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")
alpha = Alpha(path)

# ETL active list if requested
if args.refresh_active_list:
    print("Refreshing active list...")
    result = alpha.etl_active_list()

# Get list of active symbols
symbols = alpha.get_active_list_symbols()

# ETL daily candles
if args.resolution == "daily":
    print(f"Backfilling {len(symbols)} symbols at day resolution...")
    missed_symbols = alpha.etl_daily(symbols)
elif args.resolution == "hourly":
    print(f"Backfilling {len(symbols)} symbols at hour resolution...")
    missed_symbols = alpha.etl_hourly()
else:
    print(f"{args.resolution} resolution not supported. Select from ('daily', 'hourly')")
