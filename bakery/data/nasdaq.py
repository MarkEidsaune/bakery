import pandas as pd
from datetime import datetime
import time
import glob
import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.common.by import By
from cassandra.cluster import Cluster

def update_symbols():
    # Navigate to nasdaq website and download stock table
    options=Options()
    options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
    options.headless = True
    driver = webdriver.Firefox(options=options)
    driver.get('https://www.nasdaq.com/market-activity/stocks/screener')
    btn = driver.find_element(
        By.XPATH,
        value="//button[@class='nasdaq-screener__form-button--download ns-download-1']"
    )
    btn.click()
    time.sleep(5)
    
    # Close webdriver
    driver.close()
    
    # Read csv from Downloads
    file_list = glob.glob('/home/mark/Downloads/nasdaq_screener_*.csv')
    file_path = file_list[0]
    df = pd.read_csv(file_path)
    
    # Clean up/coerce datatypes
    df['Symbol'] = df['Symbol'].fillna('NA').str.strip()
    df['Name'] = df['Name'].str.replace('\'', '', regex=False)
    df['Last Sale'] = df['Last Sale'].str.replace('$', '', regex=False).astype(float)
    df['% Change'] = df['% Change'].str.replace('%', '', regex=False).astype(float)
    df['Market Cap'] = df['Market Cap'].astype('Int64').fillna(0)
    df['IPO Year'] = df['IPO Year'].astype('Int32').fillna(0)
    df['Country'] = df['Country'].fillna('NaN')
    df['Sector'] = df['Sector'].fillna('NaN')
    df['Industry'] = df['Industry'].fillna('NaN')

    # Add updated datetime column
    df['Updated'] = datetime.today().strftime('%Y-%m-%d')

    print(df[['Net Change', 'Market Cap']].head(11))
       
    # Delete .csv from Downloads folder
    os.remove(file_path)
    
    # Overwrite Cassandra table
    cass_cluster = Cluster()
    cass_session = cass_cluster.connect()
    cass_session.execute(
        '''
        truncate bakery.symbols;
        '''
    )
    for _, row in df.iterrows():
        values = (
            row['Symbol'],
            row['Name'],
            row['Last Sale'],
            row['Net Change'],
            row['% Change'],
            row['Market Cap'],
            row['Country'],
            row['IPO Year'],
            row['Volume'],
            row['Sector'],
            row['Industry'],
            row['Updated']
        )
        cass_session.execute(
            f'''
            INSERT INTO 
                bakery.symbols (symbol, name, last_sale, net_change, 
                percent_change, market_cap, country, ipo_year, volume, 
                sector, industry, updated)
            VALUES {values}
            '''
        )