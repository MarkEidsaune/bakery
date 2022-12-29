import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.common.by import By
import time
import glob
import os
from cassandra.cluster import Cluster
from tqdm import tqdm

def main():
    
    # Navigate to nasdaq website and download stock table
    options=Options()
    options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
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
    df['IPO Year'] = df['IPO Year'].astype('Int64').fillna(0)
    df['Country'] = df['Country'].fillna('NaN')
    df['Sector'] = df['Sector'].fillna('NaN')
    df['Industry'] = df['Industry'].fillna('NaN')
       
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
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
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
            row['Industry']
        )
        cass_session.execute(
            f'''
            INSERT INTO 
                bakery.symbols (symbol, name, last_sale, net_change, 
                percent_change, market_cap, country, ipo_year, volume, 
                sector, industry)
            VALUES {values}
            '''
        )
    
    # Write compressed csv to storage
    df.to_csv(
        '/media/nvme2/bakery/nasdaq/{}.gz'.format(file_path[37:]),
        compression='gzip'
    )

if __name__ == '__main__':
    main()