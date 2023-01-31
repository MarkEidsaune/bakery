import requests
import json
import time
from datetime import datetime, timedelta
from selenium import webdriver
from authlib.integrations.requests_client import OAuth2Session, OAuth2Auth

class TDClient:
    '''
    
    '''
    
    def __init__(self, creds_path):
        # Set token endpoint (for authentication requests)
        self.token_endpoint = 'https://api.tdameritrade.com/v1/oauth2/token'
        
        # Get credentials and token objects
        self.credentials_path = creds_path

        with open(self.credentials_path, 'rb') as f:
            bakery_credentials = json.loads(f.read().decode())
            self.credentials = bakery_credentials['td']['creds']
            self.token = bakery_credentials['td']['tokens']
            
        # Set refresh token expired indicator
        self.is_refresh_token_expired = (datetime.fromtimestamp(self.token['expires_at']) \
            - timedelta(seconds=self.token['expires_in']) \
            + timedelta(seconds=self.token['refresh_token_expires_in'])) \
                < datetime.now()
        
        # Set access token expired indicator
        self.is_token_expired = datetime.fromtimestamp(self.token['expires_at']) \
            < datetime.now()
        
    def refresh_tokens(self):
        '''
        
        '''
        if self.is_refresh_token_expired:
            # Get a new token from the webdriver_workflow
            print('Refresh token expired. Calling get_token_from_webdriver_workflow()')
            self.token = self.get_token_from_webdriver_workflow()
            
            # Reset token expiration tags
            self.is_refresh_token_expired = False
            self.is_token_expired = False
            print('Access & refresh tokens have been successfully updated')
            
        elif self.is_token_expired:
            # Get a new token from refresh token
            print('Access token expired. Calling get_token_from_refresh_token()')
            self.token = self.get_token_from_refresh_token()
            
            # Reset token expiration tags
            self.is_refresh_token_expired = False
            self.is_token_expired = False
            print('Access token has been successfully updated')
            
    def get_auth(self):
        '''
        
        '''
        # Get OAuth2 authentication object
        return OAuth2Auth(self.token)
    
    def get_session(self):
        '''
        
        '''
        
        return OAuth2Session(
                client_id=self.credentials['api_key'] + '@AMER.OAUTHAP', 
                client_secret=self.credentials['password']
            )
    
    def get_principals(self):
        '''
        
        '''
        payload = {'fields': 'streamerSubscriptionKeys,streamerConnectionInfo,preferences'}
        return requests.get(
                'https://api.tdameritrade.com/v1/userprincipals',
                auth=OAuth2Auth(self.token),
                params=payload
            ).json()
            
    def get_token_from_webdriver_workflow(self):
        '''
        
        '''
        # Initialize client                               
        client=OAuth2Session(
            client_id=self.credentials['api_key'] + '@AMER.OAUTHAP',
            client_secret=self.credentials['password'],
            token_endpoint=self.token_endpoint,
            redirect_uri=self.credentials['redirect_uri']
        )
        # Get authorization url
        uri, _ = client.create_authorization_url(
            url='https://auth.tdameritrade.com/auth'
        )
        # Navigate webdriver to authorization url
        print('Login and allow access to recieve a token')
        driver = webdriver.Firefox()
        driver.get(uri)
        # Wait for login process to end on redirect url
        current_url = ''
        num_waits = 0
        max_waits = 3000
        wait_time = 0.1
        while not current_url.startswith(self.credentials['redirect_uri']):
            current_url = driver.current_url
            if num_waits > max_waits:
                raise RedirectTimeoutError('Timed out waiting for redirect')
            time.sleep(wait_time)
            num_waits += 1
        # Call the fetch token method
        self.token = client.fetch_token(
            authorization_response=current_url,
            access_type='offline'
        )
        # Update credential file with new token
        with open(self.credentials_path, 'r') as f:
            bakery_credentials = json.load(f)
            bakery_credentials['td']['tokens'] = self.token
        with open(self.credentials_path, 'w') as f:
            json.dump(bakery_credentials, f)
            
        return self.token
    
    def get_token_from_refresh_token(self):
        '''
        
        '''
        # Initialize client
        client = OAuth2Session(
            client_id=self.credentials['api_key'] + '@AMER.OAUTHAP',
            client_secret=self.credentials['password'],
            token_endpoint=self.token_endpoint,
            redirect_uri=self.credentials['redirect_uri']
        )
        # Call the fetch token method
        new_token = client.fetch_token(
            self.token_endpoint,
            grant_type='refresh_token',
            refresh_token=self.token['refresh_token'],
            access_type='offline'
        )
        self.token = new_token
        # Update credential file with new token
        with open(self.credentials_path, 'r') as f:
            bakery_credentials = json.load(f)
            bakery_credentials['td']['tokens'] = self.token
        with open(self.credentials_path, 'w') as f:
            json.dump(bakery_credentials, f)
        
        return self.token

    def get_prices(
            self,
            symbol,
            period_type='year',
            period=20,
            frequency_type='daily',
            frequency=1,
            end_date=None,
            start_date=None
        ):
        '''
        
        '''
        # Set request parameters
        if start_date:
            params={
                'periodType': period_type,
                'frequencyType': frequency_type,
                'frequency': frequency,
                'endDate': end_date,
                'startDate': start_date,
                'needExtendedHoursData': 'false'
            }
        else: 
            params={
                'periodType': period_type,
                'period': period,
                'frequencyType': frequency_type,
                'frequency': frequency,
                'needExtendedHoursData': 'false'
            }
        # Get new access token if expired
        if time.time() > self.token['expires_at']:
            self.refresh_tokens()
        
        # Get authentication object
        td_auth = self.get_auth()

        # Replace forward slashes with periods
        symbol = symbol.replace('/', '.')
        # Send get request
        r = requests.get(
                f'https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory',
                auth=td_auth,
                params=params
            )

        # Return response
        return r.json()

    def get_quotes(self, symbols):
        '''
        
        '''
        # Get new access token if expired
        if time.time() > self.token['expires_at']:
            self.refresh_tokens()
            td_auth = self.get_auth()

        # Convert list of symbols into a comma separated string
        symbols = [symbol.replace('/', '.') for symbol in symbols]
        symbols_str = ','.join(symbols)

        params = {
            'symbol': symbols_str
        }

        # Get new access token if expired
        if time.time() > self.token['expires_at']:
            self.refresh_tokens()
        
        # Get authentication object
        td_auth = self.get_auth()

        r = requests.get(
            'https://api.tdameritrade.com/v1/marketdata/quotes',
            auth=td_auth,
            params=params
        )

        return r.json()





