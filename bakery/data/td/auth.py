import os
import requests
import json
import time
from datetime import datetime, timedelta
import urllib
from selenium import webdriver
from authlib.integrations.requests_client import OAuth2Session, OAuth2Auth
import inspect
import asyncio
from enum import Enum
from collections import defaultdict, deque
import copy

class TDClient:
    '''
    
    '''
    
    def __init__(self):
        # Set token endpoint (for authentication requests)
        self.token_endpoint = 'https://api.tdameritrade.com/v1/oauth2/token'
        
        # Get credentials and token objects
        self.credentials_path = os.path.join(
            os.path.expanduser('~'), 'bakery/td_ameritrade/creds.json'
        )
        self.token_path = os.path.join(
            os.path.expanduser('~'), 'bakery/td_ameritrade/tokens.json'
        )
        with open(self.credentials_path, 'rb') as f:
            self.credentials = json.loads(f.read().decode())
        with open(self.token_path, 'rb') as f:
            self.token = json.loads(f.read().decode())
            
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
        # Overwrite token file
        with open(token_path, 'w') as f:
            json.dump(self.token, f)
            
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
        # Overwrite token file
        with open(self.token_path, 'w') as f:
            json.dump(self.token, f)
        
        return self.token