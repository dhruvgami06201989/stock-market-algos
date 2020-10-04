import sys
import json
import requests
import datetime
import tweepy
from file_name_generator import FilenameGenerator


class GetStockTwit:
    def __init__(self):
        file_name_generator = FilenameGenerator()
        self.config_file = file_name_generator.config('Twitter API keys')
        self.st_base_url = r'https://api.stocktwits.com/api/2/streams/symbol/'
        self.st_format = '.json'

    def oauth_req(self):
        config = {}
        try:
            with open(self.config_file) as f:
                config.update(json.load(f))
        except:
            sys.exit("Authentication Failed!")

        else:
            twitter_auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
            twitter_auth.set_access_token(config['access_token'], config['access_token_secret'])
            api = tweepy.API(twitter_auth)
            return api

    def get_stocktwit_data(self, ticker):
        resp = requests.post(self.st_base_url + ticker + self.st_format)
        return resp

    def get_twitter_data(self, ticker):
        symbol = '$' + ticker
        api = self.oauth_req()
        results = api.search(q=symbol, lang='en', count=1000)
        return results


if __name__ == "__main__":
    getstocktweet = GetStockTwit()
    resp = getstocktweet.get_stocktwit_data("FB")
    result = getstocktweet.get_twitter_data("FB")


