import os
import sys


class FilenameGenerator:
    def __init__(self, day_date=None):
        self.ticker_sym = None
        self.day_date = day_date
        self.working_dir = r'C:\Dhruv\Project Data\Trading'

    def config(self, type):
        if type == 'Twitter API keys':
            filename = self.working_dir + '\\' + 'Config' + '\\' + 'twitter_config.json'
            return filename
        if type == 'TDA Key':
            filename = self.working_dir + '\\' + 'Config' + '\\' + 'TDA_key.json'
            return filename

    def store_data(self, type, ticker=None, period=None, interval=None):
        if type == 'Raw Data':
            filename = self.working_dir + '\\' + 'Raw Data' + '\\' + self.ticker_sym + '\\' + self.day_date
            os.makedirs(filename, exist_ok=True)
            return filename
        if type == 'TDA Data Dir':
            filename = self.working_dir + '\\' + 'Raw Data' + '\\' + 'TDA'
            os.makedirs(filename, exist_ok=True)
            return filename
        if type == 'Yahoo Stock Data':
            filename = self.working_dir + '\\' + 'Raw Data' + '\\' + 'Yahoo'
            os.makedirs(filename, exist_ok=True)
            return filename
        if type == 'SP500 Ticker':
            filename = self.working_dir + '\\' + 'Raw Data' + '\\' + 'SP500 Ticker List.json'
            return filename
        if type == 'Daily' or type == 'Hourly' or type == '4Hourly':
            filename = self.working_dir + '\\' + 'Raw Data' + '\\' + type
            os.makedirs(filename, exist_ok=True)
            return filename

    def temp_data(self, file_type=None, data_type=None):
        temp_dir = self.working_dir + '\\' + 'Temp Data'
        os.makedirs(temp_dir, exist_ok=True)
        if file_type == 'Pickle':
            filename = temp_dir + '\\' + data_type + '.pickle'
            return filename


