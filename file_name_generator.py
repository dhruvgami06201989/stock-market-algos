import os
import sys


class FilenameGenerator:
    def __init__(self, ticker, day_date):
        self.ticker_sym = ticker
        self.day_date = day_date

    def store_data(self, filename):
        if filename == 'Raw Data':
            os.makedirs(r'C:\Trading\Raw Data' + '\\' + self.ticker_sym + '\\' + self.day_date)
