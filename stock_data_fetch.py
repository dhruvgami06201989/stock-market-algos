import os
import sys
import requests
import datetime
import json
import pandas as pd
from file_name_generator import FilenameGenerator
import yfinance as yf
from concurrent import futures
import pickle
import bs4 as bs


class TDA:
    def __init__(self):
        self.base_url = r'https://api.tdameritrade.com/v1/marketdata'
        self.file_name_generator = FilenameGenerator()

    def get_price_data(self, ticker, period_type, freq_type, freq, period=None, start_datetime=None, end_datetime=None):
        ticker = ticker.upper()
        url_final = self.base_url + '/' + ticker + '/' + 'pricehistory'
        tda_key_file = self.file_name_generator.config('TDA Key')
        tda_key_dict = {}
        with open(tda_key_file) as f:
            tda_key_dict.update(json.load(f))
        tda_key = tda_key_dict['Key']
        if (start_datetime is not None) and (end_datetime is not None) and (period is None):
            end_date_epoch = round(end_datetime.timestamp() * 1000)
            start_date_epoch = round(start_datetime.timestamp() * 1000)
            payload = {'apikey': tda_key,
                       'frequency': str(freq),
                       'frequencyType': freq_type,
                       'endDate': end_date_epoch,
                       'startDate': start_date_epoch,
                       'needExtendedHoursData': 'true'
                       }
        elif (start_datetime is None) and (end_datetime is None) and (period is not None):
            payload = {'apikey': tda_key,
                       'period': str(period),
                       'periodType': period_type,
                       'frequency': str(freq),
                       'frequencyType': freq_type,
                       'needExtendedHoursData': 'true'
                       }
        else:
            sys.exit(">> Specify period or start date/end date")
        content = requests.get(url_final, params=payload)
        data = content.json()
        return data

    def get_data_for_analysis(self, ticker, type, period_type, freq_type, freq, period=None, start_datetime=None,
                              end_datetime=None):
        tda_ticker_data_path = self.file_name_generator.store_data('TDA Data Dir')
        tda_ticker_filename = (tda_ticker_data_path + '\\' + ticker + '_' + type + '_' +
                               datetime.datetime.today().date().strftime("%Y-%m-%d") + '.json')
        update_flag = 0
        if os.path.isfile(tda_ticker_filename):
            file_latest_date = datetime.datetime.fromtimestamp(os.path.getmtime(tda_ticker_filename)).date()
            if datetime.datetime.today().date() > file_latest_date:
                update_flag = 1
        else:
            update_flag = 1

        if update_flag == 1:
            print(">> Getting data from TDA API...")
            data = self.get_price_data(ticker, period_type, freq_type, freq, period)
            with open(tda_ticker_filename, 'w') as tda_resp:
                json.dump(data, tda_resp)
        else:
            print(">> Local stored json file found for " + ticker)
            with open(tda_ticker_filename, 'r') as tda_resp:
                data = json.load(tda_resp)

        return data

    def get_interval_data(self, ticker, frequency):
        ticker = ticker.upper()
        stored_data_dir = self.file_name_generator.store_data('TDA Data Dir')
        stored_data_filename = stored_data_dir + '\\' + ticker + '_' + frequency + 'min_interval_all.json'
        update_flag = 0
        store_new_flag = 0
        if os.path.isfile(stored_data_filename):
            latest_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(stored_data_filename))
            if latest_datetime.date() < datetime.datetime.today().date():       # Might have to change this later for RT
                update_flag = 1
        else:
            update_flag = 1
            store_new_flag = 1
            print(">> Store New Interval Data...")
        if update_flag == 1:
            period_type = 'day'
            frequency_type = 'minute'
            if store_new_flag == 0:
                print(">> Stored data found. Update with new data")
                with open(stored_data_filename, 'r') as tda_resp:
                    candle_data_all = json.load(tda_resp)
                latest_datetime_epoch = max([x['datetime'] for x in candle_data_all])
                latest_datetime = datetime.datetime.fromtimestamp(latest_datetime_epoch/1000).replace(second=0,
                                                                                                      microsecond=0)
                start_datetime = latest_datetime + datetime.timedelta(minutes=5)
                end_datetime = datetime.datetime.today().replace(second=0, microsecond=0)
                data = self.get_price_data(ticker, period_type, frequency_type, frequency, None, start_datetime,
                                           end_datetime)
                candle_data_ls = data['candles']
                candle_data_all.append(candle_data_ls)

            else:
                # end_date = datetime.datetime.today().date()
                end_datetime = datetime.datetime.today().replace(second=0, microsecond=0, minute=0)
                total_days = 5000
                day_interval = 10
                x = 0
                candle_data_all = []
                while x <= total_days:
                    print(str(x))
                    start_datetime = end_datetime - datetime.timedelta(days=day_interval)
                    data = self.get_price_data(ticker, period_type, frequency_type, frequency, None, start_datetime,
                                               end_datetime)
                    candle_data_ls = data['candles']
                    if not candle_data_ls:
                        print("Max days = " + str(x))
                        break
                    if x == 0:
                        candle_data_all = candle_data_ls.copy()
                    else:
                        candle_data_all = candle_data_all + candle_data_ls
                    end_datetime = start_datetime - datetime.timedelta(minutes=5)
                    x += 10

            with open(stored_data_filename, 'w') as tda_resp:
                json.dump(candle_data_all, tda_resp)

        else:
            print(">> Stored data found. No need to store new data")
            with open(stored_data_filename, 'r') as tda_resp:
                candle_data_all = json.load(tda_resp)

        return candle_data_all


class YFinanceData:
    def __init__(self, ticker_list, file_name_generator):
        self.ticker_list = ticker_list
        self.file_name_generator = file_name_generator

    def get_yf_stock_data(self, start_date=None, end_date=None, period=None, interval=None):
        if start_date and end_date:
            data = yf.download(tickers=self.ticker_list, start=start_date, end=end_date, auto_adjust=True, prepost=True)
        elif period and interval:
            data = yf.download(tickers=self.ticker_list, period=period, interval=interval, auto_adjust=True,
                               prepost=True)
        else:
            sys.exit(">> Yahoo Fetch Error: Please specify correct parameters to fetch price data!")
        return data


def save_sp500_ticker_ls(file_name_generator):
    update_flag = 0
    sp500_ticker_filename = file_name_generator.store_data('SP500 Ticker')
    if os.path.isfile(sp500_ticker_filename):
        file_latest_date = datetime.datetime.fromtimestamp(os.path.getmtime(sp500_ticker_filename)).date()
        if datetime.datetime.today().date() > file_latest_date + datetime.timedelta(days=15):
            update_flag = 1
    else:
        update_flag = 1

    if update_flag == 1:
        print(">> Updating S&P 500 ticker list...")
        resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, "lxml")
        table = soup.find('table', {'class': "wikitable sortable"})
        sp500_tickers = []
        for row in table.findAll('tr')[1:]:
            sp500_ticker = row.findAll('td')[0].text.rstrip()
            sp500_tickers.append(sp500_ticker)
        with open(sp500_ticker_filename, 'w') as sp500:
            json.dump(sp500_tickers, sp500)
    else:
        print("No need to update S&P 500 ticker list.")
        with open(sp500_ticker_filename, 'r') as sp500:
            sp500_tickers = json.load(sp500)

    return sp500_tickers


def fetch_stock_data(stock_list_name, type, file_name_generator, temp_file_type='Other'):
    stored_data_dir = file_name_generator.store_data(type)
    update_flag = 0
    daily_price_df_all = pd.DataFrame()
    # daily_price_df_all_long = pd.DataFrame()
    for ticker in stock_list_name:
        if os.path.exists(stored_data_dir + '\\' + ticker + '_daily.csv'):
            latest_modified_date = datetime.datetime.fromtimestamp(os.path.getmtime(stored_data_dir + '\\' + ticker +
                                                                                    '_daily.csv')).date()
            if datetime.datetime.today().date() < latest_modified_date:     # < for TESTING, > for PRODUCTION!!!!
                print("Updating stored data for ticker: " + ticker)
                update_flag = 1
                start_date = latest_modified_date + datetime.timedelta(days=1)
                end_date = datetime.datetime.today().date()
            else:
                print("Returning stored data for ticker: " + ticker)
                ticker_data = pd.read_csv(stored_data_dir + '\\' + ticker + '_daily.csv', index_col='Date')
                daily_price_df_all = daily_price_df_all.join(ticker_data, how='outer')
                # daily_price_df_all_long = daily_price_df_all_long.append(ticker_data, sort=False)
                # daily_price_df_all_long.loc[:, 'Ticker'] = ticker
                continue
        else:
            print("Downloading Ticker: " + ticker)
            start_date = datetime.datetime(2001, 1, 1)
            end_date = datetime.datetime.today().date()

        new_ticker_data = yf.download(tickers=ticker, start=start_date, end=end_date, auto_adjust=True, prepost=True)
        new_ticker_data = new_ticker_data.rename(columns={'Open': 'Open-' + ticker, 'High': 'High-' + ticker,
                                                          'Low': 'Low-' + ticker, 'Close': 'Close-' + ticker,
                                                          'Adj Close': 'Adj Close-' + ticker, 'Volume': 'Volume-' +
                                                                                                        ticker})

        if update_flag == 1:
            stored_ticker_data = pd.read_csv(stored_data_dir + '\\' + ticker + '_daily.csv', index_col='Date')
            new_ticker_data = stored_ticker_data.append(new_ticker_data)

        new_ticker_data.to_csv(stored_data_dir + '\\' + ticker + '_daily.csv')

        daily_price_df_all = daily_price_df_all.join(new_ticker_data, how='outer')

        if temp_file_type == 'SP500':
            with open(file_name_generator.temp_data('Pickle', 'SP500_Historical_Price'), 'wb') as pickle_file:
                pickle.dump(daily_price_df_all, pickle_file)
        elif temp_file_type == 'Other':
            with open(file_name_generator.temp_data('Pickle', 'Other'), 'wb') as pickle_file:
                pickle.dump(daily_price_df_all, pickle_file)

        # daily_price_df_all_long = daily_price_df_all_long.append(new_ticker_data, sort=False)
        # daily_price_df_all_long.loc[:, 'Ticker'] = ticker
    return daily_price_df_all





if __name__ == '__main__':
    fng = FilenameGenerator()
    tda_obj = TDA()
    prd_typ = 'day'
    prd = 10
    freq_typ = 'minute'
    freq = 1
    # tda_data = tda_obj.get_price_data('SPY', 'daily', prd_typ, freq_typ, freq, prd)
    tda_obj.get_interval_data('SPY', '30')

    # ticker_ls = ['AAPL', 'MSFT', 'SPY']
    # fetch_stock_data(ticker_ls, 'Daily', fng)
    print('debug')
