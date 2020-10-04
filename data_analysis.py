import os
import requests
import datetime
import json
import numpy as np
import pandas as pd
import pickle
import seaborn as sns
import matplotlib.pyplot as plt
from file_name_generator import FilenameGenerator
from stock_data_fetch import TDA, fetch_stock_data, save_sp500_ticker_ls
from transform_data import get_correlations
import statsmodels.tsa.stattools as ts


def get_tda_data(ticker):
    tda_obj = TDA()
    ticker = ticker.upper()
    period = 20
    period_type = 'year'
    frequency = 1
    frequency_type = 'daily'
    data = tda_obj.get_data_for_analysis(ticker, 'daily', period_type, frequency_type, frequency, period)
    select_price = 'close'

    prices_close = [data['candles'][i]['close'] for i in range(0, len(data['candles']))]
    prices_open = [data['candles'][i]['open'] for i in range(0, len(data['candles']))]
    prices_high = [data['candles'][i]['high'] for i in range(0, len(data['candles']))]
    prices_low = [data['candles'][i]['low'] for i in range(0, len(data['candles']))]
    volume = [data['candles'][i]['volume'] for i in range(0, len(data['candles']))]
    epoch_date_time_ls = [data['candles'][i]['datetime'] for i in range(0, len(data['candles']))]
    date_time_ls = [datetime.datetime.fromtimestamp(i/1000) for i in epoch_date_time_ls]
    prices_df = pd.DataFrame(data=[prices_open, prices_high, prices_low, prices_close, volume], index=date_time_ls,
                             columns=['Open-' + data['symbol'], 'High-' + data['symbol'], 'Low-' + data['symbol'],
                                      'Close-' + data['symbol'], 'Volume-' + data['symbol']])
    return prices_df


def plot_charts(price_type, prices_df, ticker_ls):
    """To plot simple price charts by ticker"""
    for ticker in ticker_ls:
        plt.plot(prices_df[price_type + '-' + ticker], label=ticker)
    plt.ylabel('Price-' + price_type)
    plt.xlabel('Time')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.show()


def get_correlation_matrix_yf_data():
    """Plot correlation matrix of price and returns"""
    file_name_generator = FilenameGenerator()
    ticker_ls = save_sp500_ticker_ls(file_name_generator)
    price_corr_df, return_corr_df = get_correlations(file_name_generator, ticker_ls)
    price_corr = price_corr_df.values
    return_corr = return_corr_df.values
    plot_correlation_matrix(price_corr_df, price_corr)
    plot_correlation_matrix(return_corr_df, return_corr)


def plot_correlation_matrix(reqd_corr_df, reqd_corr):
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    heatmap = ax.pcolor(reqd_corr, cmap=plt.cm.RdYlGn)
    fig.colorbar(heatmap)
    ax.set_xticks(np.arange(reqd_corr.shape[0]) + 0.5, minor=False)
    ax.set_yticks(np.arange(reqd_corr.shape[1]) + 0.5, minor=False)
    ax.invert_yaxis()
    ax.xaxis.tick_top()

    column_labels = reqd_corr_df.columns
    row_labels = reqd_corr_df.index

    ax.set_xticklabels(column_labels)
    ax.set_yticklabels(row_labels)
    plt.xticks(rotation=90)
    heatmap.set_clim(-1, 1)
    plt.tight_layout()
    plt.show()


def analyze_data(ticker):
    prices_df = get_tda_data(ticker)
    # Compute the z score for each day using historical data up to that day
    prices_df['mu'] = [prices_df[ticker][:i].mean() for i in range(len(prices_df))]
    print(prices_df.head())
    prices_df = prices_df.dropna()
    z_scores = [(prices_df[ticker].iloc[i] - prices_df['mu'].iloc[i]) / np.std(prices_df[ticker].iloc[:i]) for i in
                range(len(prices_df))]

    print("z_scores between -0.5 and 0.5: " + str(len([x for x in z_scores if (abs(x) < 0.5)])))

    # Plot z_scores

    plt.plot(z_scores)
    plt.show()

    # Compute pct return on each day
    daily_ret = prices_df[ticker] / prices_df[ticker].shift(-1) - 1
    log_daily_ret = np.log(prices_df[ticker] / prices_df[ticker].shift(-1))
    weekly_ret = prices_df[ticker].resample('W').ffill().pct_change()

    # # Plot log daily returns
    # fig = plt.figure()
    # ax1 = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    # ax1.plot(log_daily_ret)
    # ax1.set_xlabel("Date")
    # ax1.set_ylabel("Log return")
    # ax1.set_title("Log daily returns data")
    # plt.show()
    #
    # # Plot daily returns
    # fig = plt.figure()
    # ax1 = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    # ax1.plot(daily_ret)
    # ax1.set_xlabel("Date")
    # ax1.set_ylabel("Percent")
    # ax1.set_title("Daily returns data")
    # plt.show()
    #
    # # Plot Weekly returns
    # fig = plt.figure()
    # ax1 = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    # ax1.plot(weekly_ret)
    # ax1.set_xlabel("Date")
    # ax1.set_ylabel("Percent")
    # ax1.set_title("Weekly returns data")
    # plt.show()
    #
    # # Plot histogram of weekly returns
    # fig = plt.figure()
    # ax1 = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    # daily_ret.plot.hist(bins=60)
    # ax1.set_xlabel("Daily returns %")
    # ax1.set_ylabel("Percent")
    # ax1.set_title("Netflix daily returns data")
    # ax1.text(-0.35, 200, "Extreme Low\nreturns")
    # ax1.text(0.25, 200, "Extreme High\nreturns")
    # plt.show()
    #
    # # Plot mean for each day using historical data up to that day and raw closing price of that day
    # plt.plot(prices_df[ticker])
    # plt.plot(prices_df['mu'])
    # plt.show()

    return prices_df, z_scores


if __name__ == "__main__":
    # ticker = 'SPY'
    # simple_mean_reversion_strat(ticker)
    fng = FilenameGenerator()
    # price_df = get_reqd_price_df(['MCHP', 'USB'], fng)
    get_correlation_matrix_yf_data()
    # get_reqd_pairs(fng)
    # price_reqd_df = get_reqd_price_df(['AEP', 'WM'], fng)
    print("Debug")
