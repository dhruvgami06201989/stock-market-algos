import sys
import datetime
import pickle
import numpy as np
import pandas as pd
from file_name_generator import FilenameGenerator
from stock_data_fetch import fetch_stock_data, save_sp500_ticker_ls


# class TransformData:
#     def __init__(self, file_name_generator):
#         self.file_name_generator = file_name_generator


# To get specific dataframe from ticker list
def get_reqd_price_df(price_type, ticker_ls, file_name_generator):
    if price_type in ['Open', 'High', 'Low', 'Close']:
        columns = []
        for ticker in ticker_ls:
            close_col = price_type + '-' + ticker
            columns.append(close_col)
        data_all_df = fetch_stock_data(ticker_ls, 'Daily', file_name_generator)
        reqd_price_df = data_all_df[columns]
        return reqd_price_df
    else:
        sys.exit("Incorrect Price type in get_reqd_price_df")


def get_strong_pairs(file_name_generator, ticker_ls=None):
    if not ticker_ls:
        ticker_ls = save_sp500_ticker_ls(file_name_generator)
    # reqd_price_df = get_reqd_price_df(ticker_ls, file_name_generator)
    # --------------------- Temp code ---------------------------
    # with open(r'C:\Dhruv\Project Data\Trading\Temp Data\closing_price_df_all.pickle', 'wb') as close_price_pickle:
    #     pickle.dump(reqd_price_df, close_price_pickle)
    with open(r'C:\Dhruv\Project Data\Trading\Temp Data\closing_price_df_all.pickle', 'rb') as close_price_pickle:
        reqd_price_df = pickle.load(close_price_pickle)
    # -----------------------------------------------------------
    reqd_returns_df = reqd_price_df.pct_change()
    return_corr_df = reqd_returns_df.corr()
    corr_pairs = return_corr_df.unstack().sort_values(kind="quicksort")
    # price_corr_df = reqd_price_df.corr()
    # corr_pairs = return_corr_df.unstack().sort_values(kind="quicksort")
    strong_pairs = corr_pairs[abs(corr_pairs) > 0.7]
    strong_pairs_df = strong_pairs.reset_index().rename(columns={0: 'Correlation'})
    strong_pairs_df = strong_pairs_df[strong_pairs_df['Correlation'] != 1]
    strong_pairs_df = strong_pairs_df[strong_pairs_df['level_0'].astype(str) != strong_pairs_df['level_1'].astype(str)]

    # filtering out lower/upper triangular duplicates
    strong_pairs_df['ordered-cols'] = strong_pairs_df.apply(lambda x: '-'.join(sorted([x['level_0'], x['level_1']])),
                                                            axis=1)
    strong_pairs_df = strong_pairs_df.drop_duplicates(['ordered-cols'])
    strong_pairs_df.drop(['ordered-cols'], axis=1, inplace=True)
    return strong_pairs_df


def get_symb_pairs(symb_list):
    """symbList is a list of ETF symbols. This function takes in a list of symbols and
    returns a list of unique pairs of symbols"""

    symb_pairs = []
    # iterate through the list and create all possible combinations of
    # ticker pairs - append the pairs to the "symb_pairs" list
    i = 0
    while i < len(symb_list) - 1:
        j = i + 1
        while j < len(symb_list):
            symb_pairs.append([symb_list[i], symb_list[j]])
            j += 1
            i += 1
    # iterate through the newly created list of pairs and remove any pairs #made up of two identical tickers
    for i in symb_pairs:
        if i[0] == i[1]:
            symb_pairs.remove(i)
    # create a new empty list to store only unique pairs
    symb_pairs2 = []
    # iterate through the original list and append only unique pairs to the
    # new list
    for i in symb_pairs:
        if i not in symb_pairs2:
            symb_pairs2.append(i)
    return symb_pairs2


def get_correlations(file_name_generator, ticker_ls):
    reqd_price_df = get_reqd_price_df('Close', ticker_ls, file_name_generator)
    price_corr_df = reqd_price_df.corr()
    reqd_returns_df = reqd_price_df.pct_change()
    return_corr_df = reqd_returns_df.corr()

    return price_corr_df, return_corr_df
