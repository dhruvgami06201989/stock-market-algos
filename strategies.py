import datetime
import numpy as np
import pandas as pd
from file_name_generator import FilenameGenerator
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
from transform_data import get_reqd_price_df, get_symb_pairs
from data_analysis import plot_charts
import matplotlib.pyplot as plt


def hurst(ts):
    """Returns the Hurst Exponent of the time series vector ts"""
    # Create the range of lag values
    lags = range(2, 100)

    # Calculate the array of the variances of the lagged differences
    tau = [np.sqrt(np.std(np.subtract(ts[lag:], ts[:-lag]))) for lag in lags]

    # Use a linear fit to estimate the Hurst Exponent
    poly = np.polyfit(np.log(lags), np.log(tau), 1)

    # Return the Hurst exponent from the polyfit output
    return poly[0] * 2.0


def mean_reversion_strat_by_ticker(ticker):
    prices_df = get_tda_data(ticker)
    prices_df['mu'] = [prices_df[ticker][:i].mean() for i in range(len(prices_df))]
    print(prices_df.head())
    prices_df = prices_df.dropna()
    z_scores = [(prices_df[ticker].iloc[i] - prices_df['mu'].iloc[i]) / np.std(prices_df[ticker].iloc[:i]) for i in
                range(len(prices_df))]
    # Start with no money and no positions
    money = 500
    positions = 0
    date = prices_df.index.values[0]
    for i in range(len(prices_df)-1):
        if money > 0:
            # Buy long if z-score < 1
            if z_scores[i] < -1:
                money -= prices_df[ticker].iloc[i]
                if money > 0:
                    positions += 1
                else:
                    money += prices_df[ticker].iloc[i]
            # Sell short if z-score > 1
            # elif z_scores[i] > 1:
            #     money += prices_df[ticker].iloc[i]
            #     positions -= 1
            # Clear positions if z-score btwn -0.5 and 0.5
            elif abs(z_scores[i]) < 0.5:
                money += positions * prices_df[ticker].iloc[i]
                positions = 0
            print(str(positions) + ", " + str(money))

    # close the position on the last day
    money += positions * prices_df[ticker].iloc[-1]
    positions = 0
    print("Final money after closing: $" + str(money))


def mean_reversion_pairs(ticker_pair):
    file_name_generator = FilenameGenerator()
    pair_df = get_reqd_price_df('Close', ticker_pair, file_name_generator)

    y = pair_df[[('Close-' + ticker_pair[0])]].rename(columns={('Close-' + ticker_pair[0]): 'price'})
    x = pair_df[[('Close-' + ticker_pair[1])]].rename(columns={('Close-' + ticker_pair[1]): 'price'})

    plot_charts('Close', pair_df, ticker_pair)

    # make sure DataFrames are the same length
    min_date = max(df.dropna().index[0] for df in [y, x])
    max_date = min(df.dropna().index[-1] for df in [y, x])
    y = y[(y.index >= min_date) & (y.index <= max_date)]
    x = x[(x.index >= min_date) & (x.index <= max_date)]

    # Run Ordinary Least Squares regression to find hedge ratio and then create spread series
    df1 = pd.DataFrame({'y': y['price'], 'x': x['price']})
    est = sm.OLS(df1.y, df1.x)
    est = est.fit()
    df1['hr'] = -est.params[0]
    df1['spread'] = df1.y + (df1.x * df1.hr)

    plt.plot(df1.spread)
    plt.show()

    cadf = ts.adfuller(df1.spread)
    print('Augmented Dickey Fuller test statistic = ' + str(cadf[0]))
    print('Augmented Dickey Fuller p-value = ' + str(cadf[1]))
    print('Augmented Dickey Fuller 1%, 5% and 10% test statistics = ' + str(cadf[4]))

    if abs(cadf[0]) > abs(cadf[4]['5%']):
        print("Hurst Exponent = " + str(round(hurst(df1.spread), 2)))

        # Run OLS regression on spread series and lagged version of itself
        spread_lag = df1.spread.shift(1)
        spread_lag.iloc[0] = spread_lag.iloc[1]
        spread_ret = df1.spread - spread_lag
        spread_ret.iloc[0] = spread_ret.iloc[1]
        spread_lag2 = sm.add_constant(spread_lag)

        model = sm.OLS(spread_ret, spread_lag2)
        res = model.fit()

        halflife = round(-np.log(2) / res.params[1], 0)
        if halflife <= 0:
            halflife = 1
        print('Halflife = ' + str(halflife))

        meanSpread = df1.spread.rolling(window=halflife).mean()
        stdSpread = df1.spread.rolling(window=halflife).std()

        df1['zScore'] = (df1.spread - meanSpread) / stdSpread

        df1['zScore'].plot()
        plt.show()

        ##############################################################

        entry_zscore = 2
        exit_zscore = 0

        # set up num units long
        df1['long entry'] = ((df1.zScore < - entry_zscore) & (df1.zScore.shift(1) > - entry_zscore))
        df1['long exit'] = ((df1.zScore > - exit_zscore) & (df1.zScore.shift(1) < - exit_zscore))
        df1['num units long'] = np.nan
        df1.loc[df1['long entry'], 'num units long'] = 1
        df1.loc[df1['long exit'], 'num units long'] = 0
        df1['num units long'][0] = 0
        df1['num units long'] = df1['num units long'].fillna(method='pad')
        # set up num units short df1['short entry'] = ((df1.zScore >  entry_zscore) & ( df1.zScore.shift(1) < entry_zscore))
        df1['short exit'] = ((df1.zScore < exit_zscore) & (df1.zScore.shift(1) > exit_zscore))
        df1.loc[df1['short entry'], 'num units short'] = -1
        df1.loc[df1['short exit'], 'num units short'] = 0
        df1['num units short'][0] = 0
        df1['num units short'] = df1['num units short'].fillna(method='pad')

        df1['numUnits'] = df1['num units long'] + df1['num units short']
        df1['spread pct ch'] = (df1['spread'] - df1['spread'].shift(1)) / ((df1['x'] * abs(df1['hr'])) + df1['y'])
        df1['port rets'] = df1['spread pct ch'] * df1['numUnits'].shift(1)

        df1['cum rets'] = df1['port rets'].cumsum()
        df1['cum rets'] = df1['cum rets'] + 1

        try:
            sharpe = ((df1['port rets'].mean() / df1['port rets'].std()) * sqrt(252))
        except ZeroDivisionError:
            sharpe = 0.0

        plt.plot(df1['cum rets'])
        # plt.xlabel(i[1])
        # plt.ylabel(i[0])
        plt.show()

        ##############################################################

        start_val = 1
        end_val = df1['cum rets'].iloc[-1]

        start_date = df1.iloc[0].name
        end_date = df1.iloc[-1].name
        days = (end_date - start_date).days

        CAGR = round(((float(end_val) / float(start_val)) ** (252.0 / days)) - 1, 4)

        print("CAGR = {}%".format(CAGR * 100))
        print("Sharpe Ratio = {}".format(round(sharpe, 2)))
        # print(100 * "----")

    else:
        print('ADF Test shows that this pair of ' + ticker_pair[0] + "/" + ticker_pair[1] + ' is not mean reverting!')




if __name__ == "__main__":
    ticker_pair = ['MSFT', 'AAPL']
    mean_reversion_pairs(ticker_pair)

    print("Debug")