import datetime
import pandas as pd
import numpy as np

import yahoo_fin.stock_info as si
import pandas_datareader.data as web


value = 'MMM'
todaysDate = datetime.date.today()
year  = todaysDate.year
month = todaysDate.month
day   = todaysDate.day
date_start = '2010-01-01'
date_end = '{}-{}-{}'.format(year, month, day)

idx_tickers = ['SP500', 'DJIA', 'VIXCLS']
idx_data = web.DataReader(idx_tickers, 'fred')

ticker_list = ['MMM', "PG", "VZ"]
reference_ticker = 'MMM'
historical_datas = {}
for ticker in ticker_list:
    historical_datas[ticker] = si.get_data(ticker, start_date=date_start, end_date=date_end, index_as_date = True, interval="1d")
    
for idx, ticker in enumerate(historical_datas.keys()):
    tmp_df = historical_datas[ticker].resample('1W').agg(['sum', 'last'])
    if ticker == reference_ticker:
        Y = tmp_df['adjclose']['last'].reset_index()
        Y.columns = ['date', reference_ticker+'_Y']
        df_week = pd.DataFrame()
        df_week['date'] = tmp_df.index
        X_extra = pd.concat([tmp_df['adjclose']['last'].diff(i) for i in [2, 3, 6, 12]], axis=1).dropna().reset_index()
        X_extra.columns = ['date', reference_ticker+'_2W', reference_ticker+'_3W', reference_ticker+'_6W', reference_ticker+'_12W']

    df_week[ticker+'_1W']             = tmp_df['adjclose']['last'].diff(1).values
    df_week[ticker+'_volume']         = tmp_df['volume']['last'].values
    df_week[ticker+'_volume_sum']     = tmp_df['volume']['sum'].values 

    del tmp_df

X = df_week.merge(X_extra, how='left', on='date').dropna()
dataset = X.merge(Y, how='left', on='date')
dataset[reference_ticker+'_Y'] = dataset[reference_ticker+'_Y'].shift(-1)
dataset = dataset.dropna()





