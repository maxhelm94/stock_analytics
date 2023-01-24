import pandas as pd
import os
import numpy
import datetime

import config as cfg


def read_prc_csv(tic):

    tic = tic.lower()

    filename = tic + "_prc.csv"
    filepath = os.path.join(cfg.DATADIR, filename)
    df = pd.read_csv(filepath)
    df = cfg.standardise_colnames(df)
    # df = pd.to_datetime(df['date'])
    df = df.set_index(pd.DatetimeIndex(df['date']))
    df = df.drop(['date'], axis=1)
    '''
    fh = open(filepath)
    names = ""
    for row in fh:
        names = row
        names = names.rstrip()
        break
    namelist = names.split(",")
    print(namelist)

    filehandle = pd.read_csv(filepath)
    print(filehandle.iloc[0,1])
    result1 = pd.DataFrame(index=pd.DatetimeIndex(filehandle.iloc[1:, 0]),
                              data={filehandle.iloc[0,1]: filehandle.iloc[1:, 1].values, filehandle.iloc[0,2]: filehandle.iloc[1:, 2].values,
                                    filehandle.iloc[0,3]: filehandle.iloc[1:, 3].values, filehandle.iloc[0,4]: filehandle.iloc[1:, 4].values,
                                    filehandle.iloc[0,5]: filehandle.iloc[1:, 5].values, filehandle.iloc[0,6]: filehandle.iloc[1:, 6].values})
    '''
    return df


def mk_prc_df(tickers, prc_col='adj_close'):
    result = pd.DataFrame()

    for tic in tickers:
        ticker = tic.lower()
        df = read_prc_csv(ticker)
        df = df[prc_col]
        if result.empty:
            result = pd.DataFrame(index=df.index, data={ticker: df.values})
        else:
            df2 = pd.DataFrame(index=df.index, data={ticker: df.values})
            result = result.join(df2, how="outer")
    return result


def mk_ret_df(prc_df):
    pathToMkt = os.path.join(cfg.DATADIR, "ff_daily.csv")
    daily = pd.read_csv(pathToMkt)

    daily = cfg.standardise_colnames(daily)
    daily = daily.set_index(pd.DatetimeIndex(daily['date']))
    daily = daily.drop(['date', 'smb', 'mkt-rf', 'hml', 'rf'], axis=1)

    copy = prc_df.copy()
    for i in range(len(copy.index)):
        if i == 0:
            for j in range(len(copy.columns)):
                prc_df.iloc[i, j] = numpy.NAN
        else:
            for j in range(len(copy.columns)):
                if pd.isna(copy.iloc[i, j]):
                # if copy.iloc[i, j] == numpy.NAN:
                    continue
                # elif copy.iloc[i - 1, j] == numpy.NAN:
                elif pd.isna(copy.iloc[i - 1, j]):
                    prc_df.iloc[i, j] = numpy.NAN
                else:
                    prc_df.iloc[i, j] = (copy.iloc[i, j] - copy.iloc[i - 1, j]) / copy.iloc[i - 1, j]

    prc_df = prc_df.join(daily, how='inner')
    prc_df = prc_df[prc_df['mkt'].notna()]
    return prc_df


def mk_aret_df(ret_df):
    result = ret_df.copy()
    result = result.drop(['mkt'], axis=1)

    marketData = ret_df['mkt'].values

    for i in range(len(ret_df.index)):
        for j in range(len(ret_df.columns) - 1):
            if pd.isna(result.iloc[i, j]):
            # if result.iloc[i, j] == numpy.NAN:
                continue
            else:
                result.iloc[i, j] = result.iloc[i, j] - marketData[i]
    return result


def get_avg(df, col, year):
    ser = df[col]

    start_date = datetime.datetime(year=year, month=1, day=1)
    end_date = datetime.datetime(year=year, month=12, day=31)

    annual_data = ser.loc[start_date:end_date]
    result = annual_data.sum()
    print(result)
    return result


def get_ew_rets(df, tickers):

    df = df.loc[datetime.datetime(year=1980, month=1, day=1):datetime.datetime(year=1980, month=12, day=31)]

    copy = df.copy()
    for column in tickers:
        if column not in df:
            copy.drop(column, axis=1)

    copy['average'] = numpy.NAN

    for i in range(len(copy.index)):
        sum = 0
        z = 0
        for j in range(len(copy.columns) - 1):
            # if copy.iloc[i, j] is numpy.nan:
            if pd.isna(copy.iloc[i, j]):
                continue
            else:
                sum += copy.iloc[i, j]
                z += 1
        if z == 0:
            copy.iloc[i, -1] = numpy.NAN
        else:
            copy.iloc[i, -1] = sum/z
    result = copy['average']
    return result

def get_ann_ret(ser, start, end):
    start_list = start.split('-')
    end_list = end.split('-')

    start_date = datetime.datetime(year=int(start_list[0]), month=int(start_list[1]), day=int(start_list[2]))
    end_date = datetime.datetime(year=int(end_list[0]), month=int(end_list[1]), day=int(end_list[2]))

    ser = ser.loc[start_date:end_date]
    ser = ser.dropna()

    total_return = 1
    for value in ser:
        total_return = total_return * (1 + value)

    annualized_return = total_return**(252/ser.size) - 1
    print(annualized_return)
    return annualized_return

for key, value in cfg.TICMAP.items():
    annual_return = get_avg(mk_prc_df([key]), key, 2020)
    print(f"{key}: return is {annual_return}")

#get_ew_rets(mk_aret_df(mk_ret_df(mk_prc_df(['aapl', 'tsla']))), ['aapl', 'tsla'])
# series = pd.Series(index=[datetime.datetime(year=2020, month=1, day=2), datetime.datetime(year=2020, month=1, day=3), datetime.datetime(year=2020, month=1, day=4)],
#                    data=[1, -0.25, 0])
# get_ann_ret(series, start="2020-01-01", end="2020-12-31")