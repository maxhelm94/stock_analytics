import os

import numpy
import datetime
import pandas as pd


import config as cfg


def read_prc_csv(tic):
    """ This function creates a data frame with the contents of a CSV file 
    containing stock price information for a given ticker. 
    
    Parameters
    ----------
    tic : str
        String with the ticker (can include lowercase and/or uppercase
        characters)

    Returns
    -------
    df 
        A Pandas data frame containing the stock price information from the CSV
        containing the stock prices for the ticker `tic`.

        This data frame must meet the following criteria:
        
        - df.index: `DatetimeIndex` with dates, matching the dates contained in
          the CSV file. The labels in the index must be `datetime` objects.

        - df.columns: each column label will be a column in the CSV file, 
          with the exception of 'Date'. Column names will be formatted
          according to the `standardise_colnames` function included in the
          `project2.config.py` module.

    """
    tic = tic.lower()

    filename = tic + "_prc.csv"
    filepath = os.path.join(cfg.DATADIR, filename)

    result = pd.read_csv(filepath)
    result = cfg.standardise_colnames(result)
    result = result.set_index(pd.DatetimeIndex(result['date']))
    result = result.drop(['date'], axis=1)

    '''
    result = pd.DataFrame(index=pd.DatetimeIndex(filehandle.iloc[1:, 0]),
                          data={"Open": filehandle.iloc[1:, 1].values, "High": filehandle.iloc[1:, 2].values,
                                "Low": filehandle.iloc[1:, 3].values, "Close": filehandle.iloc[1:, 4].values,
                                "Adj Close": filehandle.iloc[1:, 5].values, "Volume": filehandle.iloc[1:, 6].values})
    '''
    return result


def mk_prc_df(tickers, prc_col='adj_close'):
    """ This function creates a data frame containing price information for a
    list of tickers and a given type of quote (e.g., open, close, ...)  

    This function uses the `read_prc_csv` function in this module to read the
    price information for each ticker in `tickers`.

    Parameters
    ----------
    tickers : list
        List of tickers

    prc_col: str, optional
        String with the name of the column we will use to compute returns. The
        column name must conform with the format in the `standardise_colnames`
        function defined in the config.py module.  
        Defaults to 'adj_close'.

    Returns
    -------
    df
        A Pandas data frame containing the `prc_col` price for each stock
        in the `tickers` list:
        
        - df.index: DatetimeIndex with dates. The labels in this index must
          include all dates for which there is at least one valid price quote
          for one ticker in `tickers`.  


        - df.columns: each column label will contain the ticker code (in lower
          case). The number of columns in this data frame must correspond to
          the number of tickers in the ``tickers` parameter. 

    """

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
    """ Creates a data frame containing returns for both individuals stock AND 
    a proxy for the market portfolio, given a data frame with stock prices, `prc_df`. 

    This function will compute returns for each column of `prc_df` and also
    include the market returns in a column called "mkt".  

    Market returns need to be obtained from the "mkt" column in the CSV file
    "ff_daily_csv". The location of this CSV file is given by the variable
    `FF_CSV`, defined in the project2.config.py module. You should **not**
    include a string literal with the location of this file in the body of
    this function. Instead, use the variable FF_CSV and the appropriate `os`
    method to generate a string with the location of the file.


    Parameters
    ----------
    prc_df : data frame
        A Pandas data frame with price information (the output of
        `mk_prc_df`). See the docstring of the `mk_prc_df` function
        for a description of this data frame.


    Returns
    -------
    df
        A data frame with stock returns for each ticker in `prc_df` AND the
        returns for the proxy of the overall market portfolio ("mkt").

        - df.index: DatetimeIndex with dates. These dates should include all
          dates in `prc_df` which are also present in the CSV file FF_CSV. 

        - df.columns: Includes all the column labels in `prc_df.columns` AND
          the column label for market returns, "mkt".

    """

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
                    continue
                elif pd.isna(copy.iloc[i - 1, j]):
                    prc_df.iloc[i, j] = numpy.NAN
                else:
                    prc_df.iloc[i, j] = (copy.iloc[i, j] - copy.iloc[i - 1, j]) / copy.iloc[i - 1, j]

    prc_df = prc_df.join(daily, how='inner')
    # prc_df = prc_df[prc_df['mkt'].notna()]
    return prc_df


def mk_aret_df(ret_df):
    """ Creates a data frame with abnormal returns for each stock in `ret_df`,
    where abnormal returns are computed by subtracting the market returns from
    the individual stock returns.

    Parameters
    ----------
    ret_df : data frame

        A Pandas data frame with return information for individual stocks and
        the proxy for the overall market portfolio. This data frame is the
        output of `mk_ret_df`.  See the docstring of the `mk_ret_df` function
        for a description of this data frame.

    Returns
    -------
    df
        A data frame with abnormal returns for each individual stock in
        `ret_df`. Abnormal returns are computed by subtracting the market
        returns (column "mkt" in `ret_df`)  from each individual stock's
        returns. 

        - df.index: DatetimeIndex with dates. These dates should include all
          dates in the `ret_df` data frame.

        - df.columns: Each column label will be a ticker from the `ret_df`
          (i.e., all the columns of `ret_df` EXCLUDING the column "mkt").
    
    """

    result = ret_df.copy()
    result = result.drop(['mkt'], axis=1)

    marketData = ret_df['mkt'].values

    for i in range(len(ret_df.index)):
        for j in range(len(ret_df.columns) - 1):
            if pd.isna(result.iloc[i, j]):
                continue
            else:
                result.iloc[i, j] = result.iloc[i, j] - marketData[i]

    return result



# ---------------------------------------------------------------------------- 
# Part 7: Auxiliary functions
# ---------------------------------------------------------------------------- 
def get_avg(df, col, year):
    """ Returns the average value of a column for a give year.

    This function will calculate the average value of the elements included in
    a column labelled `col` from a data frame `dt`, for a given year `year`.
    The data frame `df` must have a DatetimeIndex index.

    Missing values will not be included in the calculation.
    
    Parameters
    ----------
    df : data frame
        A Pandas data frame with a DatetimeIndex index.

    col : str
        The column label.

    year : int
        The year as a 4-digit integer.

    Returns
    -------
    scalar
        A scalar with the average value of the column `col` for the year
        `year`.
    """

    ser = df[col]

    start_date = datetime.datetime(year=year, month=1, day=1)
    end_date = datetime.datetime(year=year, month=12, day=31)

    annual_data = ser.loc[start_date:end_date]
    sum = 0
    z = 0
    for x in annual_data:
        if pd.isna(x):
            continue
        else:
            sum += x
            z += 1
    result = sum / z
    return result


def get_ew_rets(df, tickers):
    """ Returns a series with the returns on an equally-weighted portfolio
    of stocks (ignoring missing values).

    Parameters
    ----------
    df : data frame
        A Pandas data frame stock returns. Each column label is the stock
        ticker (in lower case).

    tickers : list
        A list of tickers (in lower case) to be included in the portfolio.

    Returns
    -------
    pandas series
        A series with the same DatetimeIndex as the original data frame,
        containing the average of all the columns in `tickers`. The
        equal-weighted average will ignore missing values.
    """

    copy = df.copy()
    for column in tickers:
        if column not in df:
            copy.drop(column, axis=1)

    copy['average'] = numpy.NAN

    for i in range(len(copy.index)):
        sum = 0
        z = 0
        for j in range(len(copy.columns) - 1):
            if pd.isna(copy.iloc[i, j]):
                continue
            else:
                sum += copy.iloc[i, j]
                z += 1
        if z == 0:
            copy.iloc[i, -1] = numpy.NAN
        else:
            copy.iloc[i, -1] = sum / z

    result = copy['average']
    return result




def get_ann_ret(ser, start, end):
    """ Returns the annualised returns for a given period.

    Given a series with daily returns, this function will return the
    annualised return for the period from `start` to `end` (including `end`).

    Parameters
    ----------
    ser : series
        A Pandas series with a DatetimeIndex index and daily returns.

    start : str
        A string representing the date corresponding to the beginning of the
        sample period in ISO format (YYYY-MM-DD).

    end : str
        A string representing the date corresponding to the end of the
        sample period in ISO format (YYYY-MM-DD).

    Returns
    -------
    scalar
        A scalar with the ANNUALISED return for the period starting in `start`
        and ending in `end`, ignoring missing observations. 

    """
    
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

    return annualized_return


Q1_ANSWER = 'TSLA'
Q2_ANSWER = '0.2009'
Q3_ANSWER = '0.5516'
Q4_ANSWER = '0.3770'
    



# ----------------------------------------------------------------------------
#   Test functions 
# ----------------------------------------------------------------------------

def _test_print(obj, msg=None):
    """ Pretty prints `obj`. Will be used by other `_test` functions

    Parameters
    ----------
    obj : any object

    msg : str, optional
        Message preceding obj representation

    """
    import pprint as pp
    sep = '-'*40
    if isinstance(obj, str):
        prt = obj
    else:
        prt = pp.pformat(obj)
        prt = f'{prt}\n\nObj type is: {type(obj)}'
    if msg is not None:
        prt = f'{msg}\n\n{prt}'
    to_print = [
        '',
        sep,
        prt,
        ]
    print('\n'.join(to_print))
    if isinstance(obj, pd.DataFrame):
        print('')
        obj.info()
    print(sep)

# This is an auxiliary function, please do not modify
def _test_cfg():

    parent = os.path.dirname(cfg.DATADIR)
    to_print = f'''
The variable `parent` should point to the project2 folder:
  parent: '{parent}'
  Folder exists: '{os.path.exists(parent)}'

The data folder for this project is located at:
  cfg.DATADIR: '{cfg.DATADIR}'
  Folder exists: '{os.path.exists(cfg.DATADIR)}'
'''
    _test_print(to_print.strip())


def _test_read_prc_csv():
    """ Test function for `read_prc_csv`
    """
    tic = 'TSLA'
    df = read_prc_csv(tic)
    _test_print(df)


def _test_mk_prc_df():
    """ Test function for `mk_prc_df`
    """
    tickers = ['AAPL', 'TSLA']
    prc_df = mk_prc_df(tickers, prc_col='adj_close')
    _test_print(prc_df)

def _test_mk_ret_df():

    # Test data frame
    prc_df = pd.DataFrame({
        'aapl': [
            121.09, 
            121.19, 
            120.70, 
            119.01,
            124.40, 
            ],
        'tsla': [
            446.64, 
            461.29, 
            448.88, 
            439.67,
            None, 
            ],
        },
        index=pd.to_datetime([
            '2020-10-13', 
            '2020-10-14', 
            '2020-10-15', 
            '2020-10-16',
            '2020-10-12', 
            ],
        ))
    msg = "The input data frame `prc_df` is:"
    _test_print(prc_df, msg=msg)

    msg = "The output data frame `ret_df` is:"
    ret_df = mk_ret_df(prc_df)
    _test_print(ret_df, msg=msg)


def _test_mk_aret_df():

    idx = pd.to_datetime([
        '2020-10-12', 
        '2020-10-13', 
        '2020-10-14', 
        '2020-10-15', 
        '2020-10-16', 
        ])
    aapl = [
        None, 
        -0.026608, 
         0.000826, 
        -0.004043, 
        -0.014002, 
        ]
    tsla = [
        None, 
        None,
         0.032800,
        -0.026903,
        -0.020518,
        ]
    mkt = [
      0.0153,
     -0.0041,
     -0.0065,
     -0.0008,
     -0.0006,
     ]
    ret_df = pd.DataFrame({'aapl': aapl, 'tsla': tsla, 'mkt': mkt,}, index=idx)
    _test_print(ret_df)

    aret_df = mk_aret_df(ret_df)
    _test_print(aret_df)


def _test_get_avg():

    # Made-up data
    prc = pd.Series({
        '2019-01-01': 1.0,
        '2019-01-02': 2.0,
        '2020-10-02': 4.0,
        '2020-11-12': 4.0,
        })
    df = pd.DataFrame({'some_tic': prc})
    df.index = pd.to_datetime(df.index)
    
    msg = 'This is the test data frame `df`:'
    _test_print(df, msg)


    res = get_avg(df, 'some_tic', 2019)
    to_print = [
            "This means `res =get_avg(df, col='some_tic', year=2019) --> 1.5",
            f"The value of `res` is {res}",
            ]
    _test_print('\n'.join(to_print))
    

def _test_get_ew_rets():


    # Made-up data
    tic1 = [1.0, 2.0, 1.0, 2.0,]
    tic2 = [2.0, None, 2.0, 1.0,]
    tic3 = [99, 99, 99, 99,]
    idx = pd.to_datetime(['2019-01-01', '2019-01-02', '2020-10-02', '2020-11-12'])
    df = pd.DataFrame({'tic1': tic1, 'tic2': tic2, 'tic3': tic3}, index=idx)
    msg = 'This is the test data frame `df`:'
    _test_print(df, msg)

    ew_ret = get_ew_rets(df, ['tic1', 'tic2'])
    msg = "The output of get_ew_rets(df, ['tic1', 'tic2']) is:"
    _test_print(ew_ret, msg)



def _mk_test_ser():
    """ This function will generate a test series with the following
    characteristics:

    - There are 400 obs
    - All values are the same (the daily_yield below)
    - The cumulative return over the 400 days is 50%
    - The datetime index starts in '2010-01-01
    """
    
    tot_ret = 1.5
    n = 400
    start = '2010-01-01'
    daily_yield = tot_ret**(1.0/n)-1

    # This is the expected result (the annualised return)
    exp_res = tot_ret ** (252./n) - 1

    # Create a series of timedelta objects, representing
    # 0, 1, 2, ... days
    start_dt = pd.to_datetime(start)
    days_to_add = pd.to_timedelta([x for x in range(400)], unit='day')
    idx = start_dt + days_to_add

    # Then create the series
    ser = pd.Series([daily_yield]*n, index=idx)

    # So, `get_ann_ret(ser, start, end) --> exp_res`
    # We have the `ser` and `start`. What about `end`?
    end = ser.index.max().strftime('%Y-%m-%d')
    
    to_print = [
            f"Given the parameters:",
            f"   - tot_ret is {tot_ret}",
            f"   - N is {n}",
            f"   - start is '{start}'",
            f"   - end is '{end}'",
            f" For the period from '{start}' to '{end}'",
            f" the annualised return is: {exp_res}",
            "",
            f"For this `ser`, `get_ann_ret(ser, '{start}', '{end}')` --> {exp_res}",
            ]
    print('\n'.join(to_print))

    # add periods before `start` and `end`
    start_dt, end_dt = ser.index.min(), ser.index.max()

    idx_bef = start_dt + pd.to_timedelta([-3, -2, -1], unit='day')
    ser_bef = pd.Series([-99]*len(idx_bef), index=idx_bef)

    idx_after = end_dt + pd.to_timedelta([1, 2, 3], unit='day')
    ser_after = pd.Series([99]*len(idx_after), index=idx_after)

    ser = pd.concat([ser_bef, ser, ser_after])

    res = get_ann_ret(ser, start, end)
    print(res)

    return ser


def _test_get_ann_ret():
    """ Test function for `get_ann_ret`

    To construct this example, suppose first that holding the stock for 400
    trading days gives a total return of 1.5 (so 50% over 400 trading days).

    The annualised return will then be:

        (tot_ret)**(252/N) - 1 = 1.5 ** (252/400) - 1 = 0.2910

    Create an example data frame with 400 copies of the daily yield, where

        daily yield = 1.5 ** (1/400) - 1

    """
    # Parameters
    tot_ret = 1.5
    n = 400
    start = '2010-01-01'
    daily_yield = tot_ret ** (1.0/n) - 1
    print(daily_yield)

    # This is what the function `get_ann_ret` should return
    expected_res = tot_ret ** (252./n) - 1

    # Create the index
    # This will add `n` days to `start`
    n_days = pd.to_timedelta([x for x in range(n)], unit='day')
    dt_idx = pd.to_datetime(start) + n_days

    # Get the end date
    end = dt_idx.max().strftime('%Y-%m-%d')

    # So, `end` - `start` --> n days

    # Create a series with `n` copies of `daily_yield`
    ser = pd.Series([daily_yield] * n, index=dt_idx)


    # Add days before `start` and after `end`
    dt_bef_idx = pd.to_datetime(start) + pd.to_timedelta([-3, -2, -1], unit='day')
    ser_before = pd.Series([-99]*3, index=dt_bef_idx)


    dt_after_idx = pd.to_datetime(end) + pd.to_timedelta([1, 2, 3], unit='day')
    ser_after = pd.Series([-99]*3, index=dt_after_idx)

    # combine series
    ser = pd.concat([ser_before, ser, ser_after])


    msg = 'This is the test ser `ser`:'
    _test_print(ser, msg)

    res = get_ann_ret(ser, start, end)
    to_print = [
        f"This means `res = get_ann_ret(ser, start='{start}', end='{end}') --> {expected_res}",
        f"The value of `res` is {res}",
        ]
    _test_print('\n'.join(to_print))



if __name__ == "__main__":
    pass
    #_test_cfg()
    #_test_read_prc_csv()
    #_test_mk_prc_df()
    #_test_mk_ret_df()
    #_test_mk_aret_df()
    #_test_get_avg()
    #_test_get_ew_rets()
    #_test_get_ann_ret()

    # Q1 answer
    tickerlist = list(cfg.TICMAP.keys())
    for key, value in cfg.TICMAP.items():
        ticker = key.lower()
        #print(mk_prc_df(tickerlist))
        annual_return = get_avg(mk_ret_df(mk_prc_df(tickerlist)), ticker, 2020)
        print(f"{key}: return is {annual_return}")
    

     # Q2 answer
    tickerlist = list(cfg.TICMAP.keys())
    lst = []
    for x in tickerlist:
        lst.append(x.lower())
    print(get_ann_ret(get_ew_rets(mk_ret_df(mk_prc_df(lst)), lst), "2010-01-01", "2020-12-31"))


    # Q3 answer
    print(get_ann_ret(mk_ret_df(mk_prc_df(['tsla']))['tsla'], "2010-01-01", "2020-12-31"))


     # Q4 answer
    print(get_ann_ret(mk_aret_df(mk_ret_df(mk_prc_df(['tsla'])))['tsla'], "2010-01-01", "2020-12-31"))











