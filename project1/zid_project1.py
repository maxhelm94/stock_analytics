import os

import toolkit_config as cfg

ROOTDIR = os.path.join(cfg.PRJDIR, 'project1')
DATDIR = os.path.join(ROOTDIR, 'data')
TICPATH = os.path.join(ROOTDIR, 'tickers.txt')

COLUMNS = ['Volume', 'Date', 'Adj Close', 'Close', 'Open', 'High']


COLWIDTHS = {
    'Volume': 14,
    'Date': 11,
    'Adj Close': 19,
    'Close': 10,
    'Open': 6,
    'High': 20
}

def get_tics(pth):
    """ Reads a file with the tickers (one ticker per line) and returns a list
    with the properly formatted tickers.

    Parameters
    ----------
    pth : str
        Full path to the location of the TICKERS.txt file. 

    Returns
    -------
    list
        List where each element is a lower case string representing a ticker. 

    Notes
    -----
    The tickers returned must conform with the following rules:
        - All characters are in lower case
        - No spaces
        - No empty tickers

    """
    # <COMPLETE THIS PART>
    result = []
    filehandle = open(pth, "r")
    for line in filehandle:
        line = line.rstrip()
        line = line.lower()
        if line == "":
            continue
        result.append(line.rstrip())
    filehandle.close()
    return result


def read_dat(tic):
    """ Returns a list with the lines of the ".dat" file containing the stock
    price information for the ticker `tic`.


    Parameters
    ----------
    tic : str
        Ticker symbol, in lower case. 

    Returns
    -------
    list
        A list with the lines of the ".dat" file for this `tic`. Each element
        is a line in the file, without newline characters (e.g. '\n')

    """
    
    result = []
    filename = tic + "_prc.dat"
    pathToTic = os.path.join(DATDIR, filename)
    filehandle = open(pathToTic, "r")
    for line in filehandle:
        line = line.rstrip()
        result.append(line)
    filehandle.close()
    return result


def line_to_dict(line):
    """ Returns the information contained in a line of a ".dat" file as a
    dictionary, where each key is a column name and each value is a string
    with the value for that column.

    This line will be split according to the field width in `COLWIDTHS` 
    of each column in `COLUMNS`.
   
    Parameters
    ----------
    line : str
        A line from ".dat" file, without any newline characters

    Returns
    -------
    dict
        A dictionary with format {<col> : <value>} where
        - Each key (<col>) is a column in `COLUMNS` (as a string)
        - Each value (<value>) is a string containing the correct value for
          this column.
    """

    result = {}
    sum = 0
    for name in COLUMNS:
        result[name] = line[sum:(sum+COLWIDTHS[name])]
        sum += COLWIDTHS[name]

    return result


def main(csvloc, replace=False):
    """ This function will read the relevant ".dat" files for all tickers in
    the `TICPATH` file and create a CSV file with all the data. 

    This file will contain the following columns:
        | Ticker | <col1> | <col2> |...
    where 
        "Ticker" contains the ticker symbol (in upper case) and "<col1>", ...
        represent the columns in `COLUMNS`

    Parameters
    ----------
    csvloc : str
        The complete path to the output CSV file. This is where the file with
        the data will be saved.

    replace : bool, optional
        Whether an existing output file should be replaced. Defaults to False

    """
    # Check if output file exists
    if os.path.exists(csvloc) and replace is False:
        msg = f"Output file '{csvloc}' already exists and `replace` is set to False"
        raise Exception(msg)

    # Create a variable with the header (first line of the CSV file)
    tic_col = 'Ticker'
    header = [tic_col] + COLUMNS

    # This will create a list containing all lines to be included in the output CSV file
    # The advantage of creating the list first is that no empty file will be
    # created in the case of an exception. The disadvantage is that it
    # consumes more memory.
    dst_lines = [header]
    for tic in get_tics(TICPATH):
        src_lines = read_dat(tic)
        for src_line in src_lines:
            dic = line_to_dict(src_line)
            dic[tic_col] = tic.upper()
            dst_line = [dic[col] for col in header]
            dst_lines.append(dst_line)

    with open(csvloc, 'w') as fobj:
        for line in dst_lines:
            line = ','.join(line)
            fobj.write(f'{line}\n')

    



# ---------------------------------------------------------------------------- 
#   Test functions:
# ---------------------------------------------------------------------------- 
def _test_get_tics():
    """ Test function for the `get_tics` function. Will print the tickers as
    returned by the `get_tics` function. 
    """
    pth = TICPATH
    tics = get_tics(pth)
    print(tics)

def _test_read_dat():
    """ Test function for the `read_dat` function. Will read the lines of the
    first ticker in `TICPATH` and print the first line in the list. 
    """
    pth = TICPATH
    tics = get_tics(pth)
    tic = tics[0]
    lines = read_dat(tic)
    # Print the first line in the file
    print(f'The first line in the dat file for {tic} is:')
    print(lines[0])
    

def _test_line_to_dict():
    """ Test function for the `read_dat` function. This function will perform
    the following operations:
    - Get the tickers using `get_tics`
    - Read the lines of the ".dat" file for the first ticker
    - Convert the first line of this file to a dictionary
    - Print this dictionary
    """
    pth = TICPATH
    tics = get_tics(pth)
    lines = read_dat(tics[0])
    dic = line_to_dict(lines[0])
    print(dic)

if __name__ == "__main__":
    # Test functions
    _test_get_tics()
    _test_read_dat()
    _test_line_to_dict()

    # Uncomment to run the main function
    csvloc = 'data.csv'
    main(csvloc, replace=True)

    #pass


    



