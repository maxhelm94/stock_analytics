# ---------------------------------------------------------------------------- 
#   About this file
# ---------------------------------------------------------------------------- 

This file contains information about the price DAT files. These files contain
fixed-width data for the following columns: 

  - 'Volume': The total number of shares traded on the day (in millions)

  - 'Date': The calendar day of this data. Each date is represented as 
            a four-digit year, a two-digit month, and a two-digit day 
            separated by hyphens. This is YYYY-MM-DD format.

  - 'Adj Close': The closing price of the stock, adjusted for dividends, 
            splits, and other events.

  - 'Close': The closing price at which the stock trades on the day, 
            unadjusted for dividends, splits, and other events.

  - 'Open': The initial price at which the stock trades for the day, 
            unadjusted for dividends, splits, and other events.

  - 'High': The highest price at which the stock trades for the day, 
            unadjusted for dividends, splits, and other events.

The data is in a fixed-width format. Instead of using delimiters, such as
a comma, to separate the data columns, data is simply presented as text,
with one data field leading directly to the next. For example, if the first
piece of information in a row is a four-character word (e.g. "blue"), the
next piece of information is a four-character number (e.g. "3.14"), and the
third piece of information is a one-character Boolean value (e.g. "t" for
true), the row will be presented as "blue3.14t". All data would need to meet
this format with no item shorter or longer than its specified length.

Identification of information in the file requires knowing the ordering
of the columns and the width of each column. Each line in your actual
stock price DAT file has exactly 80 characters. The information for the
first column starts at the beginning of the line and stops after the `n`
characters, where `n` is the width of the first column. The information for
the second column starts immediately after the first, stopping after `m`
characters. Thus, the second column contains information from character
`n+1` through `n+m`. The remaining columns follow this logic.

For instance, assume that the first column in the `DAT` files contains is
`Volume` and, based on the information you are given, the width of that column
is `14`. This means that the data for this column starts at the beginning of
the line and continue up to and including the 14th character in that line.
The data for the next column begins immediately after the 14th character,
with the 15th character in the line.

For your specific DAT files, you have the following information:

Column Name:
    column position : The order of this column in the file starting with 1. 
                      That is, the first column has column position 1, the 
                      second column has column position 2, and so on. 

    dtype :           The Pandas data type for this column

    width :           The number of characters in this column

# ---------------------------------------------------------------------------- 
#   Column information
# ---------------------------------------------------------------------------- 

Volume:
  column position: 1
  dtype: int64
  width: 14

Date:
  column position: 2
  dtype: datetime64
  width: 11

Adj Close:
  column position: 3
  dtype: float64
  width: 19

Close:
  column position: 4
  dtype: float64
  width: 10

Open:
  column position: 5
  dtype: float64
  width: 6

High:
  column position: 6
  dtype: float64
  width: 20


