# hddl
a histdata.com downloader script.

The script downloads tick-data from histdata.com and converts it to the desired candlestick data.


## Command-Line
Usage example for the command line to get EURUSD from 2013-01 to 2104-01 with Candles of duration 60s:

hddl.py -d 60 -o out.csv EURUSD 2013-01 2014-01

The data becomes saved as csv with each row as (date, open, close, high ,low)

## Python 

One may import the Downloader class

    from hddl.hddl import Download  
    data=Download(pair,year,month,freq='5s',verbose=1).download()
