#! /usr/bin/python

'''
Created on 20.04.2017
 
@author: simon
'''
import pandas as pd
import zipfile,re,argparse
from requests import Session
from datetime import datetime as DT
from io import BytesIO


URL_TEMPLATE='http://www.histdata.com/download-free-forex-historical-data/?/ascii/tick-data-quotes/{pair}/{year}/{month}'
DOWNLOAD_URL="http://www.histdata.com/get.php"
DOWNLOAD_METHOD='POST'
def parse_date(s): return DT.strptime(s+'000',"%Y%m%d %H%M%S%f")


class Downloader:
    def __init__(self,pair,year,month,freq='5s'):
        """
        The forexdownloader Object is capable of downloading tick data of one Month 
        and to convert it to the desired frequency.
        @param pair (str):    the currency pair to download eg. EURUSD, USDNZD, ...
        @param year (int):    the year of interest
        @param month (int):   the month of interesr (1-12)
        @param freq (int):    frequency of candledata in seconds
        
        @method download:   starts the download and returns a pandas.DataFrame
                            with columns (open, close, high low) indexed by date. 
        
        """
        self.url=URL_TEMPLATE.format(pair=pair,year=year,month=month)
        s=self.session=Session()
        self.year,self.month,self.pair=year,month,pair
        self.freq=freq
    def _prepare(self):
        r=self.session.get(self.url)
        m=re.search('id="tk" value="(.*?)"',r.text)
        tk=m.groups()[0]
        self.tk=tk
    def _download_raw(self):
        headers={'Referer':self.url}
        data={'tk':self.tk,'date':self.year,'datemonth':"%d%02d"%(self.year,self.month),'platform':'ASCII','timeframe':'T','fxpair':self.pair}
        r=self.session.request(DOWNLOAD_METHOD,DOWNLOAD_URL,data=data,headers=headers,stream=True)
        self.size=len(r.content)
        zf=zipfile.ZipFile(BytesIO(r.content) )
        self.file=zf.open(zf.namelist()[0])
        
    def _parse_data(self,freq='5s'):
        df=pd.read_csv(self.file,header=None,names='ask bid vol'.split(),parse_dates=True,date_parser=parse_date)
        df['p']=(df.ask+df.bid)/2
        grp=df['p'].groupby(pd.TimeGrouper(freq))
        first=grp.first()
        data=pd.DataFrame(columns='open close high low'.split(),index=first.index)
        #data.open=grp.first() # better to use last close price
        data.close=grp.last()
        data.open=data.close.shift(1)
        data.high=grp.max()
        data.low=grp.min()
        data=data.fillna(method='pad')
        data.index=data.index.tz_localize('EST').tz_convert(None)
        self.data=data
        
        return data
    
    def download(self,freq="5s",verbose=1):
        """
        starts the download.
        @return:    a pandas.DataFrame with columns (open, close, high low) indexed
                    by date
        """
        self._prepare()
        self._download_raw()
        if verbose: print "Downloaded %f MB"%(self.size/2**20)
        self._parse_data(freq)
        return self.data
def download(pair,fro, to,dest,freq='5s'):
    """
    download tickdata of "pair" from "fro" until "to" and save as file "dest"
    @param fro (str):   a datetime string like YYYY-MM 
    @param to (str):    a datetime string like YYYY-MM 
    @param dest (str):  path to destination file
    @param freq (int):  duration of candles
    
    """
    fro=pd.to_datetime(fro)
    to=pd.to_datetime(to)
    
    #data is available monthly
    #so iter over all month from to
    n_years=to.year-fro.year
    n_months=to.month-fro.month
    if n_months<0:
        n_months=n_months%12+1
        n_years-=1
    n_months=n_years*12+n_months    
    
    
    year=fro.year
    month=fro.month
    f=open(dest,'w')
    data=None
    save_header=True
    for m in range(n_months+1):
        df=Downloader(pair, year, month).download(freq)
        #data=df if data is None else data.append(df)
        print "processing %d-%02d"%(year,month)
        df.to_csv(f,mode='a',header=save_header)
        save_header=False
        month+=1
        if month>12:
            month=1
            year+=1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""
    Download historical forexdata from histdata.com. The downloaded
    tickdata may be converted into any Timeframe above one second.
    
    This script relies on the Python-libraries "Pandas" and "requests" - be sure they are
    properly installed.
    
    Author: Simon Schmid (sim.schmid@gmx.net )
    """    )
    parser.add_argument('pair',type=str, 
                help='The currencypair to download.')
    parser.add_argument('from', type=str, 
                        help="Datestring to start from.")
    parser.add_argument('to', type=str, 
                    help="Datestring to stop.")
    parser.add_argument('-d','--duration', type=int,
                        help='The duration of candlesticks. Defaults to 60s',
                        default=60)
    parser.add_argument("-o",'--output', type=str,
                        help='Output file. Defaults to "out.csv".',
                        default='out.csv')
    args=vars(parser.parse_args())
    print """
    Start downloading of {pair} from {from} to {to}.
    Target TimeFrame = {duration},
    Save to {output}
    """.format(**args)
    
    download(args['pair'],args['from'],args['to'],args['output'],"%ds"%args['duration'])