'''
Created on 12.02.2017

@author: simon
'''
from datetime import datetime as dt, datetime
import pandas as pd,numpy as np
import sqlite3
from threading import current_thread
import threading

class Candle(object):
    '''
    Representation of a single candle
    '''
    def __init__(self,onclose,duration=10):
        '''
        :param callable onclose:
        :param int duration:
        '''
        self.open=None
        self.close=None
        self.high=None
        self.low=None
        self.start_time=None
        self.duration=duration
        self.last_val=None
        self.onclose=onclose
        self.value=None
    def add_value(self,value,timestamp):
        '''
        Adds a value to the candle and updates open,close,high and low
        :param float value:
        :param int timestamp:
        '''
        time=dt.fromtimestamp(timestamp)
        self.value=value
        if not self.open and time.second%self.duration==0:
            self.open=value
            self.high=value
            self.low=value
            self.start_time=dt.fromtimestamp(timestamp)
        self.high=max(self.high,value)
        self.low=min(self.low,value)
        self.last_val=(value,timestamp)
        
        if self.open and (time-self.start_time).seconds>0 and time.second%self.duration==0:
            self.close=self.last_val[0]
            self.onclose(self,(value,timestamp))
            return False
        return True
    
class CandleStorage(object):
    '''
    The candlestorage manages all candles inside a dataframe. Its respnsible
    for Caching 
    When data exceeds max_size, as much data till min_size is reached will 
    be saved inside an sqlite file
    if more then the available data is requested, a sql query will fetch the
    remaining part.
    
    '''
    cols=['date','open','close','high','low']
    def __init__(self,data=None,max_size=5000,min_size=2000,name='test.db',tablename='stock'):
        '''
        :param pd.DataFrame data:
        :param max_size: max amount af data to save in memory.
        :param min_size: min amount of data to save in memory
        '''
        self.data = data or pd.DataFrame(columns=self.cols)
        self.idx=self.data.index.max() if data is not None else -1
        self.name=name
        self.tablename=tablename
        self._conn=sqlite3.connect(name)
        self._name=name
        self.tid=threading.current_thread().ident
        self.max_size=max_size
        self.min_size=min_size
        self.last_saved_idx=-1
        self.create_table()
        self.len=len(self.data)
        self._conns={}
    
    @property
    def conn(self):
        tid=threading.current_thread().ident
        if self.tid==tid: return self._conn
        con=self._conns.get(tid,sqlite3.connect(self._name))
        self._conns[tid]=con
        return con
    def append(self,candles,sort_date=True):
        '''
        append a candle
        :param list candles: a list of :class: Candle
        :param bool sort_date (optional): Wether to sort by date. Default = True
        :type candle:Candle
        '''
        dlen=len(self.data)
        
        if isinstance(candles,np.ndarray):
            candles=pd.DataFrame(candles)
            candles.columns=self.cols
        if type(candles)==list and type(candles[0])==Candle:
            for candle in candles:
                self.data.loc[self.idx+1]=(candle.start_time,candle.open,candle.close,candle.high,candle.low)
                self.idx+=1
        elif type(candles)==pd.DataFrame:
            #reindex
            candles.index=np.arange(self.idx+1,self.idx+1+len(candles))
            self.data=self.data.append(candles)
            self.idx=self.idx+1+len(candles)
        else:
            candles=np.array(candles)
            for candle in candles:
                self.data.loc[self.idx+1]=tuple(candle)
                self.idx+=1
        self.data=self.data.drop_duplicates('date')
        self.len+=len(self.data)-dlen  
        
        if sort_date: 
            self.data=self.data.sort_values('date')
        self.data.set_index(np.arange(len(self.data))+self.last_saved_idx+1)
        
        self.free()
        
    def free(self):
        if len(self.data)>self.max_size:
            df=self.data.iloc[:-self.min_size]
            self.data=self.data.iloc[-self.min_size:]
            #save df to sql
            c=self.conn.cursor()
            idx=df.index
            self.last_saved_idx=idx[-1]
            date=df.date.map(dt_to_int)
            vals=np.array([idx,date,df.open,df.close,df.high,df.low]).T
            c.executemany("replace into {tn}  values (?,?,?,?,?,?)".format(tn=self.tablename), vals)
            self.conn.commit()
        
    
    def __getslice__(self,i,j):

        if isinstance(i,(str,datetime)): return self._get_date_slice(i,j)
        i=max(0,self.__len__()+i) if i<0 else i
        j=j%self.__len__() if j<0 else j
        if i<self.last_saved_idx or j<self.last_saved_idx:
            isql=min(i,self.last_saved_idx)
            jsql=min(j,self.last_saved_idx+1)
            print jsql,isql
            
            r=pd.read_sql("select * from {tn} where idx>=? and idx<?".format(tn=self.tablename),
                          self.conn,
                          index_col='idx',
                          params=(isql,jsql),
                          #parse_dates={'date':'s'}
                          )
            r['date']=r.date.map(ts_to_dt)
            return r.append(self.data.loc[self.last_saved_idx:j-1])#.sort_index()
        return self.data.loc[i:j]
    
    def _get_date_slice(self,i,j):
        i=pd.to_datetime(i) if i else dt(year=1971,month=1,day=1)
        j=pd.to_datetime(j) if j else dt(year=2200,month=1,day=1)
        last_saved_date=self.data.loc[self.last_saved_idx+1].date
        if i<last_saved_date or j<last_saved_date:
            isql=min(i,last_saved_date)
            jsql=min(j,last_saved_date)
            print jsql,isql
            
            r=pd.read_sql("select * from {tn} where date>=? and date<?".format(tn=self.tablename),
                          self.conn,
                          index_col='idx',
                          params=(dt_to_int(isql),dt_to_int(jsql)),
                          #parse_dates={'date':'s'}
                          )
            #idx=self.data.index
            r['date']=r.date.map(ts_to_dt)
            return r.append(self.data.set_index('date')[last_saved_date:j].reset_index())#.sort_index()
        return self.data.set_index('date')[i:j].reset_index()        
    
    def __getitem__(self,key):
        if self.len==0: return self.data
        if type(key)==slice:
            return self.__getslice__(key.start, key.stop)
        elif type(key)==int:
            key=key%self.len
            return self.__getslice__(key,key+1).iloc[0]
        elif isinstance( key,str):
            key=pd.Period(key)
            start=key.to_timestamp()
            end=(key+1).to_timestamp()
            return self.__getslice__(start,end)
        
        elif isinstance(key,datetime):
            return pd.read_sql("select * from {tn} where date=?".format(tn=self.tablename),
              self.conn,
              index_col='idx',
              params=(dt_to_int(key)),
              parse_dates={'date':'s'}
              ).iloc[0]
    def __setslice__(self,i,j,seq):
        
        self.data[i:j]=seq
    
    def __delslice__(self,i,j):
        return self.data.__delslice__(i,j)
    
    def __len__(self): 
        #c=self.conn.cursor()
        #c.execute("SELECT COUNT(*) from stock ")
        return self.len#c.fetchone()[0]+len(self.data)
    
    def create_table(self):
        c=self.conn.cursor()
        c.execute("PRAGMA journal_mode=OFF;")
        
        self.conn.commit()
        c.executescript("""
        create table if not exists {tn}(
                    idx INTEGER PRIMARY KEY,
                    date INTEGER,
                    open FLOAT,
                    close FLOAT,
                    high FLOAT,
                    low FLOAT
                    );
        """.format(tn=self.tablename))
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS {tn}_index on {tn} (date)".format(tn=self.tablename))
        self.conn.commit()
        
    def clear(self):
        self.conn.execute("drop table if exists {tn}".format(tn=self.tablename))
        self.create_table()
        self.conn.commit()
        
from time import mktime
from datetime import datetime as DT


class CandleStorage2():
    cols=['date','open','close','high','low']
    def __init__(self,data=None,max_size=5000,min_size=2000,name='test.db',tablename='stock'):
        '''
        :param pd.DataFrame data:
        :param max_size: max amount af data to save in memory.
        :param min_size: min amount of data to save in memory
        :param name: db filename
        :param tablename: tablename 
        '''
        self.data = data or pd.DataFrame(columns=self.cols)
        self.idx=self.data.index.max() if data is not None else -1
        self.name=name
        self._conn=sqlite3.connect(name)
        self._name=name
        self.tid=threading.current_thread().ident
        self.max_size=max_size
        self.min_size=min_size
        self.last_saved_idx=-1
        self.len=0
        self.tablename=tablename
        self.create_table()
        if len(self.data): self.append(self.data)
        
    @property
    def conn(self):
        '''
        returns the sqlite connection. If called from another thread this property
        returns a new sqlite connection
        '''
        if self.tid==threading.current_thread().ident: return self._conn
        return sqlite3.connect(self._name)
    def append(self,df):
        '''
        :param pd.DataFrame df: a Pandas Datafram to append
        '''
        if len(df)==0:return 0
        conn=self.conn
        c=conn.cursor()
        #idx=df.index
        #self.last_saved_idx=idx[-1]
        
        if not isinstance(df, pd.DataFrame):
            df=pd.DataFrame(list(df),columns=['date',"open","close",'high','low'])
            df=(df.set_index('date')*1e-6).reset_index()
            df['ddate']=df.date.map(ts_to_dt)
            print df
        date=df.date
        if isinstance(df.date.iloc[0],dt): date=df.date.map(dt_to_int)
        vals=np.array([date,df.open,df.close,df.high,df.low]).T
        c.executemany("replace into {tn}  values (?,?,?,?,?)".format(tn=self.tablename), vals)
        self.len+=len(df)
        conn.commit()
    def create_table(self):
        '''
        Create the initial sqlite-table if not exists
        '''
        c=self.conn.cursor()
        c.execute("PRAGMA journal_mode=OFF;")
        
        self.conn.commit()
        c.executescript("""
        create table if not exists {tn}(
                    date INTEGER,
                    open FLOAT,
                    close FLOAT,
                    high FLOAT,
                    low FLOAT
                    );
        """.format(tn=self.tablename))
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS {tn}_index on {tn} (date)".format(tn=self.tablename))
        self.conn.commit()
        self.len=c.execute("select count(*) from {tn}".format(tn=self.tablename)).fetchone()[0]
        
    def clear(self):
        '''
        Clear the database
        '''
        self.conn.execute("drop table if exists {tn}".format(tn=self.tablename))
        self.create_table()
        self.conn.commit()
    
    @property
    def max_date(self):
        '''
        The last saved date in database if any else None
        '''
        if not self.len: return None
        return ts_to_dt(self.conn.execute('select max(date) from {tn}'.format(tn=self.tablename)).fetchone()[0] )
    @property
    def min_date(self):
        '''
        The first saved date in database if any else none
        '''
        if not self.len: return None
        return ts_to_dt(self.conn.execute('select min(date) from {tn}'.format(tn=self.tablename)).fetchone()[0] )
    def __getitem__(self,key):
        '''
        Return Elements from databas
        :param int key: return row with rowid key
        :param str key: parses datestring and returns matched elements
        :param datetime key: return elements with date key
        Further all slice operations with above types are allowed, except the stepsize
        '''
        col='date'
        slc=None
        if isinstance(key, str):
            s=pd.Period(key)
            a,b=(s,s+1)
            slc=slice(dt_to_int(a.to_timestamp()),dt_to_int(b.to_timestamp()))
        elif isinstance(key,int):
            if key<0: key=self.len+key-1
            slc=slice(key,key+1)
            col='rowid'
        elif isinstance(key,slice) and isinstance(key.start,int):
            a,b=key.start,key.stop
            if key.start<0: a=self.len+key.start-1
            if key.stop<0: b=self.len+key.stop-1
            col='rowid'
            slc=slice(a,b)
        elif isinstance(key,slice) and isinstance(key.start,(str,DT)):
            a,b=pd.to_datetime(key.start),pd.to_datetime(key.stop)
            if a is None:a=DT(1971,1,1)
            if b is None:b=DT(2971,1,1)
            slc=slice(dt_to_int(a),dt_to_int(b))
         
        a,b=None,None
        a=slc.start
        b=slc.stop
        
        if col=='rowid':
            a=min(2**32,a+2)
            b=min(2**32,b+2)
        
        ret=pd.read_sql("select * from {tn} where {col}>=? and {col}<? order by date".format(col=col,tn=self.tablename),
          self.conn,
          #index_col='idx',
          params=(a,b),
          #parse_dates={'date':'s'}
          )
        ret.date=ret.date.map(ts_to_dt)
        return ret
    def __len__(self): return self.len
    
    def is_complete(self):
        mi=self.min_date
        ma=self.max_date
        freq=(self[1].date-self[0].date).iloc[0].total_seconds()
        if (ma-mi).total_seconds()/freq==len(self): return True
        return False
            
    
def dt_to_int(dt):
    '''
    convert datetime to unixtimestamp as int
    :param datetime.datetime dt:
    '''
    return mktime(dt.timetuple())

def ts_to_dt(ts):
    return DT.fromtimestamp(ts)
