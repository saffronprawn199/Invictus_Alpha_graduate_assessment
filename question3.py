import time
import ccxt
import calendar
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta 
from requests import Session, Request, Response
from ciso8601 import parse_datetime
from typing import Optional, Dict, Any, List
import hmac
import urllib.parse

# Change this to false to get question 3.3 answers
# You can extract data by setting flag to true if you want,
# Not neede as data file are included
QUESTION3_1_AND_QUESTION3_2 = True

if QUESTION3_1_AND_QUESTION3_2:
    EXTRACT_DATA = False
    PERPETUAL_FUTURES = False
    CALCULATE_RATIOS = True
else:
    EXTRACT_DATA = False
    PERPETUAL_FUTURES = False
    CALCULATE_RATIOS = False


# Create a class for pagnition class so that you 
# can persistantly  get data for perpetual futures
class FtxClient:
    _ENDPOINT = 'https://ftx.com/api/'
    def __init__(self, api_key=None, api_secret=None, subaccount_name=None) -> None:
        self._session = Session()
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)
    
    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._ENDPOINT + path, **kwargs)
        self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self._subaccount_name)

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    def get_all_trades(self, market_name: str,resolution: int=None, start_dt: float = None, end_dt: float = None) -> List:
        limit = 80
        results = []
        while True:
            response = self._get('indexes/{}/candles?resolution={}'.format(market_name, resolution),
            {
            'end_time': end_dt,
            'start_time': start_dt,
           })
            results.extend(response)
            if len(response) == 0:
                break            
            end_dt = min(parse_datetime(t['startTime']) for t in response).timestamp()
            print(end_dt)
            if len(response) < limit:
                break
        return results

# Extract or get pertual future data for either BTC OR ETH (this is just wrapper function)
def FTX_perpetual_future_data(market_name, start_dt, end_dt):
    ts = int(time.time() * 1000)
    # 5 minute intervals times seconds
    resolution = 5*60
    # Create instance of FTX class
    pagnition_client=FtxClient(api_key= 'YUb-D2dOwqGb3vsFptFWxwpT2cGRToolaDZXJ-Qk', api_secret='Vn1L9kX75JfFbI-J_2ibJ_JxXH2p5DUckSGmn7L2')
    results= pagnition_client.get_all_trades(market_name, resolution, start_dt.timestamp(), end_dt.timestamp())
    # Historical perpetual future endpoint
    df = pd.DataFrame(results)
    # Remove +00:00
    df['startTime'] = df['startTime'].str[:-6]
    df['startTime'] =  pd.to_datetime(df['startTime'], format='%Y-%m-%d %H:%M:%S')
    df=df.sort_values(by='startTime')
    return df

def spot_prices_ohlcv(dt, pair, period='1d'):
    ohlcv = []
    limit = 1000
    if period == '1d':
        limit = 365
    elif period == '1h':
        limit = 24
    elif period == '5m':
        limit = 288
    for i in dt:
        start_dt = datetime.strptime(i, "%Y%m%d")
        since = calendar.timegm(start_dt.utctimetuple())*1000
        ohlcv.extend(ftx.fetch_ohlcv(symbol=pair, timeframe=period, since=since, limit=limit))
    df = pd.DataFrame(ohlcv, columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['Time'] = [datetime.fromtimestamp(float(time)/1000) for time in df['Time']]
    df['Open'] = df['Open'].astype(np.float64)
    df['High'] = df['High'].astype(np.float64)
    df['Low'] = df['Low'].astype(np.float64)
    df['Close'] = df['Close'].astype(np.float64)
    df['Volume'] = df['Volume'].astype(np.float64)
    return df

if PERPETUAL_FUTURES:
    if EXTRACT_DATA:
        # 2 hours ahead 
        # Start and end date: 
        start_day = "2020-05-24  00:00:00"
        start_dt = datetime.strptime(start_day, "%Y-%m-%d %H:%M:%S")
        end_day = "2021-05-24  00:00:00"
        end_dt = datetime.strptime(end_day, "%Y-%m-%d %H:%M:%S")
        # Just comment out the one you do not want
        market_name = 'BTC'
        # market_name = 'ETH'
        df = FTX_perpetual_future_data(market_name, start_dt, end_dt)
        if market_name == "ETH":
            df.to_csv('eth_usd_futures_5m_2020MayTo2021May.csv')
        elif market_name == "BTC":
            df.to_csv('btc_usd_futures_5m_2020MayTo2021May.csv')
        else:
            print('not defined')
    else:
        # 5m ETH/USD and BTC/USD perpetual future ohlcv data between 24 May 2020 and 24 May 2021 from crypto exchange ftx
        df_eth_futures=pd.read_csv('eth_usd_futures_5m_2020MayTo2021May.csv')
        df_btc_futures=pd.read_csv('btc_usd_futures_5m_2020MayTo2021May.csv')      
else:
    ftx = ccxt.ftx()
    if EXTRACT_DATA:
        # 2 hours ahead 
        # Start and end date: 
        start_day = "20200524"
        start_dt = datetime.strptime(start_day, "%Y%m%d")
        end_day = "20210524"
        end_dt = datetime.strptime(end_day, "%Y%m%d")
        # Just comment out the one you don't want to extract the data
        trading_pair = 'BTC/USD'
        # trading_pair = 'ETH/USD'
        days_num = (end_dt - start_dt).days + 1
        datelist = [start_dt + timedelta(days=x) for x in range(days_num)]
        datelist = [date.strftime("%Y%m%d") for date in datelist]
        df = spot_prices_ohlcv(datelist, trading_pair, '5m')
        if trading_pair ==  'BTC/USD':
            df.to_csv('btc_usd_futures_5m_2020MayTo2021May.csv')
        elif trading_pair == 'ETH/USD':
            df.to_csv('eth_usd_futures_5m_2020MayTo2021May.csv')
        else:
            print('not defined')
    else:
       # 5m ETH/USD and BTC/USD ohlcv data between 24 May 2020 and 24 May 2021 from crypto exchange ftx
        df_eth=pd.read_csv('eth_usd_5m_2020MayTo2021May.csv')
        df_btc=pd.read_csv('btc_usd_5m_2020MayTo2021May.csv')

# Sharp Ratio
def sharp_ratio(df):
    rfr = 0
    returns = df['Close']
    sharpe_ratio = ((returns.mean() - rfr) / returns.std()) * np.sqrt((252*288)**0.5)
    return sharpe_ratio

# Sorintino Ratio
def sortino_ratio(df):
    rfr=0
    target_return=0
    df = pd.DataFrame(df['Close'].pct_change())
    negative_returns = df.loc[df['Close']<target_return]
    expected_return = df['Close'].mean()
    down_stdev = negative_returns.std()
    sorintino_ratio = (expected_return-rfr/down_stdev)* np.sqrt((252*288)**0.5)
    return sorintino_ratio

def information_ratio(df_btc, df_eth):
    benchmark_returns = df_eth['Close']
    returns = df_btc['Close']
    return_difference = returns - benchmark_returns
    volatility = return_difference.std()
    information_ratio = return_difference.mean() / volatility
    return information_ratio* np.sqrt((252*288)**0.5)

def tracking_error(df, roll=False):
    if not roll:
        tracking_error = np.std((df['Close'].values - df['Close_FUT'].values) / df['Close_FUT'].values)
    else:
        tracking_error = np.std((df['RollingMeanVal_Close'].values - df['RollingMeanVal_Close_FUT'].values) / df['RollingMeanVal_Close_FUT'].values)
    return tracking_error

if CALCULATE_RATIOS:
    information_ratio = information_ratio(df_btc, df_eth)
    print(f'Information ratio: {information_ratio}')
    sharp_ratio_btc = sharp_ratio(df_btc)
    sortino_ratio_btc = sortino_ratio(df_btc)
    print(f'Sharp ratio BTC: {sharp_ratio_btc}')
    print(f'Sortino ratio BTC: {sortino_ratio_btc}')
    sharp_ratio_eth = sharp_ratio(df_eth)
    sortino_ratio_eth = sortino_ratio(df_eth)
    print(f'Sharp ratio ETH: {sharp_ratio_eth}')
    print(f'Sortino ratio ETH: {sortino_ratio_eth}')
else:
    df_eth = pd.read_csv('eth_usd_5m_2020MayTo2021May.csv')
    df_btc = pd.read_csv('btc_usd_5m_2020MayTo2021May.csv')
    df_eth_futures = pd.read_csv('eth_usd_futures_5m_2020MayTo2021May.csv')
    df_btc_futures = pd.read_csv('btc_usd_futures_5m_2020MayTo2021May.csv') 
    df_btc_futures = df_btc_futures.rename(columns={'startTime': 'Time', "close": 'Close_FUT'})
    df_eth_futures = df_eth_futures.rename(columns={'startTime': 'Time', "close": 'Close_FUT'})
    df_btc_new = pd.merge(df_btc, df_btc_futures, on="Time")
    df_eth_new = pd.merge(df_eth, df_eth_futures, on="Time")
    btc_tracking_error_perpetual_future = tracking_error(df_btc_new)
    eth_tracking_error_perpetual_future = tracking_error(df_eth_new)
    print('Tracking Error Bitcoin: ', btc_tracking_error_perpetual_future)
    print('Tracking Error Ethereum: ', eth_tracking_error_perpetual_future)

    df_eth_new['Time'] = pd.to_datetime(df_eth_new['Time'], format='%Y-%m-%d %H:%M:%S')
    df_eth_new = df_eth_new.set_index('Time')
    # 7 day rolling window ETH
    df_eth_new['Close_FUT'] = df_eth_new['Close_FUT'].astype('float')
    df_eth_new['RollingMeanVal_Close_FUT'] = df_eth_new['Close_FUT'].rolling('7d').mean()
    df_eth_new['RollingMeanVal_Close'] = df_eth_new['Close'].rolling('7d').mean()
    eth_7d_tracking_error_perpetual_future = tracking_error(df_eth_new, roll=True)

    df_btc_new['Time'] = pd.to_datetime(df_btc_new['Time'], format='%Y-%m-%d %H:%M:%S')
    df_btc_new = df_btc_new.set_index('Time')
    # 7 day rolling window BTC
    df_btc_new['Close_FUT'] = df_btc_new['Close_FUT'].astype('float')
    df_btc_new['RollingMeanVal_Close_FUT'] = df_btc_new['Close_FUT'].rolling('7d').mean()
    df_btc_new['RollingMeanVal_Close'] = df_btc_new['Close'].rolling('7d').mean()
    btc_7d_tracking_error_perpetual_future = tracking_error(df_btc_new, roll=True)
    print('7 day rolling window tracking error Bitcoin: ', btc_7d_tracking_error_perpetual_future)
    print('7 day rolling window tracking error Ethereum: ', eth_7d_tracking_error_perpetual_future)
