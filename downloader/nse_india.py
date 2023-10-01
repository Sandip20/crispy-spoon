# pylint: disable=broad-exception-caught
# https://www.youtube.com/watch?v=dJPLfWXHupE&ab_channel=TradeViaPython
import os
import time
from datetime import date,datetime
import requests
from nsepythonserver import *
import pandas as pd


pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 5000)

class NSE:
    pre_market_categories = ['NIFTY 50', 'Nifty Bank']
    
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}
        self.session = requests.Session()
        self.session.get('https://www.nseindia.com', headers=self.headers)
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.request_count=1

    def get_history(self, symbol, from_date, to_date, expiry_date,option_type='NA',strike_price="0.00"):
        """
        symbol: Nse Symbol
        from_date:datetime
        to_date: datetime 
        expiry_date:datetime
        
        """
        
        columns=['Settle Price','Open','High','Low','Close','Last_Traded_Price','Prev_Close','Lot_Size']
        df_columns=['FH_SYMBOL', 'FH_EXPIRY_DT', 'FH_SETTLE_PRICE', 'FH_OPENING_PRICE',
                            'FH_TRADE_HIGH_PRICE', 'FH_TRADE_LOW_PRICE',
                            'FH_LAST_TRADED_PRICE', 'FH_PREV_CLS','FH_CLOSING_PRICE','FH_TIMESTAMP','FH_MARKET_LOT']
        df_headers={
            'FH_SYMBOL': 'Symbol',
            'FH_EXPIRY_DT': 'Expiry',
            'FH_OPENING_PRICE': 'Open',
            'FH_TRADE_HIGH_PRICE': 'High',
            'FH_TRADE_LOW_PRICE': 'Low',
            'FH_CLOSING_PRICE': 'Close',
            'FH_SETTLE_PRICE': 'Settle Price',
            'FH_TIMESTAMP': 'Date',
            'FH_LAST_TRADED_PRICE': "Last_Traded_Price",
            'FH_PREV_CLS': 'Prev_Close',
            'FH_MARKET_LOT':'Lot_Size'
            }
        url = 'https://www.nseindia.com/api/historical/fo/derivatives'
        params = {
            'symbol': symbol.replace(' ', '%20').replace('&', '%26'),
            'from': from_date.strftime('%d-%m-%Y'),
            'to': to_date.strftime('%d-%m-%Y'),
            'expiryDate': expiry_date.strftime('%d-%b-%Y')
        }
        if option_type in ['CE','PE']:
            instrumentType='options'
            params.update({
                'instrumentType': 'OPTSTK',
                'strikePrice': strike_price,
                'optionType': option_type
            })
        else:
            instrumentType='futures'
            params.update({
                'instrumentType': 'FUTSTK'
            })
        try:
          
            # if self.request_count % 100 == 0:
            #     time.sleep(5) # delay for 5 seconds after every 100 requests
            # self.request_count += 1
            # response =  self.session.get(url, params=params, headers=self.headers)
            # data = response.json()
            df= derivative_history(symbol,from_date.strftime("%d-%m-%Y"),to_date.strftime("%d-%m-%Y"),instrumentType,expiry_date.strftime('%d-%b-%Y'),int(float(strike_price)),option_type)
            # if not data['data']:
            #     return pd.DataFrame()
            # df = pd.DataFrame(data['data'])
            if option_type in ['CE','PE']:
                df_columns.extend(['FH_STRIKE_PRICE','FH_OPTION_TYPE'])
                df = df[df_columns]
                df = df.rename(columns={'FH_STRIKE_PRICE':'Strike Price','FH_OPTION_TYPE':'Option Type'})
                df['Strike Price'] = pd.to_numeric(df['Strike Price'])
            else:
                df = df[df_columns]
            df=df.rename(columns=df_headers)
            df[columns] = df[columns].astype(float)
            df = df.set_index('Date', drop=True)
            return df
        except Exception as e:
            print("error:",e)
            # today = date.today().strftime('%Y-%m-%d')
            # if os.path.isfile(today + '.txt'):
            #     # Open the file in "append" mode
            #     file = open(today + '.txt', 'a')
            # else:
            #     # Create the file in "write" mode
            #     file = open(today + '.txt', 'w')
            # file.write(f'{response.url}\n')
            # file.close()
            
    def get_nse_holidays(self):
       url='https://www.nseindia.com/api/holiday-master?type=trading'
       try:
        response=self.session.get(url=url,headers=self.headers)
        data=response.json()
        return data['FO']
       
       except Exception as e:
           print('error',e)
    
    def get_expiry_date(self,year,month):
            from_date=date(year,month,1).strftime('%d-%m-%Y')
            to_date=date.today().strftime('%d-%m-%Y')
            symbol = 'TATAMOTORS'
            print(from_date)
            print(to_date)
        
            url=f'https://www.nseindia.com/api/historical/fo/derivatives/meta?from={from_date}&to={to_date}&instrumentType=FUTSTK&symbol={symbol}'
            print(url)
            try:
                
                response=self.session.get(url,headers=self.headers).json()
                date_string =  response['years'][to_date.split('-')[2]][0]
                date_format = '%d-%b-%Y'
                return datetime.strptime(date_string, date_format).date()
            
            except Exception as e:
                print('could not get Expiry',e)
    def get_vix(self,from_date,to_date):
        from_date=from_date.strftime('%d-%m-%Y')
        to_date=to_date.strftime('%d-%m-%Y')
        url=f'https://www.nseindia.com/api/historical/vixhistory?from={from_date}&to={to_date}'
  
        columns_mapper= {
 		"EOD_TIMESTAMP": 'Date',
 		"EOD_INDEX_NAME":"Index_Name",
 		"EOD_OPEN_INDEX_VAL": 'Open',
 		"EOD_CLOSE_INDEX_VAL":'Close',
 		"EOD_HIGH_INDEX_VAL":'High',
 		"EOD_LOW_INDEX_VAL":'Low',
 		"EOD_PREV_CLOSE": 'Prev_Close',
 		"VIX_PTS_CHG": 'Points_Change',
 		"VIX_PERC_CHG":'Percent_Change',
        'TIMESTAMP':'TIMESTAMP'
 	}
        try:
            
            response=self.session.get(url,headers=self.headers).json()
            df=pd.DataFrame(response['data'],columns=columns_mapper).rename(columns=columns_mapper)

            return df
        
        except Exception as e:
            print('could not get Vix',e)

    
nse = NSE() 
nse.get_history(symbol='SBIN',from_date= date(2023,8,1), to_date= date(2023,8,21),expiry_date=date(2023,8,31), option_type='CE',strike_price="570.00")

