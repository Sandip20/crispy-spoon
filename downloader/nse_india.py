# pylint: disable=broad-exception-caught
# https://www.youtube.com/watch?v=dJPLfWXHupE&ab_channel=TradeViaPython
import requests
import pandas as pd
import time
from datetime import date,datetime
import os
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
            params.update({
                'instrumentType': 'OPTSTK',
                'strikePrice': strike_price,
                'optionType': option_type
            })
        else:
            params.update({
                'instrumentType': 'FUTSTK'
            })
        try:
            if self.request_count % 100 == 0:
                time.sleep(5) # delay for 5 seconds after every 100 requests
            self.request_count += 1
            response =  self.session.get(url, params=params, headers=self.headers)
            data = response.json()
            if not data['data']:
                return pd.DataFrame()
            df = pd.DataFrame(data['data'])
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
            today = datetime.today().strftime('%Y-%m-%d')
            if os.path.isfile(today + '.txt'):
                # Open the file in "append" mode
                file = open(today + '.txt', 'a')
            else:
                # Create the file in "write" mode
                file = open(today + '.txt', 'w')
            file.write(f'{response.url}\n')
            file.close()
            
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
# nse = NSE()
# df = nse.get_history(symbol='TATAMOTORS',from_date= '31-03-2023', to_date='15-04-2023', expiry_date='27-Apr-2023')
# print(df)
# df = nse.get_history(symbol='TCS',from_date= '31-03-2023', to_date='09-04-2023', expiry_date='27-Apr-2023',option_type='CE',strike_price="3240.00")
# print(df)