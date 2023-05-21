# pylint: disable=broad-exception-caught
# https://www.youtube.com/watch?v=dJPLfWXHupE&ab_channel=TradeViaPython
import os
import time
from datetime import date, datetime, timedelta
import requests
import pandas as pd

from data.constants import DATE_FORMAT, DATE_FORMAT_B, NSE_HOST


pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 5000)


class NSE:
    """
   NSE class will be responsible for the connection with nse and  it has methods of 

   get_nse_holidays
   get_history
   get_expiry_date

    """
    pre_market_categories = ['NIFTY 50', 'Nifty Bank']

    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.get(NSE_HOST, headers=self.headers)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100, pool_maxsize=100)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.request_count = 1

    def get_history(self, symbol: str, from_date: datetime, to_date: datetime, expiry_date: datetime, option_type='NA', strike_price="0.00"):
        """
        symbol: Nse Symbol
        from_date:datetime
        to_date: datetime 
        expiry_date:datetime

        """

        columns = ['Settle Price', 'Open', 'High', 'Low', 'Close',
                   'Last_Traded_Price', 'Prev_Close', 'Lot_Size']
        df_columns = ['FH_SYMBOL', 'FH_EXPIRY_DT', 'FH_SETTLE_PRICE', 'FH_OPENING_PRICE',
                      'FH_TRADE_HIGH_PRICE', 'FH_TRADE_LOW_PRICE',
                      'FH_LAST_TRADED_PRICE', 'FH_PREV_CLS', 'FH_CLOSING_PRICE', 'FH_TIMESTAMP', 'FH_MARKET_LOT']
        df_headers = {
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
            'FH_MARKET_LOT': 'Lot_Size'
        }
        url = f'{NSE_HOST}/api/historical/fo/derivatives'
        params = {
            'symbol': symbol.replace(' ', '%20').replace('&', '%26'),
            'from': from_date.strftime(DATE_FORMAT),
            'to': to_date.strftime(DATE_FORMAT),
            'expiryDate': expiry_date.strftime(DATE_FORMAT_B)
        }
        if option_type in ['CE', 'PE']:
            strike_price = f'{float(strike_price):.2f}'

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
                time.sleep(5)  # delay for 5 seconds after every 100 requests
            self.request_count += 1
            response = self.session.get(
                url, params=params, headers=self.headers)
            data = response.json()
            print(response.url)
            if not data['data']:
                return pd.DataFrame()
            df = pd.DataFrame(data['data'])
            if option_type in ['CE', 'PE']:
                df_columns.extend(['FH_STRIKE_PRICE', 'FH_OPTION_TYPE'])
                df = df[df_columns]
                df = df.rename(
                    columns={'FH_STRIKE_PRICE': 'Strike Price', 'FH_OPTION_TYPE': 'Option Type'})
                df['Strike Price'] = pd.to_numeric(df['Strike Price'])
            else:
                df = df[df_columns]
            df = df.rename(columns=df_headers)
            df[columns] = df[columns].astype(float)
            df = df.set_index('Date', drop=True)
            return df
        except Exception as _e:
            print("error:", _e)
            today = datetime.today().strftime('%Y-%m-%d')
            if os.path.isfile(today + '.txt'):
                # Open the file in "append" mode
                _file = open(today + '.txt', 'a')
            else:
                # Create the file in "write" mode
                _file = open(today + '.txt', 'w')
            _file.write(f'{response.url}\n')
            _file.close()

    def get_nse_holidays(self):
        """
        Get the list of NSE trading holidays.

        Returns:
        - A list of dates representing NSE trading holidays.
        - If an error occurs during the HTTP request, None is returned.
        """
        url = f'{NSE_HOST}/api/holiday-master?type=trading'
        try:
            response = self.session.get(url=url, headers=self.headers)
            data = response.json()
            return data['FO']
        except Exception as _e:
            print('Error occurred while fetching NSE holidays:', _e)
            return None

    def get_expiry_date(self, year: int, month: int, day: int = 1):
        """Get the expiry date of a futures contract for the specified year and month.

        Args:
            year (int): The year for the contract expiry date.
            month (int): The month for the contract expiry date, 
                represented as a number from 1 to 12.

        Returns:
            datetime.date: The expiry date of the futures contract for the specified year and month.

        Raises:
            Exception: If the expiry date cannot be retrieved from the NSE website.
        """
        symbol = 'TATAMOTORS'
        input_date=date(year, month, day)
        if input_date.strftime('%A')=='Saturday':
          input_date-=timedelta(days=1)
        elif input_date.strftime('%A')=='Sunday':
            input_date-=timedelta(days=2)
        from_date = input_date.strftime(DATE_FORMAT)
        to_date = from_date
        # date.today().strftime(DATE_FORMAT)
        url = f'{NSE_HOST}/api/historical/fo/derivatives/meta?from={from_date}&to={from_date}&instrumentType=FUTSTK&symbol={symbol}'
        try:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            json_data = response.json()
           
            return datetime.strptime(json_data['years'][to_date.split('-')[2]][0], DATE_FORMAT_B).date()
            # for exp_date in json_data['years'][to_date.split('-')[2]]:
            #     if datetime.strptime(exp_date, DATE_FORMAT_B).date() >= datetime.strptime(from_date, DATE_FORMAT).date():
            #         date_string = exp_date
            #         break
            # return datetime.strptime(date_string, DATE_FORMAT_B).date()

        except (requests.exceptions.HTTPError, KeyError, IndexError, ValueError) as error:
            raise Exception('Could not get expiry date from NSE website.') from error
