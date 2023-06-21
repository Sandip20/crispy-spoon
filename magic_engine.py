# pylint: disable=broad-exception-caughtNSEDownloader
""" 
OptionWizard class  is main class which will  import neccessary modules and 
calls respective methods
"""
import asyncio
import os
from datetime import timedelta, datetime, date
import time
from typing import Optional, Union
import pandas as pd
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

from data.constants import EXCLUSIONS, NO_OF_WORKING_DAYS_END_CALCULATION
from data.fno_downloader import FNODownloader
from data.mongodb import Mongo
from data.nse_downloader import NSEDownloader
from data.order_manager import OrderManager
from data.process import ProcessData
from data.queries.mongo_queries_processed_options import create_find_cheapest_options_query
from data.telegram import Telegram
from data.util import add_working_days, get_last_business_day, get_next_business_day, is_holiday

load_dotenv()

class OptionWizard:
    """
    This class is responsible for  all the magic happening in the project.
    """

    def __init__(self) -> None:
        self.tg_api_token = os.environ['TG_API_TOKEN']
        self.tg_chat_id = os.environ['TG_CHAT_ID']
        base_url = f"mongodb+srv://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}"
        url = f"{base_url}:27017/?retryWrites=true&w=majority" if os.environ[
            'MONGO_INITDB_HOST'] == "localhost" else f"{base_url}/?retryWrites=true&w=majority"
        # connection with mongodb
        self.mongo = Mongo(
            url=url, db_name=os.environ['MONGO_INITDB_DATABASE'], is_ca_required=True)
        self.nse_downloader = NSEDownloader()
        self.process_data = ProcessData(self.nse_downloader, self.mongo)
        self.telegram = Telegram(
            os.environ['TG_API_TOKEN'], os.environ['TG_CHAT_ID'])
      
        self.last_accessed_date_fut = self.get_last_accessed('fut')
        self.last_accessed_date_opt = self.get_last_accessed('opt')
        self.get_tickers()
        self.holidays = self.nse_downloader.get_nse_holidays()
        self.fno_downloader = FNODownloader(
            nse_downloader = self.nse_downloader,
            mongo = self.mongo,
            df_dict = self.df_dict,
            tickers = self.tickers,
            holidays=self.holidays
            )
        self.order_manager= OrderManager(mongo=self.mongo)

    def get_last_accessed(self, instrument: str) -> datetime.date:
        """
        gets last accessed date of the instrumenttype from activity collection

        Args:
            (instrument:str) that represents  opt or fut

        Returns:
            lastaccessed date of the instrument
        """
        activity = self.mongo.find_one({'instrument': instrument}, 'activity')
        return activity['last_accessed_date']

    def get_tickers(self):
        """
            This will get all tickers from stock_step collection
            and set the 
            df_dict,
            lot_size{ticker:lot_size}
            tickers:list[]

        """
        df_dict = {}
        symbol_lot_details = {}
        tickers = []

        for item in self.mongo.find_many({}, 'stocks_step'):
            df_dict[item['Symbol']] = float(item['step'])
            symbol_lot_details[item['Symbol']
                               ] = item['lot_size'] if 'lot_size' in item.keys() else 0
            tickers.append(item['Symbol'])

        self.df_dict = df_dict
        self.lot_size = symbol_lot_details
        self.tickers = tickers

    def find_cheapest_options(
        self,
        n: int,
        input_date: Optional[Union[str,date]] = None,
        no_of_days_back: Optional[int] = False,
        back_test: Optional[bool] = False
    ) -> dict:
        """
        Finds the n cheapest straddle options for a given date or a specified number of days back.

        Args:
            n (int): The number of cheapest straddle options to find.
            input_date (str or date, optional): The date for which to find the cheapest straddle options. If not provided,
                defaults to the latest date for which straddle options are available in the database.
            no_of_days_back (int, optional): The number of days back from the specified date to search for the cheapest straddle options.
            back_test (bool, optional): Flag to indicate whether the function is being called during a back test. Defaults to False.

        Returns:
            A dictionary containing the specified date and the n cheapest straddle options for that date.

        Raises:
            ValueError: If an invalid input date is provided.

        """
        if input_date:
            if isinstance(input_date, str):
                today = datetime.fromisoformat(input_date)
            elif isinstance(input_date, date):
                today = input_date
            else:
                raise ValueError('Invalid input_date. Must be either a string in YYYY-MM-DD format or a datetime object.')
        else:
            latest_doc = self.mongo.find_one(
                filter={},
                collection=os.environ['STRADDLE_COLLECTION_NAME'],
                sort=[('Date', -1)]
            )
            if latest_doc is None:
                print('Something went wrong. The straddle collection is empty.')
                return {}
            today = latest_doc['Date']

        if no_of_days_back:
            today -= timedelta(days=no_of_days_back)
        elif today.strftime('%A') in EXCLUSIONS:
            days = EXCLUSIONS.index(today.strftime('%A')) + 1
            today -= timedelta(days=days)
        elif is_holiday(today, self.holidays) or back_test:
            recent_trading_day: datetime = get_last_business_day(today, self.holidays)
            today = recent_trading_day
        query = create_find_cheapest_options_query(today, n)

        return {'day': today, 'cheapest_options': self.mongo.aggregate(query, os.environ['STRADDLE_COLLECTION_NAME'])}

    def get_trade_date(self, today):
        """
            today:datetime
            Returns:
                business day for the trade to place
        """
        return get_next_business_day(today,  self.holidays, days=5)

    def send_to_telegram(self, cheapest_records, today):
        """
        Sends the cheapest option records for a given date to a telegram channel.

        Parameters:
        cheapest_records (list): A list of dictionaries containing information about the cheapest options.
        today (datetime): The date for which the cheapest options are being sent to telegram.

        Returns:
        None
        """
        
        trade_date = self.get_trade_date(today)
        self.telegram.send_to_telegram(cheapest_records, trade_date)
   
    def get_portfolio_pnl(self, initial_capital:float,slippage:float,brokerage:float) -> dict:
        """
        Returns the profit and loss (PNL) of the portfolio and the total capital used in the portfolio.
        
        Parameters:
            initial_capital (float): The initial capital used to trade the portfolio.
        
        Returns:
            portfolio_pnl (dict): A dictionary containing the portfolio's PNL, total capital, and symbol-wise PNL data.
        """

        portfolio_pnl = {
            'pnl': 0,
            'total_capital': initial_capital,
            'used_capital': 0,
            'symbols': {}
        }

        orders = self.mongo.find_many({}, os.environ['ORDERS_COLLECTION_NAME'])

        for order in orders:
            symbol = order['symbol']
            strike = order['strike']
            created_at = order['created_at']
            price = order['price']

            end = add_working_days(
                created_at, NO_OF_WORKING_DAYS_END_CALCULATION, self.holidays)
            
            one_day_before = pd.to_datetime(date.today())-timedelta(days=1)
            if end > one_day_before:
                end = one_day_before
        

            data = self.mongo.find_many(
                {'Symbol': symbol, 'Strike Price': strike, 'Date': {'$lte': end}},
                os.environ['OPTIONS_COLLECTION_NAME'],
                sort=[('Date', -1)],
                limit=2
            )
            current_price = float(data[0]['Close']) + float(data[1]['Close'])
            quantity = data[0]['Lot_Size']
            dte=(data[0]['Expiry']-data[0]['Date']).days

            # Calculate slippage and brokerages
            slippage_cost=slippage*quantity
            brokerage_cost=brokerage


            # Calculate PNL considering slippage and brokerages
            pnl=(current_price - price) * quantity - (slippage_cost+ brokerage_cost)
            symbol_data = {
                'symbol':symbol,
                'quantity': quantity,
                'strike': strike,
                'created_at': created_at,
                'buy_price': price,
                'current_price': current_price,
                'capital': round(price * quantity, 2),
                'pnl': round(pnl, 2),
                'expiry':data[0]['Expiry'],
                'exit_date':data[0]['Date'],
                'dte':dte
            }

            portfolio_pnl['symbols'][symbol] = symbol_data
            portfolio_pnl['dte']= dte
            portfolio_pnl['pnl'] += symbol_data['pnl']
            portfolio_pnl['used_capital'] += symbol_data['capital']
            portfolio_pnl['total_capital'] -= portfolio_pnl['used_capital']

        return portfolio_pnl

    def update_daily(self):
        """
        Update daily futures and options data and perform required data processing.

        Returns:
            None
        """
        start_time = time.time()
        if self.last_accessed_date_fut.strftime('%A') in EXCLUSIONS and date.today().strftime('%A') in EXCLUSIONS :
            print(f"you are running script on sunday for which no data is available your recent update is on : {self.last_accessed_date_fut} ")
            return
        asyncio.run(
            self.fno_downloader.update_futures_data(self.last_accessed_date_fut,None,None))

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        self.mongo.update_one(
            {'last_accessed_date': self.last_accessed_date_fut, 'instrument': 'fut'},
            {'last_accessed_date': pd.to_datetime(date.today())},
            'activity'
        )
        print("--------------futures updated------------")

        try:
            start_time = time.time()
         
            asyncio.run(
            self.fno_downloader
            .download_historical_options(None, None,self.last_accessed_date_opt)
            )
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Execution time: {execution_time} seconds")
        # update the last accessed date of updates
            self.mongo.update_one(
                {'last_accessed_date': self.last_accessed_date_opt, 'instrument': 'opt'},
                {'last_accessed_date': pd.to_datetime(date.today())},
                'activity'
            )
        except Exception as _e:
            print(f"An error occurred while downloading historical options: {_e}")
        
        start_date = pd.to_datetime(date.today())
        self.process_data.add_ce_pe_of_same_date(
            start_date=start_date, end_date=start_date)
        print('data processing')
        self.process_data.update_current_vs_prev_two_months(
            today=True).to_csv('current.csv')
        print('CSV generated')

    def download_historical(self,start_date,end_date):
        self.fno_downloader.download_historical(start_date,end_date)
        self.process_data.update_current_vs_prev_two_months(start_date,end_date)

    def update_stocks_info(self):
        """
            Updates the stocks information by downloading and processing strike info and lot info data.

            This function downloads the strike info and lot info files, cleans the data, merges the datasets,
            and updates the stocks_step collection in the database with the updated information.

            Returns:
            None
        """
        # Define the file names and paths
        strike_info_path = os.path.join('files', os.environ['STRIKE_INFO_NAME'])
        lot_info_path = os.path.join('files', os.environ['LOT_INFO_NAME'])

        # Download the files
        self.nse_downloader.download_file(strike_info_path)
        self.nse_downloader.download_file(lot_info_path)

        # Load and clean the strike info data
        strike_info_df = pd.read_excel(strike_info_path, header=2, usecols=range(4))
        strike_info_df.columns = ['Symbol', 'step', 'no_of_strikes', 'additional_strikes_enabled_intraday']

        # Load and clean the lot info data
        lot_info_df = pd.read_csv(lot_info_path, skiprows=[0, 1, 2, 3, 4], usecols=[1, 2], names=['Symbol', 'lot_size'])
        lot_info_df = lot_info_df.apply(lambda x: x.str.strip()).dropna()
        lot_info_df['lot_size'] = lot_info_df['lot_size'].astype(int)

        # Merge the data frames
        merged_df = strike_info_df.merge(lot_info_df, on="Symbol", how="left")

        # Update the database
        self.mongo.delete_many({},os.environ['STOCK_STEP_COLLECTON_NAME'])
        # self.stocks_step.delete_many({})
        self.mongo.insert_many(merged_df.to_dict('records'),os.environ['STOCK_STEP_COLLECTON_NAME'])
