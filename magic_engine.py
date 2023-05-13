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
from pymongo import InsertOne
from data.constants import EXCLUSIONS, NO_OF_WORKING_DAYS_END_CALCULATION
from data.mongodb import Mongo
from data.nse_downloader import NSEDownloader
from data.process import ProcessData
from data.queries.mongo_queries_processed_options import create_find_cheapest_options_query
from data.telegram import Telegram
from data.util import add_working_days, data_frame_to_dict, get_last_business_day, get_next_business_day, get_week, get_strike, is_holiday

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

    async def update_futures_data(self):
        """
        updates the futures data till date from last_accessed_date_fut inclusive
        """
        last_accessed_date_fut = self.last_accessed_date_fut

        if pd.to_datetime(date.today()).date() == pd.to_datetime(last_accessed_date_fut).date():
            print('Data is already updated')
            return
        start = pd.to_datetime(last_accessed_date_fut).date()
        to_today = date.today()

        expiry_date = self.nse_downloader.get_expiry(
            start.year, start.month, start.day)
        expiry_dates = [expiry_date]

        if to_today > expiry_date:
            new_date = expiry_date + timedelta(1)
            expiry_date = self.nse_downloader.get_expiry(
                new_date.year, new_date.month, new_date.day)
            expiry_dates.append(expiry_date)
        for idx, expiry_date in enumerate(expiry_dates):
            if idx != 0:
                start = expiry_dates[idx-1]
                end_date = to_today
            else:
                end_date = expiry_date if to_today > expiry_date else to_today

            tasks = []
            for ticker in self.tickers:
                tasks.append(asyncio.ensure_future(
                    self._update_futures_data(ticker, start, end_date, expiry_date)))
        await asyncio.gather(*tasks)

    async def _update_futures_data(self, ticker, start, end, expiry_date):

        """
        event loop will call  _update_futures_data_v3 of nse_downloader.
        download the data for given input mongo insertmany  to insert into  stock_futures
        """
        try:
            ohlc_fut = await self.nse_downloader.update_futures_data(ticker, start, end, expiry_date)
            if not ohlc_fut.empty:
                data = data_frame_to_dict(ohlc_fut)
                self.mongo.insert_many(data, 'stock_futures')
        except Exception as e:
            print(f"Error downloading Futures data for {ticker}: {e}")

    async def _download_historical_options_v3(self, symbol: str, s_date, end_date: datetime, expiry_date, strike_price, fut_close, option_type):
        """
            symbol: str, 
            s_date, 
            end_date: datetime, 
            expiry_date, 
            strike_price, 
            fut_close, 
            option_type
        """

        try:
            option_ohlc = await self.nse_downloader.download_historical_options(
                symbol, s_date, end_date, expiry_date, strike_price, fut_close, option_type
            )
            option_ohlc['weeks_to_expiry'] = option_ohlc['days_to_expiry'].apply(
                get_week)
            self.mongo.insert_many(data_frame_to_dict(
                option_ohlc), 'atm_stock_options')
        except Exception as _e:
            print(f"Error downloading Options data for {symbol}: {_e}")

    async def download_historical_options(self, start_date, end_date, update_daily=True):
        """
            start_date: A datetime.date object representing the start date for which options data needs to be downloaded.
            end_date: A datetime.date object representing the end date for which options data needs to be downloaded.
            update_daily: A boolean value indicating whether to update daily data or not.
        """
        self.request_count = 2
        ohlc_futures = []
        if update_daily:
            ohlc_futures = self.mongo.find_many(
                {"Date": {"$gte": pd.to_datetime(self.last_accessed_date_opt)}
                 },
                'stock_futures'
            )
        else:
            prev_month = start_date.month - 1 or 12
            year = start_date.year - (prev_month == 12)

            prev_expiry = self.nse_downloader.get_expiry(
                year, prev_month) + pd.Timedelta(days=1)
            holidays = self.nse_downloader.get_nse_holidays()
            end_date = get_last_business_day(end_date, holidays)
            expiry_next = self.nse_downloader.get_expiry(
                end_date.year, end_date.month)
            ohlc_futures = self.mongo.find_many(
                {"Date": {"$gte": pd.to_datetime(
                    prev_expiry), "$lte": pd.to_datetime(expiry_next)}},
                'stock_futures'
            )

        step_dict = self.df_dict
        tasks = []
        filtered_ohlc_futures = [
            record for record in ohlc_futures
            if record['Symbol'] in self.tickers]

        for ohlc_fut in filtered_ohlc_futures:
            symbol = ohlc_fut["Symbol"]
            step = step_dict[symbol]
            expiry = ohlc_fut["Expiry"]
            #all dates of current month
            # get step of the sticker
            s_date = ohlc_fut['Date']
            close = float(ohlc_fut["Close"])
            strike_price = get_strike(close, step)
            for option_type in ["CE","PE"]:
                tasks.append(asyncio.ensure_future(
                    self._download_historical_options_v3(
                        symbol,
                        s_date,
                        expiry,
                        expiry,
                        strike_price,
                        float(ohlc_fut['Close']),
                       option_type
                    )))
            if(self.request_count % 150 == 0):
                await asyncio.sleep(5)
            self.request_count += 2
        await asyncio.gather(*tasks)

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


    def update_record(self, record: dict, columns: list, date_of_trade: datetime) -> InsertOne:
        """
        Update a record and return an instance of pymongo InsertOne.
        
        Args:
        - record (dict): The record to be updated.
        - columns (list): The list of columns for the record.
        - date_of_trade (datetime): The date when the trade was made.
        
        Returns:
        - pymongo.InsertOne: An instance of pymongo InsertOne.
        """
        # Delete unnecessary keys
        del record['Date']
        del record['two_months_week_min_coverage']
        del record['current_vs_prev_two_months']

        # Add created_at and price keys
        record['created_at'] = date_of_trade
        record['price'] = record[columns[2]]
        
        return InsertOne(record)


    def create_order(self, cheapest_stocks):
        """
        Create orders for the given list of cheapest stocks.

        Parameters:
        -----------
        cheapest_stocks : list of dict
            List of cheapest stocks, where each item is a dictionary containing the following keys:
            - Symbol : str
            - Expiry : str
            - Strike Price : float
            - Option Type : str
            - LTP : float
            - Signal : str

        Returns:
        --------
        None
        """
        self.mongo.bulk_write(
            cheapest_stocks, os.environ['ORDERS_COLLECTION_NAME'])
        
    def place_orders(self, cheapest_records, trade_date):
        """
        This method places the orders for the cheapest_records.

        Args:
            cheapest_records (List[Dict]): A list of cheapest straddle options for each stock.
            trade_date (str): The date on which the trade is to be placed.

        Returns:
            None
        """
        columns = ['symbol', "strike", 'straddle_premium', "%coverage"]
        date_of_trade = pd.to_datetime(trade_date)

        # Check if the order is already placed for the given trade date
        result = self.mongo.find_one({'created_at': date_of_trade}, os.environ['ORDERS_COLLECTION_NAME'])
        if result is not None:
            return

        # Update the records with created_at and price fields
        cheapest_records = [self.update_record(record, columns, date_of_trade) for record in cheapest_records]

        # Place the orders
        self.create_order(cheapest_records)

        print(date_of_trade.strftime('%A'))


    def download_options_for_pnl(self, back_test=False):
        """
        download options for P&L
        """

        for order in self.mongo.find_many({}, os.environ['ORDERS_COLLECTION_NAME']):
            if back_test:
                end = add_working_days(
                    order['created_at'], NO_OF_WORKING_DAYS_END_CALCULATION, self.holidays)
                result = self.mongo.find_many(
                    {'Symbol': order['symbol'], "Date": {"$gte": pd.to_datetime(order['created_at']), "$lte": pd.to_datetime(end)},
                     'Strike Price': order['strike'], 'Expiry': order['expiry']}, os.environ['OPTIONS_COLLECTION_NAME'])
                if len(result) > 0 and 'Lot_Size' in result[0]:
                    continue
                data = self.nse_downloader.get_oneday_options_history(
                    ticker=order['symbol'],
                    opt_type='CE',
                    start_date=order['created_at'],
                    end_date=end,
                    expiry_date=order['expiry'],
                    strike=order['strike']
                )
                records = data_frame_to_dict(data)
                self.mongo.insert_many(
                    records, os.environ['OPTIONS_COLLECTION_NAME'])
                data = self.nse_downloader.get_oneday_options_history(
                    ticker=order['symbol'],
                    opt_type='PE',
                    start_date=order['created_at'],
                    end_date=end,
                    expiry_date=order['expiry'],
                    strike=order['strike']
                )
                records = data_frame_to_dict(data)
                self.mongo.insert_many(
                    records, os.environ['OPTIONS_COLLECTION_NAME'])

    def get_portfolio_pnl(self):
        """
        Returns the profit and loss (PNL) of the portfolio.
        """
        portfolio_pnl = {
            'pnl': 0,
            'total_capital': 0,
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
            data = self.mongo.find_many(
                {'Symbol': symbol, 'Strike Price': strike, 'Date': {'$lte': end}},
                os.environ['OPTIONS_COLLECTION_NAME'],
                sort=[('Date', -1)],
                limit=2
            )
            current_price = float(data[0]['Close']) + float(data[1]['Close'])
            quantity = data[0]['Lot_Size']

            symbol_data = {
                'quantity': quantity,
                'strike': strike,
                'created_at': created_at,
                'buy_price': price,
                'current_price': current_price,
                'capital': round(price * quantity, 2),
                'pnl': round((current_price - price) * quantity, 2)
            }

            portfolio_pnl['symbols'][symbol] = symbol_data
            portfolio_pnl['pnl'] += symbol_data['pnl']
            portfolio_pnl['total_capital'] += symbol_data['capital']

            print({
                'Symbol': symbol,
                'Strike Price': strike,
                'pnl': symbol_data['pnl'],
                'capital': symbol_data['capital']
            })

        return portfolio_pnl

    def close_week_orders(self):
        """
        Closes all active orders for the current week. Retrieves the relevant option data from the database and calculates
        the profit or loss made on the trades. Finally, inserts the details of the closed positions into a separate
        collection and deletes the orders from the orders collection.

        Returns:
            None
        """

        for order in self.mongo.find_many({}, os.environ['ORDERS_COLLECTION_NAME']):
            symbol = order['symbol']
            strike = order['strike']
            created_at = order['created_at']
            entry_price = order['price']
            query = {'Symbol': symbol, 'Strike Price': strike}
            data = self.mongo.find_many(
                query,
                os.environ['OPTIONS_COLLECTION_NAME'],
                sort=[('Date', -1)],
                limit=2)
            exit_price = round(
                float(data[0]['Close']), 2)+round(float(data[1]['Close']), 2)

            quantity = data[0]['Lot_Size']

            position = {}
            position['symbol'] = symbol
            position['strike'] = strike
            position['created_at'] = created_at
            position['exit_date'] = data[0]['Date']
            position['expiry'] = data[0]['Expiry']
            position['entry_price'] = round(entry_price, 2)
            position['exit_price'] = round(exit_price, 2)
            position['margin'] = round(entry_price*quantity, 2)
            position['profit_loss'] = round(
                (exit_price - entry_price) * quantity, 2)
            self.mongo.insert_one(
                position, os.environ['CLOSED_POSITIONS_COLLECTION_NAME'])
        self.mongo.delete_many({}, os.environ['ORDERS_COLLECTION_NAME'])

    def update_daily(self):
        """
        Update daily futures and options data and perform required data processing.

        Returns:
            None
        """
        start_time = time.time()
        asyncio.run(self.update_futures_data())

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
            start_date = pd.to_datetime(date.today())
            asyncio.run(self.download_historical_options(
                start_date, start_date))
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

        self.process_data.add_ce_pe_of_same_date(
            start_date=start_date, end_date=start_date)
        print('data processing')
        self.process_data.update_week_min_coverage()
        self.process_data.update_current_vs_prev_two_months(
            today=True).to_csv('current.csv')
        print('CSV generated')
