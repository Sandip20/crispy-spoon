# pylint: disable=broad-exception-caught
"""
Module responsible for downloading the F&O data
"""
import os
import asyncio
from datetime import timedelta, date, datetime
import time

from typing import List
import pandas as pd
from dateutil.relativedelta import relativedelta
from data.constants import EXCEPTION_DATE, MONTHS_IN_YEAR, NO_OF_WORKING_DAYS_END_CALCULATION
from data.util import add_working_days, data_frame_to_dict, get_last_business_day, get_strike, get_week
from data.mongodb import Mongo
from data.nse_downloader import NSEDownloader


class FNODownloader:
    """
    A class for downloading historical futures data from the National Stock Exchange (NSE) using an NSEDownloader instance 
    and saving it to a MongoDB instance.

    Attributes:
    ----------
    nse_downloader : NSEDownloader
        An instance of NSEDownloader for downloading data from the NSE.
    mongo : Mongo
        An instance of Mongo for saving data to a MongoDB database.
    """

    def __init__(self, nse_downloader: NSEDownloader, mongo: Mongo, df_dict, tickers: List,holidays:List) -> None:
        """
        Initializes a new instance of the FNODownloader class.

        Parameters:
        -----------
        nse_downloader : NSEDownloader
            An instance of NSEDownloader for downloading data from the NSE.
        mongo : Mongo
            An instance of Mongo for saving data to a MongoDB database.
        """
        self.nse_downloader = nse_downloader
        self.mongo = mongo
        self.df_dict = df_dict
        self.tickers = tickers
        self.holidays = holidays

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
        except Exception as _e:
            print(f"Error downloading Futures data for {ticker}: {_e}")

    async def _download_historical_options_v3(self, symbol: str, s_date, end_date: datetime,
                                              expiry_date, strike_price, fut_close, option_type):
        """
            symbol: str, 
            s_date, 
            end_date: datetime, 
            expiry_date, 
            strike_price, 
            fut_close, 
            option_type
        """
        option_ohlc={}
        try:
            option_ohlc = await self.nse_downloader.download_historical_options(
                symbol, s_date, end_date, expiry_date, strike_price, fut_close, option_type
            )
            option_ohlc['weeks_to_expiry'] = option_ohlc['days_to_expiry'].apply(get_week)
            records=data_frame_to_dict(option_ohlc)
            self.mongo.insert_many(records, 'atm_stock_options')
        except Exception as _e:
            print(f"Error downloading Options data for {symbol}: {option_ohlc}")

    async def update_futures_data(self, last_accessed_date_fut,start_date,end_date):
        """
        updates the futures data till date from last_accessed_date_fut inclusive
        """
        if start_date and end_date :
            start=start_date
            to_today=end_date
        else:
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
            if expiry_date is not str:
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

    async def download_historical_options(self, start_date, end_date, last_accessed_date_opt, update_daily=True,update_date_wise=False):
        """
            start_date: A datetime.date object representing the start date for which options data needs to be downloaded.
            end_date: A datetime.date object representing the end date for which options data needs to be downloaded.
            update_daily: A boolean value indicating whether to update daily data or not.
        """
        request_count = 2
        ohlc_futures = []
        if update_daily:

            ohlc_futures = self.mongo.find_many(
                {"Date": {"$gte": pd.to_datetime(last_accessed_date_opt)}
                 },
                'stock_futures'
            )

        elif update_date_wise:

            ohlc_futures = self.mongo.find_many(
            {
                "Date": {
                    "$gte": pd.to_datetime(start_date),
                    "$lte":pd.to_datetime(end_date)
                    }
            },
            'stock_futures'
        )

        else:
            prev_month = start_date.month - 1 or 12
            year = start_date.year - (prev_month == 12)

            prev_expiry = self.nse_downloader.get_expiry(
                year, prev_month) + pd.Timedelta(days=1)
            
            if last_accessed_date_opt is not None:
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
            for option_type in ["CE", "PE"]:
                tasks.append(asyncio.ensure_future(
                    self._download_historical_options_v3(
                        symbol,
                        s_date,
                        s_date,
                        expiry,
                        strike_price,
                        float(ohlc_fut['Close']),
                        option_type
                    )))
            
            if(request_count % 150 == 0):
                await asyncio.sleep(5)
            request_count += 2
        await asyncio.gather(*tasks)
        
    def download_options_for_pnl(self):
        """
        Download options data for P&L calculation.

        Fetches options data for each order from the database, filters based on criteria,
        and inserts the relevant records into the options collection.
        """

        for order in self.mongo.find_many({}, os.environ['ORDERS_COLLECTION_NAME']):
            # Calculate the end date based on working days
            end = add_working_days(order['created_at'], NO_OF_WORKING_DAYS_END_CALCULATION, self.holidays)

            # Set the one day before the current date as the maximum end date
            one_day_before = get_last_business_day(pd.to_datetime(date.today()),self.holidays)
            end = min(end, one_day_before, order['expiry'])

            start_date = order['created_at']

            # Query to filter options records
            query = {
                'Symbol': order['symbol'],
                "Date": {
                    "$gte": pd.to_datetime(order['created_at']),
                    "$lte": pd.to_datetime(end)
                },
                'Strike Price': order['strike'],
                'Expiry': order['expiry']
            }

            # Find relevant options records from the database
            result = self.mongo.find_many(query, os.environ['OPTIONS_COLLECTION_NAME'], sort=[('Date', -1)])

            # Check if the end date is already present in the result set
            date_present = any(data_dict['Date'] == end for data_dict in result) or end== pd.to_datetime(EXCEPTION_DATE)
            if date_present:
                continue

            # If 'Lot_Size' is present in the first result, update the start date
            if len(result) > 0 and 'Lot_Size' in result[0]:
                start_date = end

            # Fetch options data for both CE and PE types
            opt_types = ['CE', 'PE']
            for opt_type in opt_types:
                data = self.nse_downloader.get_oneday_options_history(
                    ticker=order['symbol'],
                    opt_type=opt_type,
                    start_date=start_date,
                    end_date=end,
                    expiry_date=order['expiry'],
                    strike=order['strike']
                )
                records = data_frame_to_dict(data)
                self.mongo.insert_many(records, os.environ['OPTIONS_COLLECTION_NAME'])
    
    async def _download_historical_futures(self,ticker,year,month):
        try:
            print(f'{ticker} is processing ')
            ohlc_fut = await asyncio.get_event_loop().run_in_executor(None, self.nse_downloader.get_month_fut_history, ticker, year,month)
            data=data_frame_to_dict(ohlc_fut)
            if not ohlc_fut.empty:
                self.mongo.insert_many(data, 'stock_futures')
        except Exception as _e:
            print(f"Error downloading Futures data for {ticker}: {_e}")
   
    async def download_historical_futures(self, start_date: date, end_date: date) -> None:
        """Downloads historical futures data for the given tickers between start_date and end_date.

        Args:
            start_date (date): The start date for downloading historical data.
            end_date (date): The end date for downloading historical data.

        Returns:
            None: The function does not return anything, but downloads the historical data for the given tickers and time period.

        """
        tickers = self.tickers
        tasks = []
        while start_date <= end_date:
        
            previous_month_number = (MONTHS_IN_YEAR if start_date.month == 1 else start_date.month - 1)
            if previous_month_number == MONTHS_IN_YEAR:
                previous_expiry = self.nse_downloader.get_expiry(start_date.year - 1, previous_month_number)
            else:
                previous_expiry = self.nse_downloader.get_expiry(start_date.year, previous_month_number)

            # Add one day to make it the start of the contract for the current month
            current_month_expiry = self.nse_downloader.get_expiry(start_date.year, start_date.month)
            current_month_start = previous_expiry + timedelta(days=1)
            for ticker in tickers:
                tasks.append(asyncio.create_task(
                    self._update_futures_data(ticker, current_month_start, current_month_expiry, current_month_expiry)
                    # self._download_historical_futures(ticker, start_date.year, start_date.month,)
                    ))
            start_date += relativedelta(months=1)
        await asyncio.gather(*tasks)
        print('Download completed.')

    def download_historical(self,start_date,end_date):
        """
        start_date:datetime,
        end_date:datetime
        """
        start_time = time.time()
        asyncio.run(self.download_historical_futures(start_date,end_date)) 
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"Execution time to download futures: {execution_time} seconds")
        start_time = time.time()
        asyncio.run(self.download_historical_options(start_date, end_date,None,False,True))
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time to downdload Options: {execution_time} seconds")
        


