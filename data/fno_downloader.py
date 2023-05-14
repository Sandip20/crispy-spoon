# pylint: disable=broad-exception-caught
"""
Module responsible for downloading the F&O data
"""
import os
import asyncio
from datetime import timedelta, date, datetime

from typing import List
import pandas as pd
from data.constants import NO_OF_WORKING_DAYS_END_CALCULATION
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

    async def update_futures_data(self, last_accessed_date_fut):
        """
        updates the futures data till date from last_accessed_date_fut inclusive
        """
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

    async def download_historical_options(self, start_date, end_date, last_accessed_date_opt, update_daily=True):
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
            for option_type in ["CE", "PE"]:
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
            if(request_count % 150 == 0):
                await asyncio.sleep(5)
            request_count += 2
        await asyncio.gather(*tasks)

    async def download_historical_futures_monthly(self, symbol: str, year: int, month: int):
        """
        Downloads historical futures data for a given symbol, year, and month from the NSE and saves it to a MongoDB database.

        Parameters:
        -----------
        symbol : str
            The symbol for which to download historical futures data.
        year : int
            The year for which to download historical futures data.
        month : int
            The month for which to download historical futures data.

        Returns:
        --------
        data : pd.DataFrame
            A pandas DataFrame containing the downloaded historical futures data.
        """
        ohlc_fut: pd.DataFrame = await self.nse_downloader.download_historical_futures(symbol, year, month)
        #  data_frame_to_dict(ohlc_fut)
        #     # if not ohlc_fut.empty and (data[0]["Symbol"] in self.df_dict or ticker == self.map_symbol_name(data[0]["Symbol"])):
        #     #     self.stock_futures.insert_many(data)
    def download_options_for_pnl(self):
        """
        download options for P&L
        """

        for order in self.mongo.find_many({}, os.environ['ORDERS_COLLECTION_NAME']):
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