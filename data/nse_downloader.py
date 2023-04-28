# pylint: disable=too-many-arguments
"""
This module is responsible for downloading the files csv and  F&O data from  nse
"""

import asyncio
from datetime import timedelta,datetime
import pandas as pd
import requests
from data.constants import MONTHS_IN_YEAR
from src.nse_india import NSE


class NSEDownloader:

    """
    A class that provides methods for downloading data from the National Stock Exchange (NSE).

    Example usage:

        # Create a new instance of the NSEDownloader class
        downloader = NSEDownloader()

        # Download the historical data for a specific stock
        data = downloader.download_csv('RELIANCE')

        # Download the options data for a specific stock
        data = downloader.download_options_data('RELIANCE')

        # Download the futures data for a specific stock
        data = downloader.download_futures_data('RELIANCE')
    """
    def __init__(self) -> None:
        self.nse_india=NSE()
        
    def close_connection(self):
        self.nse_india.session.close()
               
    def get_expiry(self, year:int, month:int)->datetime.date:
        """Get the expiry date of a contract for the specified year and month.

        Args:
            year (int): The year for the contract expiry date.
            month (int): The month for the contract expiry date, represented as a number from 1 to 12.

        Returns:
            datetime.date: The expiry date of the contract for the specified year and month.

        """
        return self.nse_india.get_expiry_date(year, month)
    
    def get_month_fut_history(self, ticker: str, year: int, month: int) -> pd.DataFrame:
        """Get the historical contract for the specified ticker, year, and month.

        Args:
            ticker (str): The ticker symbol for the contract.
            year (int): The year for the contract.
            month (int): The month for the contract, represented as a number from 1 to 12.

        Returns:
            pandas.DataFrame: A DataFrame containing the historical contract data.

        Raises:
            ValueError: If the month value is not between 1 and 12.

        """
        # Get previous month expiry date
        previous_month_number = (MONTHS_IN_YEAR if month == 1 else month - 1)
        if previous_month_number == MONTHS_IN_YEAR:
            previous_expiry = self.get_expiry(year - 1, previous_month_number)
        else:
            previous_expiry = self.get_expiry(year, previous_month_number)

        # Add one day to make it the start of the contract for the current month
        current_month_expiry = self.get_expiry(year, month)
        current_month_start = previous_expiry + timedelta(days=1)

        # Get historical contract for the passed year and month
        history = self.nse_india.get_history(
            symbol=ticker,
            from_date=current_month_start,
            to_date=current_month_expiry,
            expiry_date=current_month_expiry
        )
        return history
    
    async def _download_historical_futures_v3(self, ticker: str, year: int,
                                              month: int) -> pd.DataFrame:
        """
        Download and insert historical futures data for a given ticker, year, and month.

        Args:
            ticker (str): The ticker symbol of the futures contract.
            year (int): The year of the futures contract expiration date.
            month (int): The month (1-12) of the futures contract expiration date.

        Returns:
             pd.DataFrame .
        """
        try:
            print(f"Processing {ticker}...")
            return await asyncio.to_thread(self.get_month_fut_history, ticker, year, month)
            #  data_frame_to_dict(ohlc_fut)
            # if not ohlc_fut.empty and (data[0]["Symbol"] in self.df_dict or ticker == self.map_symbol_name(data[0]["Symbol"])):
            #     self.stock_futures.insert_many(data)
        except requests.exceptions.RequestException as error:
            print(f"Error downloading futures data for {ticker}: {error}")
    def get_oneday_options_history(
            self, ticker:str, opt_type:str,
            start_date:datetime, expiry_date:datetime, strike:float):
        """
        Returns the one-day options history for a given ticker symbol, option type,
        expiry date, and strike price, as retrieved from the NSE India API.

        Parameters:
        -----------
        ticker: str
            The symbol for the underlying security of the option, e.g., "SBIN"
        opt_type: str
            The type of the option, either "CE" for call option or "PE" for put option
        s: str
            The date string for the start date of the historical data, in the format "yyyy-mm-dd"
        e: str
            The date string for the expiry date of the option, in the format "yyyy-mm-dd"
        strike: float
            The strike price of the option.

        Returns:
        --------
        A pandas DataFrame containing the one-day options history for the given inputs.

        Example:
        --------
        get_oneday_options_history("SBIN", "CE", "2022-04-25", "2022-04-28", 500.0)
        """
        return self.nse_india.get_history(
            symbol=ticker,
            from_date=start_date,
            to_date=start_date,
            expiry_date=expiry_date,
            option_type=opt_type,
            strike_price=strike
        )
    async def _download_historical_options_v3(self, symbol: str, s_date: datetime,
                                              end_date: datetime,
                                              strike_price: float,
                                              fut_close: float,
                                              option_type: str) -> pd.DataFrame:
        """
        Download historical options data for a given symbol.

        Args:
            symbol (str): The symbol for which to download the options data.
            s_date (datetime): The start date of the options data.
            end_date (datetime): The end date of the options data.
            strike_price (float): The strike price of the options data.
            fut_close (float): The closing price of the futures data.
            option_type (str): CE or PE The type of options data to download.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the historical options data.

        Raises:
            requests.exceptions.RequestException: If an error occurs while downloading the data.
        """
        try:
            print(f'{symbol} is processing')
            loop = asyncio.get_event_loop()
            opt_data = await loop.run_in_executor(None, self.get_oneday_options_history, symbol,
                                                  option_type, s_date,
                                                  end_date, strike_price)
            opt_data['days_to_expiry'] = (end_date - s_date).days
            opt_data['fut_close'] = fut_close
            return opt_data

            # opt_data['weeks_to_expiry']=opt_data['days_to_expiry'].apply(self.get_week)
            # record=
            # self.stock_options.insert_many(self.data_frame_to_dict(opt_data))
        except requests.exceptions.RequestException as error:
            print(f"Error downloading Options data for {symbol} option Type:{type}: {error}")

    async def _update_futures_data_v3(self,ticker:str,start:datetime,end:datetime,expiry_date:datetime) ->pd.DataFrame :
        """
        Download historical futures data for a given ticker.

        Args:
            ticker (str): The ticker for which to download the options data.
            s_date (datetime): The start date of the options data.
            end_date (datetime): The end date of the options data.
            expiry_date (datetime): expiry date of the future .

        Returns:
            pd.DataFrame: A pandas DataFrame containing the historical options data.

        Raises:
            requests.exceptions.RequestException: If an error occurs while downloading the data.
        """
        try:
            print(f'{ticker} is processing')
            ohlc_fut =await asyncio.get_event_loop().run_in_executor(None,
            self.nse_india.get_history,
            ticker,
            start,
            end,
            expiry_date)
            ohlc_fut= ohlc_fut.dropna()
            return ohlc_fut
        except requests.exceptions.RequestException as error:
                print(f"Error downloading futures data for {ticker} option Type:{type}: {error}")