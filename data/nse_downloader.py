"""
This module is responsible for downloading the files csv and  F&O data from  nse
"""

from datetime import timedelta
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

    def download_csv(self, symbol):
        """
        Downloads the historical data in CSV format for the given stock symbol.

        :param symbol: A string representing the stock symbol to download.
        :return: A string containing the downloaded CSV data.
        """
        # code to download the CSV data goes here
    def get_expiry(self, year, month):
        """Get the expiry date of a contract for the specified year and month.

        Args:
            year (int): The year for the contract expiry date.
            month (int): The month for the contract expiry date, represented as a number from 1 to 12.

        Returns:
            datetime.date: The expiry date of the contract for the specified year and month.

        """
        return self.nse_india.get_expiry_date(year, month)
    def get_month_fut_history(self, ticker, year, month):
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
   
    def download_options_data(self, symbol):
        """
        Downloads the options data for the given stock symbol.

        :param symbol: A string representing the stock symbol to download.
        :return: A string containing the downloaded options data.
        """
        # code to download the options data goes here

    def download_futures_data(self, symbol):
        """
        Downloads the futures data for the given stock symbol.

        :param symbol: A string representing the stock symbol to download.
        :return: A string containing the downloaded futures data.
        """
        # code to download the futures data goes here
