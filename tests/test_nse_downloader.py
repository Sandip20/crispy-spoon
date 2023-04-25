# pylint: disable=import-error
"""
Unit test cases for nse_downloaders
"""
import unittest
import os
import sys
from datetime import date
from unittest.mock import MagicMock
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data.nse_downloader import NSEDownloader
# Now you can import the `NSEDownloader` class from the `nse_downloader` module


class TestNSEDownloader(unittest.TestCase):

    def setUp(self):
        # initialize test data and dependencies
        self.nse = NSEDownloader()

    def test_get_expiry(self):
        # test case for getting expiry date
        year = 2023
        month = 3
        expected_date = date(2023, 3, 29)
        actual_date = self.nse.get_expiry(year, month)
        self.assertEqual(expected_date, actual_date)

    def test_get_month_fut_history(self):
        # test case for getting historical contract data
        ticker = 'TATASTEEL'
        year = 2023
        month = 4
        expected_columns = ['Symbol', 'Expiry', 'Settle Price', 'Open', 'High', 'Low','Last_Traded_Price','Prev_Close','Close', 'Lot_Size']
        history = self.nse.get_month_fut_history(ticker, year, month)
        self.assertIsInstance(history, pd.DataFrame)
        self.assertListEqual(expected_columns, list(history.columns))
        self.assertTrue(len(history) > 0)

    async def test_download_historical_futures_v3(self):
        # Set up a mock for get_month_fut_history
        data = {'Date': ['2023-03-01', '2023-03-02', '2023-03-03'],
                'Open': [800.0, 810.0, 805.0],
                'High': [810.0, 820.0, 815.0],
                'Low': [790.0, 795.0, 790.0],
                'Close': [805.0, 815.0, 800.0],
                'Volume': [1000, 2000, 1500]}
        expected_df = pd.DataFrame(data)
        mock_get_month_fut_history = MagicMock(return_value=expected_df)
        
        # Set up test object
        
        self.nse.get_month_fut_history = mock_get_month_fut_history
        
        # Call method and get actual result
        actual_result = await self.nse._download_historical_futures_v3('TATAMOTORS', 2023, 3)
        
        # Check that get_month_fut_history was called correctly
        mock_get_month_fut_history.assert_called_once_with('TATAMOTORS', 2023, 3)
        
        # Check that the result is as expected
        pd.testing.assert_frame_equal(actual_result, expected_df)

    
    def tearDown(self):
        self.nse.close_connection()
        
if __name__ == '__main__':
    unittest.main()