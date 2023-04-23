"""
Unit test cases for nse_downloaders
"""
import unittest
import os
import sys
from datetime import date
import pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Now you can import the `NSEDownloader` class from the `nse_downloader` module
from data.nse_downloader import NSEDownloader

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
    
    def tearDown(self):
        self.nse.close_connection()
        
if __name__ == '__main__':
    unittest.main()