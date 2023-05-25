# pylint: disable=bad-indentation
import os

import pandas as pd
from data.mongodb import Mongo
from data.util import find_available_trade_exit, update_record

class OrderManager:
    """Class to manage orders for a trading system."""
    def __init__(self,mongo:Mongo) -> None:
        """
        Initialize OrderManager instance with a MongoDB client.

        Parameters
        ----------
        mongo : Mongo
            An instance of the `Mongo` class from the `data.mongodb` module.
        """
        self.mongo=mongo

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
        cheapest_records = [update_record(record, columns, date_of_trade) for record in cheapest_records]

        # Place the orders
        self.create_order(cheapest_records)

        print(date_of_trade.strftime('%A'))

    def clear_existing_trades(self):
        """

        clears all the trades 
        """
        self.mongo.delete_many({},os.environ['CLOSED_POSITIONS_COLLECTION_NAME'])
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
            query = {'Symbol': symbol, 'Strike Price': strike,'Expiry':order['expiry']}
            data = self.mongo.find_many(
                query,
                os.environ['OPTIONS_COLLECTION_NAME'],
                sort=[('Date', -1)],
                limit=10)
            exit_price = round(
                float(data[0]['Close']), 2)+round(float(data[1]['Close']), 2)

            quantity = data[0]['Lot_Size']
            """
            find the  options data date and expiry date difference is 6 or 5 or 4 whichever is available at the top
            """

            # filtered=filter(find_available_trade_exit,data,order['expiry'])
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