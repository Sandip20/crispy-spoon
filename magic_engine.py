""" 
OptionWizard class  is main class which will  import neccessary modules and 
calls respective methods
"""
import asyncio
from datetime import timedelta,datetime,date
from dateutil.relativedelta import relativedelta
import os
# from data.constants import MONTHS_IN_YEAR
import pandas as pd
from data.mongodb import Mongo

from data.nse_downloader import NSEDownloader
from data.util import data_frame_to_dict, map_symbol_name

class OptionWizard:
    """
    This class is responsible for  all the magic happening in the project.
    """
    def __init__(self) -> None :
        self.tg_api_token=os.environ['TG_API_TOKEN']
        self.tg_chat_id=os.environ['TG_CHAT_ID']
        base_url=f"mongodb://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}"
        url= f"{base_url}:27017/?retryWrites=true&w=majority" if os.environ['MONGO_INITDB_HOST']=="localhost" else f"{base_url}/?retryWrites=true&w=majority"
        # connection with mongodb
        self.mongo=Mongo(url=url,db_name=os.environ['MONGO_INITDB_DATABASE'],is_ca_required=True)
        self.nse_downloader= NSEDownloader()
        self.last_accessed_date_fut=self.get_last_accessed('fut')
        self.last_accessed_date_opt=self.get_last_accessed('opt')
        self.get_tickers()

    def get_last_accessed(self,instrument:str)->datetime.date:
        """
        gets last accessed date of the instrumenttype from activity collection

        Args:
            (instrument:str) that represents  opt or fut

        Returns:
            lastaccessed date of the instrument
        """
        activity=self.mongo.find_one({'instrument':instrument},'activity')
        return activity['last_accessed_date']
    
    def get_tickers(self):
        """
            This will get all tickers from stock_step collection
            and set the 
            df_dict,
            lot_size{ticker:lot_size}
            tickers:list[]

        """
        df_dict={}
        symbol_lot_details={}
        tickers=[]

        for item in self.mongo.find_many({},'stocks_step'):
            df_dict[item['Symbol']]=float(item['step'])
            symbol_lot_details[item['Symbol']]= item['lot_size'] if 'lot_size' in item.keys() else 0
            tickers.append(item['Symbol'])

        self.df_dict=df_dict
        self.lot_size=symbol_lot_details
        self.tickers=tickers
        
    async def update_futures_data(self):
        """
        updates the futures data till date from last_accessed_date_fut inclusive
        """
        last_accessed_date_fut=self.last_accessed_date_fut

        if pd.to_datetime(date.today()).date() == pd.to_datetime(last_accessed_date_fut).date():
            print('Data is already updated')
            return
        start=pd.to_datetime(last_accessed_date_fut).date()
        to_today=date.today()
        expiry_date=self.nse_downloader.get_expiry(to_today.year,to_today.month)
        if to_today>expiry_date:
            new_date=to_today+relativedelta(months=1)
            expiry_date=self.nse_downloader.get_expiry(new_date.year,new_date.month)
        tasks=[]
        for ticker in self.tickers:
            tasks.append(asyncio.ensure_future(self._update_futures_data(map_symbol_name(ticker),start,to_today,expiry_date)))
        await asyncio.gather(*tasks)

    async def _update_futures_data(self,ticker,start,end,expiry_date):

        """
        event loop will call  _update_futures_data_v3 of nse_downloader.
        download the data for given input mongo insertmany  to insert into  stock_futures
        """
        try:
            ohlc_fut = await asyncio.get_event_loop().run_in_executor(None,
            self.nse_downloader._update_futures_data_v3,
            ticker,
            start,
            end,
            expiry_date)
            if not ohlc_fut.empty:
                data=data_frame_to_dict(ohlc_fut)
                self.mongo.insert_many(data,'stock_futures')
        except Exception as e:
            print(f"Error downloading Futures data for {ticker}: {e}")

    def update_daily(self):
        asyncio.run(self.update_futures_data()) 


    