# pylint: disable=broad-exception-caught
""" 
OptionWizard class  is main class which will  import neccessary modules and 
calls respective methods
"""
import asyncio
from datetime import timedelta,datetime,date, timezone
from dateutil.relativedelta import relativedelta
import time
import os
from data.constants import DATE_FORMAT_B, MONTHS_IN_YEAR
import pandas as pd
from data.mongodb import Mongo
from dotenv import load_dotenv
from data.nse_downloader import NSEDownloader
from data.process import ProcessData
from data.util import data_frame_to_dict, get_last_business_day, get_week,get_strike
load_dotenv()

class OptionWizard:
    """
    This class is responsible for  all the magic happening in the project.
    """
    def __init__(self) -> None :
        self.tg_api_token=os.environ['TG_API_TOKEN']
        self.tg_chat_id=os.environ['TG_CHAT_ID']
        base_url=f"mongodb+srv://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}"
        url= f"{base_url}:27017/?retryWrites=true&w=majority" if os.environ['MONGO_INITDB_HOST']=="localhost" else f"{base_url}/?retryWrites=true&w=majority"
        # connection with mongodb
        print('url:',url)
        self.mongo=Mongo(url=url,db_name=os.environ['MONGO_INITDB_DATABASE'],is_ca_required=True)
        self.nse_downloader= NSEDownloader()
        self.process_data= ProcessData(self.nse_downloader,self.mongo)
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

        expiry_date=self.nse_downloader.get_expiry(start.year,start.month,start.day)
        expiry_dates=[expiry_date]

        if to_today>expiry_date:
            new_date= expiry_date + timedelta(1)
            expiry_date=self.nse_downloader.get_expiry(new_date.year,new_date.month,new_date.day)
            expiry_dates.append(expiry_date)
        for idx,expiry_date in enumerate(expiry_dates):
            if idx!=0:
                start=expiry_dates[idx-1]
                end_date=to_today
            else:
                end_date = expiry_date if to_today>expiry_date else to_today

            
            tasks=[]
            for ticker in self.tickers:
                tasks.append(asyncio.ensure_future(self._update_futures_data(ticker,start,end_date,expiry_date)))
        await asyncio.gather(*tasks)

    async def _update_futures_data(self,ticker,start,end,expiry_date):

        """
        event loop will call  _update_futures_data_v3 of nse_downloader.
        download the data for given input mongo insertmany  to insert into  stock_futures
        """
        try:
            ohlc_fut = await self.nse_downloader.update_futures_data(ticker,start,end,expiry_date)
            if not ohlc_fut.empty:
                data=data_frame_to_dict(ohlc_fut)
                self.mongo.insert_many(data,'stock_futures')
        except Exception as e:
            print(f"Error downloading Futures data for {ticker}: {e}")

    async def _download_historical_options_v3(self,symbol:str, s_date,end_date:datetime,expiry_date,strike_price,fut_close,option_type):
          
        try:
            option_ohlc= await self.nse_downloader.download_historical_options(
                symbol,s_date,end_date,expiry_date,strike_price,fut_close,option_type
                )
            option_ohlc['weeks_to_expiry']=option_ohlc['days_to_expiry'].apply(get_week)
            self.mongo.insert_many(data_frame_to_dict(option_ohlc),'atm_stock_options')
        except Exception as e:
            print(f"Error downloading Futures data for {symbol}: {e}")

       
    async def download_historical_options(self,start_date,end_date,update_daily=True):
        self.request_count=2
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

            prev_expiry = self.nse_downloader.get_expiry(year, prev_month) + pd.Timedelta(days=1)
            holidays=self.nse_downloader.get_nse_holidays()
            end_date= get_last_business_day(end_date,holidays)
            expiry_next = self.nse_downloader.get_expiry(end_date.year, end_date.month)
            ohlc_futures = self.mongo.find_many(
                {"Date": {"$gte": pd.to_datetime(prev_expiry), "$lte": pd.to_datetime(expiry_next)}},
                'stock_futures'
            )

        step_dict = self.df_dict
        tasks=[]
        filtered_ohlc_futures = [
            record for record in  ohlc_futures
            if record['Symbol'] in self.tickers ]
        
        print('total coun:',len(filtered_ohlc_futures))

        for ohlc_fut in filtered_ohlc_futures:
            symbol = ohlc_fut["Symbol"]
            step=step_dict[symbol]
            expiry=ohlc_fut["Expiry"]
            #all dates of current month
            # get step of the sticker 
            s_date=ohlc_fut['Date']
            close=float(ohlc_fut["Close"])
            strike_price=get_strike(close,step)
            tasks.append(asyncio.ensure_future(
                self._download_historical_options_v3(
                    symbol,
                    s_date,
                    expiry,
                    expiry,
                    strike_price,
                    float(ohlc_fut['Close']),
                    "CE"
                    )))
            tasks.append(asyncio.ensure_future(
                self._download_historical_options_v3(
                symbol,
                s_date,
                expiry,
                expiry,
                strike_price,
                ohlc_fut['Close'],
                "PE"
                )))
            if(self.request_count%150 == 0):
                await asyncio.sleep(5)
            self.request_count += 2
        await asyncio.gather(*tasks)
        
    def update_security_names(self):
        try:
            self.mongo.update_many({'Symbol':'LTI'},{'Symbol':'LTIM'},'stock_futures')
            self.mongo.update_many({'Symbol':'LTI'},{'Symbol':'LTIM'},'stock_options')
        except Exception as e:
            print(e)
            
    def update_daily(self):
        start_time = time.time()
        asyncio.run(self.update_futures_data()) 
       
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        self.mongo.update_one(
            {'last_accessed_date':self.last_accessed_date_fut,'instrument':'fut'},
            {'last_accessed_date':pd.to_datetime(date.today())},
            'activity'
            )
        print("--------------futures updated------------")

        start_time = time.time()
        start_date=pd.to_datetime(date.today())
        asyncio.run(self.download_historical_options(start_date,start_date))
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        # update the last accessed date of updates
        self.mongo.update_one(
            {'last_accessed_date':self.last_accessed_date_opt,'instrument':'opt'},
            {'last_accessed_date':pd.to_datetime(date.today())},
            'activity'
            )
        # self.update_security_names()
        self.process_data.add_ce_pe_of_same_date(start_date=start_date,end_date=start_date)
        print('data processing')
        self.process_data.update_week_min_coverage()
        self.process_data.update_current_vs_prev_two_months(today=True).to_csv('current.csv')
        print('CSV generated')


    