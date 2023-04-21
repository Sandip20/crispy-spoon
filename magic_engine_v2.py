# pylint: disable=missing-function-docstring

import os 
import pandas as pd
from datetime import date,datetime, timezone
from datetime import timedelta
import requests
import time
from pymongo import InsertOne, MongoClient,UpdateMany
import certifi
from queue import Queue
from threading import Thread
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
import asyncio
from downloader.nse_india import NSE
ca = certifi.where()
load_dotenv()
NUM_THREADS = 7

q = Queue()
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
exclusions=["Saturday","Sunday"]
class OptionWizard:
    col=['Symbol','Expiry','Open','High','Low','Close']
   
    def __init__(self) -> None:
        self.tg_api_token=os.environ['TG_API_TOKEN']
        self.tg_chat_id=os.environ['TG_CHAT_ID']
        self.nse_india=NSE()
    
    def download_file(self,file_path):
        folder_name="files"
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            
        file_name = os.path.split(file_path)[1]
        url=f"{os.environ['NSE_FO_ARCHIVE_URL']}{file_name}"
        print(f"myurls:{url}")
        response = requests.get(url)
        
        with open(file_path, "wb") as f:
            f.write(response.content)
    
    def map_symbol_name(self,symbol):
        symbol_map = {
            'LTI': 'LTIM',
             'COFORGE': 'NIITTECH',
            # 'ZYDUSLIFE': 'CADILAHC',
            # 'SHRIRAMFIN':'SRTRANSFIN'
        }
        return symbol_map.get(symbol, symbol)
    def connect_mongo(self):
        try:
            if(os.environ['MONGO_INITDB_HOST']=="localhost"):
                url=f"mongodb://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}:27017/?retryWrites=true&w=majority"
                self.client=MongoClient(url)
                print(url)
            else:
                
                url=f"mongodb+srv://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}/?retryWrites=true&w=majority"
                self.client=MongoClient(url,tlsCAFile=ca)
            db = self.client[f"{os.environ['MONGO_INITDB_DATABASE']}"]
            self.stock_options=db[f"{os.environ['STOCK_OPTION_COLLECTION_NAME']}"]
            self.stock_futures=db[f"{os.environ['STOCK_FUTURE_COLLECTION_NAME']}"]
            self.activity=db[f"{os.environ['UPDATE_LOG_COLLECTION_NAME']}"]
            self.processed_options_data=db[f"{os.environ['STRADDLE_COLLECTION_NAME']}"]
            self.skipped_futures=[]
            self.skipped_futures_collection=db[f"{os.environ['STOCK_SKIPPED_COLLECTION_NAME']}"]
            self.stocks_step=db[f"{os.environ['STOCK_STEP_COLLECTON_NAME']}"]
            self.orders=db[f"{os.environ['ORDERS_COLLECTION_NAME']}"]
            self.options_data=db[f"{os.environ['OPTIONS_COLLECTION_NAME']}"]
            self.closed_positions=db[f"{os.environ['CLOSED_POSITIONS_COLLECTION_NAME']}"]
            print(db.list_collection_names())
            doc_cursor=self.stocks_step.find({},{"_id":0,"Symbol":1,"step":1,'lot_size':1})
            ls= list(doc_cursor)
            df_dict={}
            symbol_lot_details={}
            tickers=[]
            for item in ls:
                df_dict[item['Symbol']]=float(item['step'])
                symbol_lot_details[item['Symbol']]= item['lot_size'] if 'lot_size' in item.keys() else 0
                tickers.append(item['Symbol'])
            self.df_dict=df_dict
            self.lot_size=symbol_lot_details
            self.tickers=tickers
            
            self.last_accessed_date_fut=self.get_last_accessed('fut')
            self.last_accessed_date_opt=self.get_last_accessed('opt')
            
            
        except Exception as e:
            print("Unable to connect to the server.",e)
    def telegram_bot(self,bot_message):
        send_text='https://api.telegram.org/bot'+self.tg_api_token+'/sendMessage?chat_id='+self.tg_chat_id+'&parse_mode=html&text='+bot_message
        response=requests.get(send_text)
        print(response.text)
    def update_stocks_info(self):
        # Define the file names and paths
        strike_info_path = os.path.join('files', os.environ['STRIKE_INFO_NAME'])
        lot_info_path = os.path.join('files', os.environ['LOT_INFO_NAME'])
        
        # Download the files
        self.download_file(strike_info_path)
        self.download_file(lot_info_path)
        
        # Load and clean the strike info data
        strike_info_df = pd.read_excel(strike_info_path, header=2, usecols=range(4))
        strike_info_df.columns = ['Symbol', 'step', 'no_of_strikes', 'additional_strikes_enabled_intraday']
        
        # Load and clean the lot info data        
        lot_info_df = pd.read_csv(lot_info_path, skiprows=[0, 1,2,3,4], usecols=[1, 2], names=['Symbol', 'lot_size'])
        lot_info_df = lot_info_df.apply(lambda x: x.str.strip())
        lot_info_df['lot_size'] = pd.to_numeric(lot_info_df['lot_size'], errors='coerce')
        lot_info_df = lot_info_df.dropna().astype({'lot_size': int})
      
        # Merge the data frames
        strike_info_df=strike_info_df.merge(lot_info_df,on="Symbol",how="left")
        # Update the database
        self.stocks_step.delete_many({})
        result = self.stocks_step.insert_many(strike_info_df.to_dict('records'))
        
        # Print the result
        print(f"Inserted {result.acknowledged} new records")
    def get_strike(self,price,step):
        
        r=(price%step)
        if r < (step/2):
            if  isinstance(step,float):
                price=float(price-r)
            else:
                price=int(price-r)
        else:
            if isinstance(step,float):
                price=float(price+(step-r))
            else:
                price=int(price+(step-r))
            
        return f'{price:.2f}'
    def get_month_fut_history(self,ticker,year,month):
        #get previous month expiry date
        prev_month=month-1
        if(prev_month==0):
            prev_month=12
            prev_expiry= self.get_expiry(year-1,prev_month)
        else:
            prev_expiry= self.get_expiry(year,prev_month)
        
        # add one day to make it as start of the contract for  current month
        prev_expiry=prev_expiry+timedelta(days=1)
        # get current month expiry date for  
        expiry_next= self.get_expiry(year,month)
     
        #get historical contract of passed year and month
        return get_history(
            symbol=ticker,
            start=prev_expiry,
            end=expiry_next,
            futures=True,
            expiry_date=expiry_next
            )
    def get_oneday_options_history(self,ticker,opt_type,s,e,strike):
        return self.nse_india.get_history(
        symbol=ticker,
        from_date=s,
        to_date=s,
        expiry_date=e,
        option_type=opt_type,
        strike_price=strike,
        )
    
    def _download_historical_options(self):
        global q
        while q.qsize()!=0:
            input_dict=q.get()
            symbol=input_dict['symbol']
            s_date=input_dict['s_date']
            end_date=input_dict['end_date']
            strike_price=input_dict['strike_price']
            fut_close=input_dict["fut_close"]
            type=input_dict['type']
            print(f'{symbol} is processing')
            opt_data=self.get_oneday_options_history(symbol,type,s_date,end_date,strike_price)
            opt_data['days_to_expiry']=(end_date-s_date).days
            opt_data['fut_close']=fut_close
            opt_data['weeks_to_expiry']=opt_data['days_to_expiry'].apply(self.get_week)
            record=self.data_frame_to_dict(opt_data)
            is_exist=self.stock_options.find_one(record[0])
            if is_exist is None:
                self.stock_options.insert_one(record[0])
            q.task_done()
    async def _download_historical_options_v3(self,symbol,s_date,end_date,strike_price,fut_close,type):
        try:
            print(f'{symbol} is processing')
            opt_data=await asyncio.get_event_loop().run_in_executor(None,self.get_oneday_options_history,symbol,type,s_date,end_date,strike_price)
            opt_data['days_to_expiry']=(end_date-s_date).days
            opt_data['fut_close']=fut_close
            # opt_data['weeks_to_expiry']=opt_data['days_to_expiry'].apply(self.get_week)
            # record=
            self.stock_options.insert_many(self.data_frame_to_dict(opt_data))
        except Exception as e:
            print(f"Error downloading Options data for {symbol} option Type:{type}: {e}")
                
    def _download_historical_futures(self):
        global q
       
        while q.qsize()!=0:
            input_dict=q.get()
            ticker=input_dict['ticker']
            year=input_dict['year']
            month=input_dict['month']
            print(f'{ticker} is processing ')
            ohlc_fut=self.get_month_fut_history(ticker,year,month)
            data=self.data_frame_to_dict(ohlc_fut)
         
            if (not ohlc_fut.empty) and ((data[0]['Symbol'] in  self.df_dict.keys()) or  ticker==self.map_symbol_name(data[0]['Symbol'])) :
             
                self.stock_futures.insert_many(data)
            else:
                input={}
                input['ticker']=ticker
                input['year']=year
                input['month']=month
                input['new_symbol']=data[0]['Symbol']
                
                document=self.skipped_futures_collection.find_one(
                    {
                        'ticker':input['ticker'],
                        'year':input['year'],
                        'month':input['month']
                    })
                if document is None:
                    self.skipped_futures_collection.insert_one(input)
                self.skipped_futures.append(input['ticker'])
            
            
            q.task_done()
    async def _download_historical_futures_v3(self,ticker,year,month,):
          try:
            print(f'{ticker} is processing ')
            ohlc_fut = await asyncio.get_event_loop().run_in_executor(None, self.get_month_fut_history, ticker, year,month)
            data=self.data_frame_to_dict(ohlc_fut)
            if (not ohlc_fut.empty) and ((data[0]['Symbol'] in  self.df_dict.keys()) or  ticker==self.map_symbol_name(data[0]['Symbol'])) :
                self.stock_futures.insert_many(data)
          except Exception as e:
            print(f"Error downloading Futures data for {ticker}: {e}")

    def download_historical_futures_v2(self,start_date,end_date):
        _month=start_date.month
        _year=start_date.year
        tickers=self.tickers
       
        for ticker in tickers:
            if(self.map_symbol_name(ticker) is not None):
                ticker=self.map_symbol_name(ticker)
            no_of_months=(relativedelta(end_date,start_date).months+1)
            month=_month
            year=_year
            while no_of_months>0 :
                input_dict={
                    "ticker":ticker ,
                    "year":year,
                    "month":month
                    }
                q.put(input_dict)
                month+=1
                if month%13==0:
                    month=1
                    year+=1
                no_of_months-=1
        print('Threads are starting')
        self.start_threads('_download_historical_futures')
        print('download completed') 
    async def download_historical_futures_v3(self,start_date,end_date):
        _month=start_date.month
        _year=start_date.year
        tickers=self.tickers
        tasks=[]
        for ticker in tickers:
            if(self.map_symbol_name(ticker) is not None):
                ticker=self.map_symbol_name(ticker)
            no_of_months=(relativedelta(end_date,start_date).months+1)
            month=_month
            year=_year
            while no_of_months>0 :
                tasks.append(
                    
                    asyncio.ensure_future(self._download_historical_futures_v3(ticker,year,month))
                )
                month+=1
                if month%13==0:
                    month=1
                    year+=1
                no_of_months-=1
        await asyncio.gather(*tasks)
        print('download completed') 
    def future_input_for_optionsV2(self, start_date, end_date, update_daily=False):
        ohlc_futures = []
        if update_daily:
            last_accessed_date_opt = self.last_accessed_date_opt
            ohlc_futures = self.stock_futures.find(
                {"Date": {"$gte": pd.to_datetime(last_accessed_date_opt)}},
                {"Symbol": 1, "Expiry": 1, "Close": 1, "Date": 1, "_id": 0}
            )
        else:
            prev_month = start_date.month - 1 or 12
            year = start_date.year - (prev_month == 12)
            prev_expiry = self.get_expiry(year, prev_month) + pd.Timedelta(days=1)
            expiry_next = self.get_expiry(end_date.year, end_date.month)
            ohlc_futures = self.stock_futures.find(
                {"Date": {"$gte": pd.to_datetime(prev_expiry), "$lte": pd.to_datetime(expiry_next)}},
                {"Symbol": 1, "Expiry": 1, "Close": 1, "Date": 1, "_id": 0}
            )
        step_dict = self.df_dict
        for ohlc_fut in ohlc_futures:
            symbol = ohlc_fut["Symbol"]
            if symbol == "LTI":
                symbol = "LTIM"
            if symbol not in self.tickers:
                continue
            # if (symbol, ohlc_fut['Date']) in already_exists:
            #     continue
            options_data = self.stock_options.count_documents(
                {"Symbol": symbol, "Date": ohlc_fut['Date']})
            if options_data == 2:
                continue
            step=step_dict[symbol]
            end_date=ohlc_fut["Expiry"]
            #all dates of current month
            # get step of the sticker 
            s_date=ohlc_fut['Date']
            close=float(ohlc_fut["Close"])
            strike_price=self.get_strike(close,step)
            input_dict= {
                "end_date":end_date,
                "symbol":self.map_symbol_name(symbol),
                "s_date":s_date,
                "close":close,
                "strike_price":strike_price,
                "fut_close":float(ohlc_fut['Close']),
                "type":"CE"
            }
            q.put(input_dict)
            input_dict_copy=input_dict.copy()
            input_dict_copy['type']="PE"
            q.put(input_dict_copy)
    def future_input_for_options(self,start_date=None,end_date=None,update_daily=False):
        ohlc_futures=[]
        if(update_daily):
            last_accessed_date_opt=self.last_accessed_date_opt
            stock_futures_cursor=self.stock_futures.find(
                {
                    "Date":{
                        "$gte": pd.to_datetime(last_accessed_date_opt)
                        }
                    },
                {"_id":0,"Symbol":1,"Expiry":1,"Close":1,"Date":1
                }
                )
            ohlc_futures.extend(list(stock_futures_cursor))
        else:
            prev_month=start_date.month-1
            year=start_date.year
            if(prev_month==0):
                prev_month=12
                prev_expiry= self.get_expiry(year-1,prev_month)
            else:
                prev_expiry= self.get_expiry(year,prev_month)
        
            # add one day to make it as start of the contract for  current month
            prev_expiry=prev_expiry+timedelta(days=1)
            # get current month expiry date for  
            expiry_next= self.get_expiry(end_date.year,end_date.month)
            # expiry_dates=list(self.stock_futures.distinct('Expiry'))
            ohlc_futures= list(self.stock_futures.find(
                {'Date':{
                    "$gte":pd.to_datetime(prev_expiry),
                    "$lte":pd.to_datetime(expiry_next)
                    }
                 },
                {"_id":0,"Symbol":1,"Expiry":1,"Close":1,"Date":1}))
           
            # for expiry in expiry_dates:
            #     stock_futures_cursor=self.stock_futures.find({'Expiry':expiry},{"_id":0,"Symbol":1,"Expiry":1,"Close":1,"Date":1})
            #     ohlc_futures.extend(list(stock_futures_cursor)) 
 
        step_dict=self.df_dict
        already_exists=[]
        for ohlc_fut in ohlc_futures:
            symbol=ohlc_fut["Symbol"]
            if(symbol=="LTI"):
                symbol="LTIM"
            
            options_data=list(self.stock_options.find({"Symbol":symbol,"Date":ohlc_fut['Date']}))
            if(len(options_data)==2):
                already_exists.append({"symbol":symbol,"date":ohlc_fut['Date']})
                continue
            if symbol not in self.tickers:
                continue    
            step=step_dict[symbol]
                        
            end_date=ohlc_fut["Expiry"]
            #all dates of current month
            # get step of the sticker 
            s_date=ohlc_fut['Date']
            close=ohlc_fut["Close"]
            strike_price=self.get_strike(close,step)
            input_dict={
                "end_date":end_date,
                "symbol":self.map_symbol_name(symbol),
                "s_date":s_date,
                "close":close,
                "strike_price":strike_price,
                "fut_close":ohlc_fut['Close'],
                "type":"CE"
            }
            q.put(input_dict)
            input_dict_copy=input_dict.copy()
            input_dict_copy['type']="PE"
            q.put(input_dict_copy)
        print(f"skipped scripts{already_exists}")
    def add_ce_pe_of_same_dateV2(self, start_date, end_date):
        pipeline = [
           {
    '$group': {
        '_id': {
            'symbol': '$Symbol',
            'Date': '$Date',
            'strike_price': '$Strike Price',
            'Expiry': '$Expiry',
            'days_to_expiry': '$days_to_expiry',
            'weeks_to_expiry': '$weeks_to_expiry',
            'fut_close': '$fut_close',
            'option_type': '$Option Type',
            'close': '$Close'
        },
    },
},
{
    '$group': {
        '_id': {
            'symbol': '$_id.symbol',
            'Date': '$_id.Date',
            'strike_price': '$_id.strike_price',
            'Expiry': '$_id.Expiry',
            'days_to_expiry': '$_id.days_to_expiry',
            'weeks_to_expiry': '$_id.weeks_to_expiry',
            'fut_close': '$_id.fut_close',
        },
        'premiums': {
            '$push': '$_id.close'
        },
        'option_types': {
            '$addToSet': '$_id.option_type'
        }
    }
},

            {
                '$project': {
                    'symbol': '$_id.symbol',
                    'premiums': '$premiums',
                    'strike': '$_id.strike_price',
                    'Date': '$_id.Date',
                    'Expiry': '$_id.Expiry',
                    'days_to_expiry': '$_id.days_to_expiry',
                    'weeks_to_expiry': '$_id.weeks_to_expiry',
                    'fut_close': {'$toDouble':'$_id.fut_close'},
                    'straddle_premium': {
                        '$sum': '$premiums'
                    },
                    '_id': 0
                }
            },
            {
                '$project': {
                    'symbol': '$symbol',
                    'premiums': '$premiums',
                    'strike': '$strike',
                    'Date': '$Date',
                    'Expiry': '$Expiry',
                    'days_to_expiry': '$days_to_expiry',
                    'weeks_to_expiry': '$weeks_to_expiry',
                    'straddle_premium': '$straddle_premium',
                    '%coverage': {
                        '$multiply': [
                            {'$divide': ['$straddle_premium', '$fut_close']},
                            100
                        ]
                    }
                }
            }
        ]
        if start_date and end_date:
            prev_expiry = self.get_expiry(
                start_date.year if start_date.month != 1 else start_date.year - 1, 
                start_date.month-1 if start_date.month!=1 else 12) + timedelta(days=1)
            next_expiry = self.get_expiry(end_date.year, end_date.month)
            match_query = {
                "$match": {
                    "Date": {
                        "$gte": pd.to_datetime(prev_expiry),
                        "$lte": pd.to_datetime(next_expiry)
                    }
                }
            }
            pipeline.insert(0, match_query)
            self.processed_options_data.delete_many({"Date": {"$gte": pd.to_datetime(prev_expiry), "$lte": pd.to_datetime(next_expiry)}})
        aggregated = list(self.stock_options.aggregate(pipeline))
        if aggregated:
            self.processed_options_data.insert_many(aggregated)
        print("Processed successfully")
    def download_historical_options(self,start_date,end_date,delete_old=False):
            if delete_old:
                self.stock_options.delete_many({})
            self.future_input_for_optionsV2(start_date=start_date,end_date=end_date)
            self.start_threads('_download_historical_options')

    async def download_historical_options_v3(self,start_date,end_date,update_daily=True):
        self.request_count=2
        ohlc_futures = []
        if update_daily:
            
            ohlc_futures = list(self.stock_futures.find(
            {"Date": {"$gte": pd.to_datetime(self.last_accessed_date_opt)}},
            {"Symbol": 1, "Expiry": 1, "Close": 1, "Date": 1, "_id": 0}
            ))
        else:
            prev_month = start_date.month - 1 or 12
            year = start_date.year - (prev_month == 12)
            prev_expiry = self.get_expiry(year, prev_month) + pd.Timedelta(days=1)
            expiry_next = self.get_expiry(end_date.year, end_date.month)
            ohlc_futures = list(self.stock_futures.find(
                {"Date": {"$gte": pd.to_datetime(prev_expiry), "$lte": pd.to_datetime(expiry_next)}},
                {"Symbol": 1, "Expiry": 1, "Close": 1, "Date": 1, "_id": 0}
            ))
        step_dict = self.df_dict
        tasks=[]
        filtered_ohlc_futures = [
            record for record in  ohlc_futures
            if record['Symbol'] in self.tickers ]

        for ohlc_fut in filtered_ohlc_futures:
            symbol = ohlc_fut["Symbol"]
            if symbol == "LTI":
                symbol = "LTIM"
            # if symbol not in self.tickers:
            #     continue
            # 
            # for this logic is not neccessary to check if any document is already present
            # options_data = self.stock_options.count_documents(
            #     {"Symbol": symbol, "Date": ohlc_fut['Date']})
            # if options_data == 2:
            #     continue
            step=step_dict[symbol]
            end_date=ohlc_fut["Expiry"]
            #all dates of current month
            # get step of the sticker 
            s_date=ohlc_fut['Date']
            close=float(ohlc_fut["Close"])
            strike_price=self.get_strike(close,step)
            tasks.append(asyncio.ensure_future(
                self._download_historical_options_v3(
                    self.map_symbol_name(symbol),
                    s_date,
                    end_date,
                    strike_price,
                    float(ohlc_fut['Close']),
                    "CE"
                    )))
            tasks.append(asyncio.ensure_future(self._download_historical_options_v3(
                self.map_symbol_name(symbol),
                s_date,
                end_date,
                strike_price,
                ohlc_fut['Close'],
                "PE"
                )))
            if(self.request_count%150 == 0):
                await asyncio.sleep(5)
            self.request_count += 2
        await asyncio.gather(*tasks)


    def get_week(self,days_to_expiry):
        if days_to_expiry > 26:
            return 'week5'
        elif days_to_expiry > 19:
            return 'week4'
        elif days_to_expiry > 12:
            return 'week3'
        elif days_to_expiry > 5:
            return 'week2'
        elif days_to_expiry > -1:
            return 'week1'
        else:
            return 'expired'
  
    def data_frame_to_dict(self,df):
        df['Date']=pd.to_datetime(df.index)
        df['Expiry']=pd.to_datetime(df['Expiry'])
        return df.to_dict('records')
    
    def get_portfolio_pnl(self):
        port_folio={'pnl':0,
                    'total_capital':0
                    }
        for order in self.orders.find():
            symbol = order['symbol']
            quantity = order['quantity']
            strike=order['strike']
            created_at=order['created_at']
            price=order['price']
            data=list(self.options_data.find({'Symbol':symbol,'Strike Price':strike}).sort([('Date', -1)]).limit(2))
            current_price=float(data[0]['Close'])+float(data[1]['Close'])
            port_folio[symbol]={}
            port_folio[symbol]['quantity']=quantity
            port_folio[symbol]['strike']=strike
            port_folio[symbol]['created_at']=created_at
            port_folio[symbol]['buy_price']=price
            port_folio[symbol]['current_price']=current_price
            port_folio[symbol]['capital']=current_price*quantity  
            port_folio[symbol]['pnl']=(current_price - price) * quantity
            port_folio['pnl'] += port_folio[symbol]['pnl']
            port_folio['total_capital'] += port_folio[symbol]['capital']
        return port_folio
    def create_order(self,cheapest_stocks):
        self.orders.bulk_write(cheapest_stocks)
    def send_to_telegram(self, cheapest_records, today):
    # Define column names and calculate maximum symbol length
        columns = ['Symbol', 'Strike', 'Straddle Premium', '%Coverage', 'Current vs prev two months']
        holidays=self.nse_india.get_nse_holidays()
        def get_next_business_day(today, days=1):
            for i in range(1, days+1):
                if (
                    ((today + timedelta(days=i)).strftime('%d-%b-%Y') not in [h['tradingDate'] for h in holidays]) 
                    and ((today + timedelta(days=i)).strftime('%A') not in exclusions)):
                    return (today + timedelta(days=i)).strftime('%d-%b-%Y')
                
        next_business_day=get_next_business_day(today,days=5)

        """
        today+timedelta(days=i).strftime('%d-%b-%Y') not in list of dictionary and  today+timedelta(days=i).strftime('%A') not in exclusions
        return today+timedelta(days=i).strftime('%d-%b-%Y') 
        """
        # Format message header
        bot_message = f"<b>Scripts for {next_business_day}</b>\n\n"

        # Format column headers
        header = " | ".join(f"<b>{col}</b>" for col in columns)
        bot_message += f"{header}\n{'-' * len(header)}\n"

        # Format record rows
        for rec in cheapest_records:
            row_values = [f"{val:.2f}" if isinstance(val, float) else val for val in (rec[col.lower().replace(' ', '_')] for col in columns)]
            row = " | ".join(str(val) for val in row_values)
            bot_message += f"<code>{row}</code>\n"

        # Format footer with script symbols
        script_symbols = ', '.join(rec['symbol'] for rec in cheapest_records)
        bot_message += f"\n<b>Script symbols:</b> {script_symbols}"

        # Send message to telegram
        self.telegram_bot(bot_message.replace('&','_'))
   

    def find_cheapest_options(self,n,input_date=None,no_of_days_back=False):
        columns=['symbol',"strike",'straddle_premium',"%coverage"]
        self.columns=columns
        latest_doc=self.processed_options_data.find_one(sort=[("Date", -1)])
        today=latest_doc['Date']

        day_name=today.strftime("%A")
        if input_date:
            today=input_date
        elif no_of_days_back:
            today=today-timedelta(days=no_of_days_back)
        elif day_name in exclusions:
            days=exclusions.index(day_name)+1
            today=today-timedelta(days=days)

        query=[
                    {
                        '$match': {
                            'Date': datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=timezone.utc),
                            "two_months_week_min_coverage" : { "$type": "double","$ne" : float('nan') } ,
                            "current_vs_prev_two_months":{"$gte":-5,"$lte":0},
    
                        }
                    }, {
                        '$group': {
                            '_id': {
                                'symbol': '$symbol', 
                                'Date': '$Date', 
                                'Expiry': '$Expiry', 
                                '%coverage': '$%coverage', 
                                'two_months_week_min_coverage': '$two_months_week_min_coverage', 
                                'current_vs_prev_two_months': '$current_vs_prev_two_months', 
                                'strike': '$strike', 
                                'straddle_premium': '$straddle_premium', 
                                'week_min_coverage': '$week_min_coverage', 
                                'weeks_to_expiry': '$weeks_to_expiry', 
                                'days_to_expiry': '$days_to_expiry'
                            }, 
                            'distinct_val': {
                                '$addToSet': '$Date'
                            }
                        }
                    }, {
                        '$unwind': {
                            'path': '$distinct_val', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$project': {
                            'symbol': '$_id.symbol', 
                            'Date': '$_id.Date', 
                            '%coverage': '$_id.%coverage', 
                            'two_months_week_min_coverage': '$_id.two_months_week_min_coverage', 
                            'current_vs_prev_two_months': '$_id.current_vs_prev_two_months', 
                            'strike': '$_id.strike', 
                            'straddle_premium': '$_id.straddle_premium', 
                            'week_min_coverage': '$_id.week_min_coverage', 
                            'weeks_to_expiry': '$_id.weeks_to_expiry', 
                            'days_to_expiry': '$_id.days_to_expiry', 
                             "expiry":"$_id.Expiry",
                            '_id': 0
                        }
                    }, {
                        '$sort': {
                            'current_vs_prev_two_months': 1
                        }
                    }
                    , {
                        '$limit': n
                    }
        ]
        return {'day':today,"cheapest_options":list(self.processed_options_data.aggregate(query))}
    
    def download_options_for_pnl(self,back_test=False):
        for order in self.orders.find():
            if back_test:
                end=order['created_at']+timedelta(days=3)
                result =list(self.options_data.find({'Symbol':order['symbol'],"Date":pd.to_datetime(end)}))
                if(len(result)):
                    continue
                data=get_history(
                    symbol= self.map_symbol_name(order['symbol']),
                    start=order['created_at'],
                    end=end,
                    strike_price=order['strike'],
                    option_type='CE',
                    expiry_date=order['expiry'])
                records=self.data_frame_to_dict(data)
                self.options_data.insert_many(records)  
                data=get_history(
                    symbol=self.map_symbol_name(order['symbol']),
                    start=order['created_at'],
                    end=end,
                    strike_price=order['strike'],
                    option_type='PE',
                    expiry_date=order['expiry'])
                records=self.data_frame_to_dict(data)
                self.options_data.insert_many(records) 
            else:
                for day in  range(1,4,1):
                    end=order['created_at']+timedelta(days=day)
                    result =list(self.options_data.find({'Symbol':order['symbol'],"Date":pd.to_datetime(end)}))
                    if(len(result)):
                        continue
                    data=get_history(
                        symbol=order['symbol'],
                        start=order['created_at'],
                        end=end,
                        strike_price=order['strike'],
                        option_type='CE',
                        expiry_date=order['expiry'])
                    records=self.data_frame_to_dict(data)
                    self.options_data.insert_many(records)  
                    data=get_history(
                        symbol=order['symbol'],
                        start=order['created_at'],
                        end=end,
                        strike_price=order['strike'],
                        option_type='PE',
                        expiry_date=order['expiry'])
                    records=self.data_frame_to_dict(data)
                    self.options_data.insert_many(records)  

    def update_record(self,record,columns,date_of_trade):
            del record['Date']
            # del record['two_months_week_min_coverage']
            # del record['current_vs_prev_two_months']
            symbol=record[columns[0]]
            record['created_at']=date_of_trade
            record['quantity']=symbol if symbol =="COFORGE" else self.lot_size[self.map_symbol_name(symbol)]
            record['price']= record[columns[2]]
            return InsertOne(record)
   
    def get_current_month_data(self,current_expiry:date):
        return pd.DataFrame(list(self.processed_options_data.find({
            "Expiry":pd.to_datetime(current_expiry)
        },{"_id":0})))
    
    def get_last_two_months_data(self,today:date):
        today=today
        new_date=today-relativedelta(months=1)
        
        prev_one_month_expiry=self.get_expiry(new_date.year,new_date.month)
        
        new_date=today-relativedelta(months=3)
        
        prev_second_month_expiry=self.get_expiry(new_date.year,new_date.month)
        
        return pd.DataFrame(list(self.processed_options_data.find({
        "Expiry":{
            "$lte":pd.to_datetime(prev_one_month_expiry),"$gt":pd.to_datetime(prev_second_month_expiry),
        }
    },{"_id":0})))
        
    def  process(self,update_last_two_months=False):
        today=date.today()
        new_date=today-relativedelta(months=1)
        expiry_date=self.get_expiry(new_date.year,new_date.month)
        if update_last_two_months:
            query=[
                    {
                    "$match":{
                        "Date":{
                            "$lt":pd.to_datetime(expiry_date)
                        }
                    }    
                    },
                    {
                        "$group":{
                                "_id":{ 
                                    "weeks_to_expiry":"$weeks_to_expiry",
                                    "symbol":"$symbol"
                                    }, 
                                "week_min_coverage":{
                                "$min":"$%coverage"    
                                }
                            }
                        
                    },
                    {
                        "$project":{
    
                            "week_min_coverage":"$week_min_coverage",
                            "symbol":"$_id.symbol",
                            "weeks_to_expiry":"$_id.weeks_to_expiry",
                            "_id":0
                            }
                    }
                ]
            aggregated=  list(self.processed_options_data.aggregate(query))
            
            for rec in aggregated:
                self.processed_options_data.update_many(
                    {
                        "symbol":rec['symbol'],
                        "weeks_to_expiry":rec['weeks_to_expiry'],
                        "Date":{
                            "$lte":pd.to_datetime(expiry_date)
                        }
                    },
                    {
                        "$set":{
                            "week_min_coverage":rec['week_min_coverage']
                            }
                    })
        
        current_month=self.get_current_month_data(today)
        df=self.get_last_two_months_data(today)
        for symbol in current_month["symbol"].unique():
                    for week in current_month["weeks_to_expiry"].unique():
                        
                        mask1=current_month["weeks_to_expiry"]==week
                        mask2=current_month["symbol"]==symbol

                        df_new=current_month[mask1&mask2]

                        mask3= df["weeks_to_expiry"]== week 
                        mask4=df["symbol"]==symbol

                        df_new2=df[mask3&mask4]
                       
                        if df_new2.shape[0]!=0 and df_new2.shape[0]!=0 :
                            current_month.loc[mask1&mask2,'current_vs_prev_two_months']=round((df_new["%coverage"]-df_new2['week_min_coverage'].unique()[0]),1)
                            current_month.loc[mask1&mask2,'two_months_week_min_coverage']=df_new2['week_min_coverage'].unique()[0]

        data =current_month.to_dict('records')
        for rec in data:
            self.processed_options_data.update_many(
                {
                    "symbol":rec['symbol'],
                    "Date":rec['Date']
                },
                {
                    "$set":{
                        "current_vs_prev_two_months":rec['current_vs_prev_two_months'],
                        "two_months_week_min_coverage":rec["two_months_week_min_coverage"]
                        }
                })
        df_final=current_month
        df_final.to_csv(f"./data/{'current_month'}.csv")
        df.to_csv(f"./data/consolidated.csv")
        print('csv generated')
    
    def _update_futures_data(self):
        global q
        while q.qsize()!=0:
            input=q.get()
            # if 'start'in input.keys() and input['start'] is not None:
            start=input['start']
            ticker=input['ticker']
            end=input['end']
            expiry_date=input['expiry_date']
            print(f'{ticker} is processing')
            ohlc_fut=self.nse_india.get_history(
                symbol=ticker,
                from_date=start,
                to_date=end,
                expiry_date=expiry_date
            )
            ohlc_fut= ohlc_fut.dropna()
            if not ohlc_fut.empty:
                data=self.data_frame_to_dict(ohlc_fut)
                print(data[0]['Symbol'])
                self.stock_futures.insert_many(data)
            else:
                input['start']=pd.to_datetime(input['start'])
                input['end']=pd.to_datetime(input['end'])
                input['expiry_date']=pd.to_datetime(input['expiry_date'])
                document=self.skipped_futures_collection.find_one(
                    {
                        'ticker':input['ticker'],
                        'start':input['start'],
                        'expiry_date':input['expiry_date']
                    })
                if document is None:
                    self.skipped_futures_collection.insert_one(input)
                self.skipped_futures.append(input['ticker'])
            q.task_done()
    
    async def _update_futures_data_v3(self,ticker,start,end,expiry_date):
        try:
            print(f'{ticker} is processing')
            ohlc_fut = await asyncio.get_event_loop().run_in_executor(None,
            self.nse_india.get_history,
            ticker, 
            start,
            end, 
            expiry_date)
            #

            ohlc_fut= ohlc_fut.dropna()
            if not ohlc_fut.empty:
                data=self.data_frame_to_dict(ohlc_fut)
                self.stock_futures.insert_many(data)
        except Exception as e:
            print(f"Error downloading Futures data for {ticker}: {e}")
            
    def get_expiry(self,year,month):
         return self.nse_india.get_expiry_date(year,month)
        
     
    def start_threads(self,method_name):
        for t in range(NUM_THREADS):
                worker=Thread(target=getattr(self, method_name),daemon=True)
                worker.start()
        q.join()
    def update_futures_data(self,start_date=None,end_date=None):
        last_accessed_date_fut=self.last_accessed_date_fut
        start= start_date or pd.to_datetime(last_accessed_date_fut).date()
        to_today=end_date or date.today()
        expiry_date=self.get_expiry(to_today.year,to_today.month)
        
        if(to_today>expiry_date):
            new_date=to_today+relativedelta(months=1)
            expiry_date=self.get_expiry(new_date.year,new_date.month)
        for ticker in self.tickers:
            input={}
            input['ticker']=self.map_symbol_name(ticker)
            input['start']=start
            input['end']=to_today
            input['expiry_date']= expiry_date
            q.put(input)
        self.start_threads('_update_futures_data')
        print('futures updated')
        self.activity.find_one_and_replace({'last_accessed_date':last_accessed_date_fut,'instrument':"fut"},{'instrument':"fut",'last_accessed_date':pd.to_datetime(date.today()-timedelta(days=0))})
    async def update_futures_data_v3(self):
        last_accessed_date_fut=self.last_accessed_date_fut
        self.request_count=1

        if pd.to_datetime(date.today()).date() == pd.to_datetime(last_accessed_date_fut).date():
            print('Data is already updated')
            return
        
        start=pd.to_datetime(last_accessed_date_fut).date()
        
        to_today=date.today()
        expiry_date=self.get_expiry(to_today.year,to_today.month)
        if(to_today>expiry_date):
            new_date=to_today+relativedelta(months=1)
            expiry_date=self.get_expiry(new_date.year,new_date.month)
        tasks=[]
        for ticker in self.tickers:
            tasks.append(
                asyncio.ensure_future(
                self._update_futures_data_v3(self.map_symbol_name(ticker),start,to_today,expiry_date)))
            # if(self.request_count%100 == 0):
            #     await asyncio.sleep(3)
            # self.request_count += 1
            
          
            
        await asyncio.gather(*tasks)
    
    def get_last_accessed(self,instrument):
        activity=self.activity.find_one({'instrument':instrument})
        return activity['last_accessed_date']
          
    def update_options_data(self,start_date,end_date):
        if pd.to_datetime(start_date).date() == pd.to_datetime(self.last_accessed_date_opt).date():
            print('Data is already updated')
            return
        self.future_input_for_optionsV2(self.last_accessed_date_opt,end_date,update_daily=True)
        self.start_threads('_download_historical_options')
        print('OPtions updated')
        self.activity.find_one_and_replace({'last_accessed_date':self.last_accessed_date_opt,'instrument':'opt'},{'instrument':'opt','last_accessed_date':pd.to_datetime(date.today())})
    #runs daily to update the  futures and options of the scripts
    def update_to_latest(self):
        start_date=pd.to_datetime(date.today())
        self.update_futures_data()
        if len(self.skipped_futures)>0:
            print('Could not update below tickers:\n')
            print(self.skipped_futures)
            return    
        print("--------------futures updated------------")
     
        self.update_options_data(start_date=start_date,end_date=start_date)
        print("--------------options  updated------------")
        self. update_security_names()
        
        
        self.add_ce_pe_of_same_dateV2(start_date=start_date,end_date=start_date)
   
        print('data processing')
        # self.update_week_min_coverage()
        self.update_current_vs_prev_two_months(today=True).to_csv('current.csv')
        print('CSV generated')
    def update_to_latest_v3(self):
        start_time = time.time()
        asyncio.run(self.update_futures_data_v3()) 
       
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        self.activity.find_one_and_replace({'last_accessed_date':self.last_accessed_date_opt,'instrument':'fut'},{'instrument':'fut','last_accessed_date':pd.to_datetime(date.today())})
        print("--------------futures updated------------")
        start_time = time.time()
        start_date=pd.to_datetime(date.today())
        asyncio.run(self.download_historical_options_v3(start_date,start_date))
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        # update the last accessed date of updates
        self.activity.find_one_and_replace({'last_accessed_date':self.last_accessed_date_opt,'instrument':'opt'},{'instrument':'opt','last_accessed_date':pd.to_datetime(date.today())})
        self. update_security_names()
        self.add_ce_pe_of_same_dateV2(start_date=start_date,end_date=start_date)
        # print('data processing')
        # # self.update_week_min_coverage()
        self.update_current_vs_prev_two_months(today=True).to_csv('current.csv')
        print('CSV generated')
    
    # download the historical of all the scripts   till date          
    def download_historical(self,start_date,end_date):
        start_time = time.time()
        self.download_historical_futures_v2(start_date,end_date)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
    
    def download_historical_v3(self,start_date,end_date):
        start_time = time.time()
        asyncio.run(self.download_historical_futures_v3(start_date,end_date)) 
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"Execution time to download futures: {execution_time} seconds")
        start_time = time.time()
       
        asyncio.run(self.download_historical_options_v3(start_date,end_date))
        
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time to downdload Options: {execution_time} seconds")
        self. update_security_names()
        self.add_ce_pe_of_same_dateV2(start_date=start_date,end_date=end_date)
        self.update_week_min_coverage(start_date=start_date,end_date=end_date)
        self.update_current_vs_prev_two_months(start_date=start_date,end_date=end_date)
    def update_security_names(self):
        try:
            self.stock_futures.update_many({'Symbol':'LTI'},{'$set':{'Symbol':'LTIM'}})
            self.stock_options.update_many({'Symbol':'LTI'},{'$set':{'Symbol':'LTIM'}})
        except Exception as e:
            print(e)
    def update_week_min_coverage(self, start_date=None,end_date=None,update_last_two_months=False):
        print("------updating update_week_min_coverage----------")
        if(start_date):
            today = start_date
        else:
            today=date.today()
        new_date = today - relativedelta(months=1)
        expiry_date = self.get_expiry(new_date.year, new_date.month)

        if start_date and end_date:
            prev_expiry = self.get_expiry(
                start_date.year if start_date.month != 1 else start_date.year - 1, 
                start_date.month-1 if start_date.month!=1 else 12) + timedelta(days=1)
            next_expiry = self.get_expiry(end_date.year, end_date.month)
            from_expiry_date = prev_expiry
            pipeline = [
                {
                    "$match": {
                        "Date": {"$lte": pd.to_datetime(next_expiry),"$gte": pd.to_datetime(from_expiry_date)}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "weeks_to_expiry": "$weeks_to_expiry",
                            "symbol": "$symbol",
                            "Expiry":"$Expiry"
                        },
                        "week_min_coverage": {"$min": "$%coverage"}
                    }
                },
                {
                    "$project": {
                        "week_min_coverage": 1,
                        "symbol": "$_id.symbol",
                        "Expiry":"$_id.Expiry",
                        "weeks_to_expiry": "$_id.weeks_to_expiry",
                        "_id": 0
                    }
                }
            ]

            self.processed_options_data.update_many(
                { "Date": {"$lte": pd.to_datetime(next_expiry),"$gte": pd.to_datetime(from_expiry_date)}},
                {"$unset": {"week_min_coverage": ""}}
            )

            aggregated = list(self.processed_options_data.aggregate(pipeline, allowDiskUse=True))
            bulk_operations = [
                        UpdateMany(
                            {
                                "symbol": rec['symbol'],
                                "weeks_to_expiry": rec['weeks_to_expiry'],
                                "Date": {"$lte": pd.to_datetime(next_expiry),"$gte": pd.to_datetime(from_expiry_date)},
                                "Expiry":rec['Expiry']
                            },
                            {"$set": {"week_min_coverage": rec['week_min_coverage']}}
                        ) for rec in aggregated]
            result=self.processed_options_data.bulk_write(bulk_operations)
            print(f"Modified {result.modified_count} documents")
            
        if update_last_two_months:
            two_months_back = today - relativedelta(months=3)
            from_expiry_date = self.get_expiry(two_months_back.year, two_months_back.month)
            query = [
                {
                    "$match": {
                        "Date": {"$lt": pd.to_datetime(expiry_date),"$gt": pd.to_datetime(from_expiry_date)}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "weeks_to_expiry": "$weeks_to_expiry",
                            "symbol": "$symbol"
                        },
                        "week_min_coverage": {"$min": "$%coverage"}
                    }
                },
                {
                    "$project": {
                        "week_min_coverage": 1,
                        "symbol": "$_id.symbol",
                        "weeks_to_expiry": "$_id.weeks_to_expiry",
                        "_id": 0
                    }
                }
            ]

            self.processed_options_data.update_many(
                { "Date": {"$lte": pd.to_datetime(expiry_date),"$gt": pd.to_datetime(from_expiry_date)}},
                {"$unset": {"week_min_coverage": ""}}
            )

            aggregated = list(self.processed_options_data.aggregate(query, allowDiskUse=True))
            bulk_operations = [
                        UpdateMany(
                            {
                                "symbol": rec['symbol'],
                                "weeks_to_expiry": rec['weeks_to_expiry'],
                                "Date": {"$lte": pd.to_datetime(expiry_date),"$gt": pd.to_datetime(from_expiry_date)}
                            },
                            {"$set": {"week_min_coverage": rec['week_min_coverage']}}
                        ) for rec in aggregated]
            result=self.processed_options_data.bulk_write(bulk_operations)
            print(f"Modified {result.modified_count} documents")     
    def update_current_vs_prev_two_months(self,start_date=None,end_date=None,today=False):
        print("------updating Current Vs PreviousTwo months data----------")
        def process_monthly_data(current_month,last_two_months):
            if 'two_months_week_min_coverage' in current_month.columns:
                current_month=current_month.drop(columns='two_months_week_min_coverage')
                
            if 'week_min_coverage' in current_month.columns:
                current_month=current_month.drop(columns='week_min_coverage')
         
            last_two_months = last_two_months[["symbol", "weeks_to_expiry","week_min_coverage"]].rename(columns={"week_min_coverage": "two_months_week_min_coverage"})
            last_two_months = last_two_months.groupby(["symbol", "weeks_to_expiry",]).min().reset_index()
            last_two_months["two_months_week_min_coverage"] = last_two_months["two_months_week_min_coverage"].astype(float)
            current_month['weeks_to_expiry']=current_month['days_to_expiry'].apply(self.get_week)
            current_month = current_month.merge(last_two_months, on=["symbol", "weeks_to_expiry"], how="left")
            current_month["current_vs_prev_two_months"] = (
                current_month["%coverage"] - current_month["two_months_week_min_coverage"]
            ).round(1)
            current_month=current_month.dropna()
            self.processed_options_data.bulk_write([
                UpdateMany(
                    {"symbol": rec['symbol'], "Date": rec['Date']},
                    {
                        "$set": {
                            "current_vs_prev_two_months": rec['current_vs_prev_two_months'],
                            "two_months_week_min_coverage": rec["two_months_week_min_coverage"],
                            "weeks_to_expiry":rec["weeks_to_expiry"],
                        }
                    }
                ) for rec in current_month.to_dict('records')
            ])
            return current_month
            
        if today:
            today=date.today()
            current_expiry=self.get_expiry(today.year,today.month)
            current_month = self.get_current_month_data(current_expiry)
            last_two_months = self.get_last_two_months_data(current_expiry)
            current_month=process_monthly_data(current_month=current_month,last_two_months=last_two_months)
            mask= current_month["current_vs_prev_two_months"]>-5
            current_month[mask].to_csv('current.csv')
            return current_month
        elif start_date and end_date:
            expiry =self.get_expiry(
                start_date.year , 
                start_date.month)
            df = self.get_last_two_months_data(expiry)
            if df.empty or df['Expiry'].nunique()<2:
                print(f"No Data found for two months before {expiry}")
                return
            no_of_months=relativedelta(end_date,start_date).months+1
            while no_of_months>0:
                print(f"processing {expiry} expiry")
                current_month = self.get_current_month_data(expiry)
                df_two_months_data = self.get_last_two_months_data(expiry)
                start_time = time.time()
                current_month=process_monthly_data(current_month=current_month,last_two_months=df_two_months_data)
                end_time = time.time()
                time_taken=end_time-start_time
                print(f"Time Taken to process data for {expiry}:{time_taken} seconds")
                expiry=expiry+relativedelta(months=1)
                expiry=self.get_expiry(expiry.year,expiry.month)
                no_of_months-=1
    def place_orders(self,cheapest_records,trade_date):
        columns=['symbol',"strike",'straddle_premium',"%coverage"]
        date_of_trade=pd.to_datetime(trade_date)
        result =self.orders.find_one({'created_at': date_of_trade})
        if result is not None:
            return
        # if(date_of_trade.strftime('%A') =="Tuesday"):
            
        cheapest_records=[self.update_record(record,columns,date_of_trade) for record in cheapest_records]
        self.create_order(cheapest_records)
        
        print(date_of_trade.strftime('%A'))
    def close_week_orders(self):
        for order in self.orders.find():
            symbol = order['symbol']
            quantity = order['quantity']
            strike=order['strike']
            created_at=order['created_at']
            entry_price=order['price']
            query={'Symbol':symbol,'Strike Price':strike}
            data=list(self.options_data.find(query).sort([('Date', -1)]).limit(2))
            exit_price=round(float(data[0]['Close']),2)+round(float(data[1]['Close']),2)
            position={}
            position['symbol']=symbol
            position['strike']=strike
            position['created_at']=created_at
            position['exit_date']=data[0]['Date']
            position['expiry']=data[0]['Expiry']
            position['entry_price']=round(entry_price,2)
            position['exit_price']=round(exit_price,2)
            position['margin']=round(entry_price*quantity,2)  
            position['profit_loss']=round((exit_price - entry_price) * quantity,2)
            self.closed_positions.insert_one(position)
        self.orders.delete_many({})