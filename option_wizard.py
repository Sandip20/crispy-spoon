
import os 
import pandas as pd
from nsepy import get_history
from nsepy.derivatives import get_expiry_date
from datetime import date,datetime, timezone
from datetime import timedelta
import requests
from pymongo import InsertOne, MongoClient,UpdateMany
import certifi
from queue import Queue
from threading import Thread
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
ca = certifi.where()
load_dotenv()
NUM_THREADS = 5

q = Queue()
import warnings

class FileDownloader:
    def __init__(self):
        self.tg_api_token=os.environ['TG_API_TOKEN']
        self.tg_chat_id=os.environ['TG_CHAT_ID']
    
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

class SymbolMapper:
    def __init__(self):
        pass
    
    def map_symbol_name(self,symbol):
        symbol_map = {
            'LTI': 'LTIM',
             'COFORGE': 'NIITTECH',
            # 'ZYDUSLIFE': 'CADILAHC',
            # 'SHRIRAMFIN':'SRTRANSFIN'
        }
        return symbol_map.get(symbol, symbol)

class MongoDBConnector:
    def __init__(self):
        pass
    
    def connect_mongo(self):
        try:
            self.client=MongoClient(f"mongodb://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@localhost:27017/?retryWrites=true&w=majority")
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

class TelegramBot:
    def __init__(self):
        self.tg_api_token=os.environ['TG_API_TOKEN']
        self.tg_chat_id=os.environ['TG_CHAT_ID']
    
    def telegram_bot(self,bot_message):
    
        send_text='https://api.telegram.org/bot'+self.tg_api_token+'/sendMessage?chat_id='+self.tg_chat_id+'&parse_mode=html&text='+bot_message
        response=requests.get(send
