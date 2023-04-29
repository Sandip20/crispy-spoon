# pylint: disable=import-error
"""
This module will be responsible to process the data
"""
from datetime import date, datetime,timedelta
import time
from dateutil.relativedelta import relativedelta
import pandas as pd
from data.nse_downloader import NSEDownloader
from pymongo import UpdateMany
from data.mongodb import Mongo
from data.util import get_week

class ProcessData:
    """
    It will create instance of process data which in turn providers process method defaultly process the 
    data daily
    """
    def __init__(self,nse_downloader:NSEDownloader, mongo:Mongo) -> None:
        """
        create instace

        """
        self.nse_downloader=nse_downloader
        self.mongo =mongo

    def  process(self,
                 last_two_months_data:pd.DataFrame,
                 current_month:pd.DataFrame,
                 expiry_date:datetime.date,
                 update_last_two_months=False):    
        """
        Args:
       last_two_months_data:pd.DataFrame
       current_month:pd.DataFrame
       expiry_date:date.datetime
       update_last_two_months=False
       
        Default it will process the daily Data of the processed_options_data 
        """
        
        # today=date.today()
        # new_date=today-relativedelta(months=1)
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

            for rec in self.mongo.aggregate(query,'processed_options_data'):
                self.mongo.update_many( {
                        "symbol":rec['symbol'],
                        "weeks_to_expiry":rec['weeks_to_expiry'],
                        "Date":{
                            "$lte":pd.to_datetime(expiry_date)
                        }
                    },{
                            "week_min_coverage":rec['week_min_coverage']
                            },
                            'processed_options_data')
        for symbol in current_month["symbol"].unique():
            for week in current_month["weeks_to_expiry"].unique():
                mask1=current_month["weeks_to_expiry"]==week
                mask2=current_month["symbol"]==symbol
                df_new=current_month[mask1&mask2]
                mask3= last_two_months_data["weeks_to_expiry"]== week
                mask4=last_two_months_data["symbol"]==symbol
                df_new2=last_two_months_data[mask3&mask4]
                if df_new2.shape[0]!=0:
                    current_month.loc[mask1&mask2,'current_vs_prev_two_months']=round((df_new["%coverage"]-df_new2['week_min_coverage'].unique()[0]),1)
                    current_month.loc[mask1&mask2,'two_months_week_min_coverage']=df_new2['week_min_coverage'].unique()[0]

        for rec in current_month.to_dict('records'):
            self.mongo.update_many(
                {
                    "symbol":rec['symbol'],
                    "Date":rec['Date']
                },
                {
                        "current_vs_prev_two_months":rec['current_vs_prev_two_months'],
                        "two_months_week_min_coverage":rec["two_months_week_min_coverage"]
                },
                'processed_options_data'
                )
        current_month.to_csv(f"./data/{'current_month'}.csv")
        last_two_months_data.to_csv("./data/consolidated.csv")
        print('csv generated')
        
    def update_week_min_coverage(self, start_date=None,end_date=None,update_last_two_months=False):
        """
        update  week min coverage
        """
        print("------updating update_week_min_coverage----------")
        if(start_date):
            today = start_date
        else:
            today=date.today()
        new_date = today - relativedelta(months=1)
        expiry_date = self.nse_downloader. get_expiry(new_date.year, new_date.month)

        if start_date and end_date:
            prev_expiry =self.nse_downloader.get_expiry(
                start_date.year if start_date.month != 1 else start_date.year - 1, 
                start_date.month-1 if start_date.month!=1 else 12) + timedelta(days=1)
            next_expiry = self.nse_downloader.get_expiry(end_date.year, end_date.month)
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

            self.mongo.update_many(
                { "Date": {"$lte": pd.to_datetime(next_expiry),"$gte": pd.to_datetime(from_expiry_date)}},
                {"week_min_coverage": ""},
                'processed_options_data'
            )

            aggregated = self.mongo.aggregate(pipeline,'processed_options_data')
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

            result=self.mongo.bulk_write(bulk_operations)
            print(f"Modified {result.modified_count} documents")

        if update_last_two_months:
            two_months_back = today - relativedelta(months=3)
            from_expiry_date = self.nse_downloader. get_expiry(two_months_back.year, two_months_back.month)
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
            self.mongo.update_many(
                { "Date": {"$lte": pd.to_datetime(expiry_date),"$gt": pd.to_datetime(from_expiry_date)}},
               {"week_min_coverage": ""},
              'processed_options_data'
            )

            aggregated = self.mongo.aggregate(query,'processed_options_data')
            bulk_operations = [
                        UpdateMany(
                            {
                                "symbol": rec['symbol'],
                                "weeks_to_expiry": rec['weeks_to_expiry'],
                                "Date": {"$lte": pd.to_datetime(expiry_date),"$gt": pd.to_datetime(from_expiry_date)}
                            },
                            {"$set": {"week_min_coverage": rec['week_min_coverage']}}
                        ) for rec in aggregated]
            result=self.mongo.bulk_write(bulk_operations)
            print(f"Modified {result.modified_count} documents")
    def add_ce_pe_of_same_date(self, start_date, end_date):

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
            prev_expiry = self.nse_downloader.get_expiry(
                start_date.year if start_date.month != 1 else start_date.year - 1,
                start_date.month-1 if start_date.month!=1 else 12) + timedelta(days=1)
            next_expiry = self.nse_downloader.get_expiry(end_date.year, end_date.month)
            match_query = {
                "$match": {
                    "Date": {
                        "$gte": pd.to_datetime(prev_expiry),
                        "$lte": pd.to_datetime(next_expiry)
                    }
                }
            }
            pipeline.insert(0, match_query)
            self.mongo.delete_many(
                {"Date": {"$gte": pd.to_datetime(prev_expiry),
                "$lte": pd.to_datetime(next_expiry)}},
                'processed_options_data'
                )
        aggregated =self.mongo.aggregate(pipeline,'stock_options')
        if aggregated:
            self.mongo.insert_many(aggregated,'processed_options_data')
        print("Processed successfully")


    def get_current_month_data(self,current_expiry:date):
        return pd.DataFrame(self.mongo.find_many ({"Expiry":pd.to_datetime(current_expiry)},'processed_options_data'))
    def get_last_two_months_data(self,today:date)->pd.DataFrame:
        """
        Args:
        today:date
        Returns:
        pd.Dataframe

        """
        new_date=today-relativedelta(months=1)
        prev_one_month_expiry=self.nse_downloader.get_expiry(new_date.year,new_date.month)
        new_date=today-relativedelta(months=3)
        prev_second_month_expiry=self.nse_downloader.get_expiry(new_date.year,new_date.month)
        return pd.DataFrame(self.mongo.find_many({
        "Expiry":{
            "$lte":pd.to_datetime(prev_one_month_expiry),
            "$gt":pd.to_datetime(prev_second_month_expiry),
        }
    },'processed_options_data'))

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
            current_month['weeks_to_expiry']=current_month['days_to_expiry'].apply(get_week)
            current_month = current_month.merge(last_two_months, on=["symbol", "weeks_to_expiry"], how="left")
            current_month["current_vs_prev_two_months"] = (
                current_month["%coverage"] - current_month["two_months_week_min_coverage"]
            ).round(1)
            current_month=current_month.dropna()
            self.mongo.bulk_write()([
                UpdateMany(
                    {"symbol": rec['symbol'], "Date": rec['Date']},
                    {
                            "current_vs_prev_two_months": rec['current_vs_prev_two_months'],
                            "two_months_week_min_coverage": rec["two_months_week_min_coverage"],
                            "weeks_to_expiry":rec["weeks_to_expiry"],
                    }
                ) for rec in current_month.to_dict('records')
            ],'processed_options_data')
            return current_month
            
        if today:
            today=date.today()
            current_expiry=self.nse_downloader.get_expiry(today.year,today.month)
            current_month = self.get_current_month_data(current_expiry)
            last_two_months = self.get_last_two_months_data(current_expiry)
            current_month=process_monthly_data(current_month=current_month,last_two_months=last_two_months)
            mask= current_month["current_vs_prev_two_months"]>-5
            current_month[mask].to_csv('current.csv')
            return current_month
        if start_date and end_date:
            expiry =self.nse_downloader.get_expiry(
                start_date.year,
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
                expiry=self.nse_downloader.get_expiry(expiry.year,expiry.month)
                no_of_months-=1