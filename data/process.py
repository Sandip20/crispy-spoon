# pylint: disable=import-error
"""
This module will be responsible to process the data
"""
from datetime import date, datetime,timedelta

from dateutil.relativedelta import relativedelta
import pandas as pd
from nse_downloader import NSEDownloader
from pymongo import UpdateMany
from mongodb import Mongo


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