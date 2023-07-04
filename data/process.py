# pylint: disable=import-error
"""
This module will be responsible to process the data
"""
import os
from datetime import date, datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
import pandas as pd
from pymongo import UpdateMany
from data.nse_downloader import NSEDownloader
from data.mongodb import Mongo
from data.queries.mongo_queries_processed_options import ADD_CE_PE_PIPELINE, create_week_min_query
from data.util import get_last_business_day, get_week


class ProcessData:
    """
    It will create instance of process data which in turn providers process method defaultly process the 
    data daily
    """

    def __init__(self, nse_downloader: NSEDownloader, mongo: Mongo) -> None:
        """
        create instace

        """
        self.nse_downloader = nse_downloader
        self.mongo = mongo

    def process(self,
                last_two_months_data: pd.DataFrame,
                current_month: pd.DataFrame,
                expiry_date: datetime.date,
                update_last_two_months=False):
        """
        Args:
       last_two_months_data:pd.DataFrame
       current_month:pd.DataFrame
       expiry_date:date.datetime
       update_last_two_months=False

        Default it will process the daily Data of the processed_options_data 
        """
        if update_last_two_months:
            query = [
                {
                    "$match": {
                        "Date": {
                            "$lt": pd.to_datetime(expiry_date)
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "weeks_to_expiry": "$weeks_to_expiry",
                            "symbol": "$symbol"
                        },
                        "week_min_coverage": {
                            "$min": "$%coverage"
                        }
                    }

                },
                {
                    "$project": {

                        "week_min_coverage": "$week_min_coverage",
                        "symbol": "$_id.symbol",
                        "weeks_to_expiry": "$_id.weeks_to_expiry",
                        "_id": 0
                    }
                }
            ]

            for rec in self.mongo.aggregate(query, os.environ['STRADDLE_COLLECTION_NAME']):
                self.mongo.update_many({
                    "symbol": rec['symbol'],
                    "weeks_to_expiry": rec['weeks_to_expiry'],
                    "Date": {
                        "$lte": pd.to_datetime(expiry_date)
                    }
                }, {
                    "week_min_coverage": rec['week_min_coverage']
                },
                    os.environ['STRADDLE_COLLECTION_NAME'])
        for symbol in current_month["symbol"].unique():
            for week in current_month["weeks_to_expiry"].unique():
                mask1 = current_month["weeks_to_expiry"] == week
                mask2 = current_month["symbol"] == symbol
                df_new = current_month[mask1 & mask2]
                mask3 = last_two_months_data["weeks_to_expiry"] == week
                mask4 = last_two_months_data["symbol"] == symbol
                df_new2 = last_two_months_data[mask3 & mask4]
                if df_new2.shape[0] != 0:
                    current_month.loc[mask1 & mask2, 'current_vs_prev_two_months'] = round(
                        (df_new["%coverage"]-df_new2['week_min_coverage'].unique()[0]), 1)
                    current_month.loc[mask1 & mask2, 'two_months_week_min_coverage'] = df_new2['week_min_coverage'].unique()[
                        0]

        for rec in current_month.to_dict('records'):
            self.mongo.update_many(
                {
                    "symbol": rec['symbol'],
                    "Date": rec['Date']
                },
                {
                    "current_vs_prev_two_months": rec['current_vs_prev_two_months'],
                    "two_months_week_min_coverage": rec["two_months_week_min_coverage"]
                },
                os.environ['STRADDLE_COLLECTION_NAME']
            )
        current_month.to_csv(f"./data/{'current_month'}.csv")
        last_two_months_data.to_csv("./data/consolidated.csv")
        print('csv generated')

    def update_week_min_coverage(self, start_date=None, end_date=None, update_last_two_months=False):

        """
            Update the week minimum coverage.

            Args:
            start_date (Optional[date]): Start date for coverage update, defaults to None
            end_date (Optional[date]): End date for coverage update, defaults to None
            update_last_two_months (bool): Flag to update coverage for the last two months, defaults to False

            Returns:
            None
        """
        print("------updating update_week_min_coverage----------")
        
        today=start_date if start_date is not None else date.today()

        new_date = today - relativedelta(months=1)
        expiry_date = self.nse_downloader. get_expiry(
            new_date.year, new_date.month)

        if start_date and end_date:
            prev_expiry = self.nse_downloader.get_expiry(
                start_date.year if start_date.month != 1 else start_date.year - 1,
                start_date.month-1 if start_date.month != 1 else 12) + timedelta(days=1)
            next_expiry = self.nse_downloader.get_expiry(
                end_date.year, end_date.month)
            from_expiry_date = prev_expiry

            pipeline = create_week_min_query(
                from_expiry_date=pd.to_datetime(from_expiry_date),
                to_expiry_date=pd.to_datetime(next_expiry))
            
            self.mongo.update_many(
                {"Date": {"$lte": pd.to_datetime(
                    next_expiry), "$gte": pd.to_datetime(from_expiry_date)}},
                {"week_min_coverage": ""},
                os.environ['STRADDLE_COLLECTION_NAME']
            )

            aggregated = self.mongo.aggregate(
                pipeline, os.environ['STRADDLE_COLLECTION_NAME'])
            bulk_operations = [
                UpdateMany(
                    {
                        "symbol": rec['symbol'],
                        "weeks_to_expiry": rec['weeks_to_expiry'],
                        "Date": {"$lte": pd.to_datetime(next_expiry), "$gte": pd.to_datetime(from_expiry_date)},
                        "Expiry": rec['Expiry']
                    },
                    {"$set": {
                        "week_min_coverage": rec['week_min_coverage']}}
                ) for rec in aggregated]

            result = self.mongo.bulk_write(bulk_operations, os.environ['STRADDLE_COLLECTION_NAME'])
            print(f"Modified {result.modified_count} documents")

        if update_last_two_months:
            two_months_back = today - relativedelta(months=3)
            from_expiry_date = self.nse_downloader. get_expiry(
                two_months_back.year, two_months_back.month)
            query = [
                {
                    "$match": {
                        "Date": {"$lt": pd.to_datetime(expiry_date), "$gt": pd.to_datetime(from_expiry_date)}
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
                {"Date": {"$lte": pd.to_datetime(
                    expiry_date), "$gt": pd.to_datetime(from_expiry_date)}},
                {"week_min_coverage": ""},
                os.environ['STRADDLE_COLLECTION_NAME']
            )

            aggregated = self.mongo.aggregate(
                query, os.environ['STRADDLE_COLLECTION_NAME'])
            bulk_operations = [
                UpdateMany(
                    {
                        "symbol": rec['symbol'],
                        "weeks_to_expiry": rec['weeks_to_expiry'],
                        "Date": {"$lte": pd.to_datetime(expiry_date), "$gt": pd.to_datetime(from_expiry_date)}
                    },
                    {"$set": {
                        "week_min_coverage": rec['week_min_coverage']}}
                ) for rec in aggregated]
            result = self.mongo.bulk_write(bulk_operations, os.environ['STRADDLE_COLLECTION_NAME'])
            print(f"Modified {result.modified_count} documents")

    def add_ce_pe_of_same_date(self, start_date, end_date):

        """
        Adds the call and put options of same date to a new collection named STRADDLE_COLLECTION_NAME. It gets the
        expiry of the given end_date and gets the data between the next expiry date of end_date and previous expiry
        date of start_date. It then aggregates the data by the symbol and the date and adds the call and put
        options to the new collection.

        :param start_date: A datetime object representing the start date.
        :param end_date: A datetime object representing the end date.
        :return: None
        """

        if start_date and end_date:
            current_expiry = self.get_month_expiry(end_date)
            # start_date=start_date-relativedelta(months=1)
            # #previous months first day to get that months expiry date for script
            # prev_expiry=self.get_month_expiry(start_date.replace(day=1)) + timedelta(days=1) 

            match_query = {
                "$match": {
                    "Expiry": pd.to_datetime(current_expiry)
                
                }
            }
            
            ADD_CE_PE_PIPELINE.insert(0, match_query)
            self.mongo.delete_many(
         {"Expiry": pd.to_datetime(current_expiry)}
             ,
                os.environ['STRADDLE_COLLECTION_NAME']
            )
        aggregated = self.mongo.aggregate(
            ADD_CE_PE_PIPELINE, os.environ['STOCK_OPTION_COLLECTION_NAME'])
        if aggregated:
            self.mongo.insert_many(
                aggregated, os.environ['STRADDLE_COLLECTION_NAME'])
        print("Processed successfully")

    def get_current_month_data(self, current_expiry: date):
        return pd.DataFrame(self.mongo.find_many({"Expiry": pd.to_datetime(current_expiry)}, os.environ['STRADDLE_COLLECTION_NAME']))

    def get_last_two_months_data(self, today: date) -> pd.DataFrame:
        """
        Args:
        today:date
        Returns:
        pd.DataFrame

        """
        new_date = (today-relativedelta(months=1)).replace(day=2)
        prev_one_month_expiry = self.get_month_expiry(new_date)
        new_date = (today-relativedelta(months=2)).replace(day=2)
     
        prev_second_month_expiry = self.get_month_expiry(new_date)
        return pd.DataFrame(self.mongo.find_many({
            "Expiry":{
                "$in":[
                    pd.to_datetime(prev_one_month_expiry),
                    pd.to_datetime(prev_second_month_expiry)
                    ]
                }
        }, os.environ['STRADDLE_COLLECTION_NAME']))

    def get_month_expiry(self, end_date: datetime):
        """
        Returns:current month expiry 
        """
        holidays = self.nse_downloader.get_nse_holidays()
        last_working_day: datetime = get_last_business_day(end_date, holidays)
        return self.nse_downloader.get_expiry(last_working_day.year, last_working_day.month, last_working_day.day)

    def update_current_vs_prev_two_months(self, start_date=None, end_date=None, today=False):
        """
        Args:
        start_date: start date of the month for which we need to  perform the operation update curr_vs_prev_two months
        end_date: end date of the month for which we need to  perform the operation update curr_vs_prev_two months
        today:  it takes current day as start point and process the current month and prev two month currosponding to currentday
        """
        print("------updating Current Vs PreviousTwo months data----------")

        def process_monthly_data(current_month, last_two_months):
            if 'two_months_week_min_coverage' in current_month.columns:
                current_month = current_month.drop(
                    columns='two_months_week_min_coverage')
            if 'week_min_coverage' in current_month.columns:
                current_month = current_month.drop(columns='week_min_coverage')

            last_two_months = last_two_months[["symbol", "weeks_to_expiry", "week_min_coverage"]].rename(
                columns={"week_min_coverage": "two_months_week_min_coverage"})
            last_two_months = last_two_months.groupby(
                ["symbol", "weeks_to_expiry",]).min().reset_index()
            last_two_months["two_months_week_min_coverage"] = last_two_months["two_months_week_min_coverage"].astype(
                float)
            current_month['weeks_to_expiry'] = current_month['days_to_expiry'].apply(
                get_week)
            current_month = current_month.merge(
                last_two_months, on=["symbol", "weeks_to_expiry"], how="left")
            current_month["current_vs_prev_two_months"] = (
                current_month["%coverage"] -
                current_month["two_months_week_min_coverage"]
            ).round(1)
            current_month = current_month.dropna()
            self.mongo.bulk_write([
                UpdateMany(
                    {"symbol": rec['symbol'], "Date": rec['Date']},

                    {"$set":
                        {
                            "current_vs_prev_two_months": rec['current_vs_prev_two_months'],
                            "two_months_week_min_coverage": rec["two_months_week_min_coverage"],
                            "weeks_to_expiry":rec["weeks_to_expiry"],
                        }
                     }
                ) for rec in current_month.to_dict('records')
            ], os.environ['STRADDLE_COLLECTION_NAME'])
            return current_month

        if today:
            current_expiry = self.get_month_expiry(date.today())
            current_month = self.get_current_month_data(current_expiry)
           

            last_two_months = self.get_last_two_months_data(current_expiry)
            expiry_mask=last_two_months['Expiry'] != pd.Timestamp(current_expiry)
            last_two_months = last_two_months[expiry_mask]

            current_month = process_monthly_data(
                current_month=current_month, last_two_months=last_two_months).drop(columns=['_id'])
            mask = current_month["current_vs_prev_two_months"] > -5
            current_month[mask].to_csv('current.csv',index=False)
            return current_month
        
        if start_date and end_date:
            expiry = self.get_month_expiry(start_date)
            no_of_months = relativedelta(end_date, start_date).months+1
            while no_of_months > 0:
                print(f"processing {expiry} expiry")
                current_month = self.get_current_month_data(expiry)
                df_two_months_data = self.get_last_two_months_data(expiry)

                start_time = time.time()

                current_month = process_monthly_data(
                    current_month=current_month, last_two_months=df_two_months_data)

                end_time = time.time()

                time_taken = end_time-start_time
                print(
                    f"Time Taken to process data for {expiry}:{time_taken} seconds")
                expiry = expiry+relativedelta(months=1)
                expiry =self.get_month_expiry(expiry.replace(day=1))
                no_of_months -= 1