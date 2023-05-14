"""
Utility Module 
"""
from datetime import timedelta,datetime
import pandas as pd
from typing import Union

from pymongo import InsertOne

from data.constants import DATE_FORMAT_B,EXCLUSIONS

def data_frame_to_dict(data_frame):
    """
    Convert a pandas DataFrame to a list of dictionaries.

    Args:
        df (pandas.DataFrame): The DataFrame to convert.

    Returns:
        A list of dictionaries, where each dictionary represents a row in the DataFrame.
        The dictionary keys are the column names and the values are the corresponding row values.

    Example:
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': ['foo', 'bar', 'baz']})
        >>> data_frame_to_dict(df)
        [{'A': 1, 'B': 'foo'}, {'A': 2, 'B': 'bar'}, {'A': 3, 'B': 'baz'}]
    """
    data_frame['Date'] = pd.to_datetime(data_frame.index)
    data_frame['Expiry'] = pd.to_datetime(data_frame['Expiry'])
    return data_frame.to_dict('records')

def get_next_business_day(today,holidays,days=1)->Union[datetime,None]:
    """
    Args:
    holidays:List Nse holidays
    days:int total days of bussiness
    Returns:
    Bussiness Day in format '%d-%b-%Y' 
    """
    for i in range(1, days+1):
        if (
            ((today + timedelta(days=i)).strftime(DATE_FORMAT_B) not in [h['tradingDate'] for h in holidays])
            and ((today + timedelta(days=i)).strftime('%A') not in EXCLUSIONS)):
                return (today + timedelta(days=i)).strftime(DATE_FORMAT_B)
    return None

def get_last_business_day(today,holidays,days=5)->Union[datetime,None]:
    """
    Args:
    holidays:List Nse holidays
    days:int total days of bussiness
    Returns:
    Bussiness Day in format '%d-%b-%Y' 
    """
    for i in range(1,days+1):
        if (
            ((today - timedelta(days=i)).strftime(DATE_FORMAT_B) not in [h['tradingDate'] for h in holidays])
            and ((today - timedelta(days=i)).strftime('%A') not in EXCLUSIONS)):
                return (today - timedelta(days=i))
    return None

def map_symbol_name(symbol):
    """
    Old new symbol  mappings
    Args:symbol

    Returns:
        New Symbol mapped if not found returns sent symbol as is

    """
    symbol_map = {
        'LTI': 'LTIM',
            'COFORGE': 'NIITTECH',
        # 'ZYDUSLIFE': 'CADILAHC',
        # 'SHRIRAMFIN':'SRTRANSFIN'
    }
    return symbol_map.get(symbol, symbol)

def get_strike(price,step):
    """
    Args:

    Futures closing  of the Stock  and the strike difference (Step)

    Returns:

    Strike of the stock

    """
    reminder = price%step
    if reminder < (step/2):
        price= float(price-reminder) if  isinstance(step,float) else int(price-reminder)
    else:
        price=float(price+(step-reminder)) if isinstance(step,float) else   int(price+(step-reminder))
    return f'{price:.2f}'
def get_week(days_to_expiry:int)->str:
    """
    It accepts  number of days to expiry and return  the  week to expiry
    Args:
    days_to_expiry 
    
    """
    if days_to_expiry > 26:
        return 'week5'
    if days_to_expiry > 19:
        return 'week4'
    if days_to_expiry > 12:
        return 'week3'
    if days_to_expiry > 5:
        return 'week2'
    if days_to_expiry > -1:
        return 'week1'
    return 'expired'


# Function to check if a given date is a holiday
def is_holiday(today,holidays):
  """
    Checks if a given date is a holiday.

    Parameters:
    today (datetime.date): The date to check.
    holidays (list of datetime.date): A list of holidays.

    Returns:
    bool: True if the date is a holiday, False otherwise.
    """
  return (today.strftime(DATE_FORMAT_B) in [h['tradingDate'] for h in holidays])

# Function to add a given number of working days to a start date
def add_working_days(start_date:datetime, num_days:int, holidays):
        
    """
    Adds a given number of working days to a start date.

    Parameters:
    start_date (datetime.date): The start date.
    num_days (int): The number of working days to add.
    holidays (list of datetime.date): A list of holidays.

    Returns:
    datetime.date: The end date after adding the specified number of working days.
    """
    # Initialize variables
    current_date = start_date
    days_added = 0
    
    # Loop until the desired number of working days have been added
    while days_added < num_days:
        # Add one day to the current date
        current_date += timedelta(days=1)
        
        # Check if the current date is a weekend or holiday
        weekday = current_date.weekday()
        if weekday >= 5 or is_holiday(current_date, holidays):
            continue
        
        # If the current date is a working day, increment the days_added counter
        days_added += 1
    
    # Return the end date
    return current_date

def update_record(record: dict, columns: list, date_of_trade: datetime) -> InsertOne:
    """
    Update a record and return an instance of pymongo InsertOne.
    
    Args:
    - record (dict): The record to be updated.
    - columns (list): The list of columns for the record.
    - date_of_trade (datetime): The date when the trade was made.
    
    Returns:
    - pymongo.InsertOne: An instance of pymongo InsertOne.
    """
    # Delete unnecessary keys
    del record['Date']
    del record['two_months_week_min_coverage']
    del record['current_vs_prev_two_months']

    # Add created_at and price keys
    record['created_at'] = date_of_trade
    record['price'] = record[columns[2]]
    
    return InsertOne(record)
# # Example usage
# start_date = datetime.date(2023, 5, 9)
# num_days = 10
# holidays = [datetime.date(2023, 5, 10), datetime.date(2023, 5, 14)]

# end_date = add_working_days(start_date, num_days, holidays)
# print(end_date)
