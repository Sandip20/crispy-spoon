"""
Utility Module 
"""
from datetime import timedelta,datetime
import pandas as pd

from data.constants import DATE_FORMAT_B
exclusions=["Saturday","Sunday"]
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

def get_next_business_day(today,holidays,days=1):
    """
    Args:
    holidays:List Nse holidays
    exclusions:List exclusions of the days
    days:int total days of bussiness
    Returns:
    Bussiness Day in format '%d-%b-%Y' 
    """
    for i in range(1, days+1):
        if (
            ((today + timedelta(days=i)).strftime(DATE_FORMAT_B) not in [h['tradingDate'] for h in holidays])
            and ((today + timedelta(days=i)).strftime('%A') not in exclusions)):
                return (today + timedelta(days=i)).strftime(DATE_FORMAT_B)

def get_last_business_day(today,holidays,days=5)->datetime:
    """
    Args:
    holidays:List Nse holidays
    exclusions:List exclusions of the days
    days:int total days of bussiness
    Returns:
    Bussiness Day in format '%d-%b-%Y' 
    """
    for i in range(1,days+1):
        if (
            ((today - timedelta(days=i)).strftime(DATE_FORMAT_B) not in [h['tradingDate'] for h in holidays])
            and ((today - timedelta(days=i)).strftime('%A') not in exclusions)):
                return (today - timedelta(days=i))

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