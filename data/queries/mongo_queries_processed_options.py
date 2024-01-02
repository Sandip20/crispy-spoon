"""
Processed options queries
"""
from datetime import timezone,datetime
from typing import Any, Dict, List

from data.constants import EXCLUDE_SYMBOLS

ADD_CE_PE_PIPELINE = [
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

def create_week_min_query(from_expiry_date: datetime, to_expiry_date: datetime) -> List[Dict]:
    """
    Returns a MongoDB aggregation pipeline query for computing the minimum coverage
    of each symbol and weeks-to-expiry combination, for options data documents in the
    given date range.

    Args:
    - from_expiry_date (datetime): the starting date for the range (inclusive)
    - to_expiry_date (datetime): the ending date for the range (inclusive)

    Returns:
    - A list of MongoDB aggregation pipeline stages as dictionaries, that can be used
      with the `aggregate()` method of a `pymongo` collection object to compute the
      desired query result.
    """
    pipeline = [
        {
            "$match": {
                "Date": {
                    "$lte": to_expiry_date,
                    "$gte": from_expiry_date
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "weeks_to_expiry": "$weeks_to_expiry",
                    "symbol": "$symbol",
                    "Expiry": "$Expiry"
                },
                "week_min_coverage": {
                    "$min": "$%coverage"
                }
            }
        },
        {
            "$project": {
                "week_min_coverage": 1,
                "symbol": "$_id.symbol",
                "Expiry": "$_id.Expiry",
                "weeks_to_expiry": "$_id.weeks_to_expiry",
                "_id": 0
            }
        }
    ]
    return pipeline

def create_find_cheapest_options_query(today:datetime,n:int)->List[Dict[str, Any]]:
    """
    Returns a MongoDB query pipeline that filters and aggregates documents based on the provided criteria.

    Args:
        today (date): A `date` object representing the current date.
        n (int): An integer indicating the maximum number of documents to return.

    Returns:
        List[Dict[str, Any]]: A list of MongoDB query pipeline stages, as Python dictionaries.
    """

    return [
            {
                '$match': {
                    'Date': datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=timezone.utc),
                     "strike":{"$lt":7000},
                    'two_months_week_min_coverage': {'$ne': float('nan')},
                    'current_vs_prev_two_months': {'$gte': -5, '$lte': 0}
                }
            },
            {
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
                    'distinct_val': {'$addToSet': '$Date'}
                }
            },
            {'$unwind': {'path': '$distinct_val', 'preserveNullAndEmptyArrays': True}},
            {
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
                    'expiry': '$_id.Expiry',
                    '_id': 0
                }
            },
            {'$sort': {'current_vs_prev_two_months': 1}},
            {'$limit': n}
    ]