"""
Processed options queries
"""
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