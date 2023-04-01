

from flask import Flask, jsonify, request
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timezone
from dotenv import load_dotenv
import pytz
import os 
load_dotenv()

app = Flask(__name__)

# MongoDB configuration
client=MongoClient(f"mongodb://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@localhost:27017/?retryWrites=true&w=majority")
db = client[f"{os.environ['MONGO_INITDB_DATABASE']}"]
          
COMMON_ROUTE_V1="/api/v1"
# API route to retrieve data based on date
@app.route(f'{COMMON_ROUTE_V1}/options', methods=['GET'])
def get_options_data():
    try:
        collection=db['atm_stock_options']
        symbol = request.args.get('symbol')
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        weeks_to_expiry=request.args.get('weeks_to_expiry')
        # Convert input date string to datetime object
        # from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
        # to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
        # Query MongoDB collection for documents with matching date
        query={
            "Symbol":symbol,
          "weeks_to_expiry":weeks_to_expiry,
            "Date":{
                        "$gte": pd.to_datetime((from_date)),
                        "$lt": pd.to_datetime((to_date))
                        }
        }
        
        
        
        print(query)
        documents = collection.find(query,{
            "_id":0
        })
        # Convert MongoDB cursor to list of dictionaries
        data = list(documents)
        # Return data as JSON response
        return jsonify(data), 200
    except ValueError:
        # Handle invalid date format
        return jsonify({'error': 'Invalid date format. Please use YYYY-MM-DD.'}), 400
    except Exception as e:
        # Handle other exceptions
        return jsonify({'error': str(e)}), 500
@app.route(f'{COMMON_ROUTE_V1}/straddles', methods=['GET'])
def get_straddles_data():
    try:
        collection=db['options_straddle']
        symbol = request.args.get('symbol')
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        weeks_to_expiry=request.args.get('weeks_to_expiry')
        # Convert input date string to datetime object
        # from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
        # to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
        # Query MongoDB collection for documents with matching date
        query={
            "symbol":symbol,
            "Date":{
                        "$gte": pd.to_datetime((from_date)),
                        "$lte": pd.to_datetime((to_date))
                        },
            "weeks_to_expiry":weeks_to_expiry
        }
        documents = collection.find(query,{
            "_id":0
        })
        # Convert MongoDB cursor to list of dictionaries
        data = list(documents)
        # Return data as JSON response
        return jsonify(data), 200
    except ValueError:
        # Handle invalid date format
        return jsonify({'error': 'Invalid date format. Please use YYYY-MM-DD.'}), 400
    except Exception as e:
        # Handle other exceptions
        return jsonify({'error': str(e)}), 500
@app.route(f'{COMMON_ROUTE_V1}/futures', methods=['GET'])
def get_futures_data():
    try:
        collection=db['stock_futures']
        symbol = request.args.get('symbol')
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        weeks_to_expiry=request.args.get('weeks_to_expiry')
        from_date= datetime.strptime(from_date, "%Y-%m-%d").date()
        print("from_date",from_date)
        to_date= datetime.strptime(to_date, "%Y-%m-%d").date()
        # Query MongoDB collection for documents with matching date
        query=[
                    {
                        '$project': {
                            'Symbol': 1, 
                            '_id': 0, 
                            'Close': 1, 
                            'Date': 1, 
                            'Expiry': 1, 
                            'weeks_to_expiry': {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$gt': [
                                                    {
                                                        '$subtract': [
                                                            '$Expiry', '$Date'
                                                        ]
                                                    }, 26 * 24 * 60 * 60 * 1000
                                                ]
                                            }, 
                                            'then': 'week5'
                                        }, {
                                            'case': {
                                                '$gt': [
                                                    {
                                                        '$subtract': [
                                                            '$Expiry', '$Date'
                                                        ]
                                                    }, 19 * 24 * 60 * 60 * 1000
                                                ]
                                            }, 
                                            'then': 'week4'
                                        }, {
                                            'case': {
                                                '$gt': [
                                                    {
                                                        '$subtract': [
                                                            '$Expiry', '$Date'
                                                        ]
                                                    }, 12 * 24 * 60 * 60 * 1000
                                                ]
                                            }, 
                                            'then': 'week3'
                                        }, {
                                            'case': {
                                                '$gt': [
                                                    {
                                                        '$subtract': [
                                                            '$Expiry', '$Date'
                                                        ]
                                                    }, 5 * 24 * 60 * 60 * 1000
                                                ]
                                            }, 
                                            'then': 'week2'
                                        }, {
                                            'case': {
                                                '$gt': [
                                                    {
                                                        '$subtract': [
                                                            '$Expiry', '$Date'
                                                        ]
                                                    }, -1
                                                ]
                                            }, 
                                            'then': 'week1'
                                        }, {
                                            'case': True, 
                                            'then': 'expired'
                                        }
                                    ]
                                }
                            }, 
                            'days_to_expiry': {
                                '$divide': [
                                    {
                                        '$subtract': [
                                            '$Expiry', '$Date'
                                        ]
                                    }, 24 * 60 * 60 * 1000
                                ]
                            }
                        }
                    }, 
                    {
        '$match': {
            'weeks_to_expiry': weeks_to_expiry,
            'Symbol':symbol,
            "Date":{
                           "$gte":datetime(from_date.year, from_date.month, from_date.day, 0, 0, 0, tzinfo=timezone.utc),
                             "$lt": datetime(to_date.year, to_date.month, to_date.day, 0, 0, 0, tzinfo=timezone.utc)
                         },
            
        }
    }
     
            ]
        documents = collection.aggregate(query)
        # Convert MongoDB cursor to list of dictionaries
        # data = [item for item in list(documents) iterm['Date']=item['Date'].to_datetime().replace(tzinfo=pytz.utc)]
        data=list(documents)
        # data = [{'Date': item['Date'].replace(tzinfo=pytz.timezone('Asia/Kolkata')), **item} for item in list(documents)]
        # Return data as JSON response
        return jsonify(data), 200
    except ValueError:
        # Handle invalid date format
        return jsonify({'error': 'Invalid date format. Please use YYYY-MM-DD.'}), 400
    except Exception as e:
        # Handle other exceptions
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
