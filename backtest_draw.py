import os
from datetime import date
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
base_url = f"mongodb+srv://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}"
client=MongoClient(base_url)
db = client[f"{os.environ['MONGO_INITDB_DATABASE']}"]
closed_order=db[os.environ['CLOSED_POSITIONS_COLLECTION_NAME']]


start_date=date(2022,10,1)
end_date=date(2023,5,20)
pipeline = [
    {
        "$match": {
            "created_at": {
                "$gte": pd.to_datetime(start_date),
                "$lte": pd.to_datetime(end_date)
            }
        }
    },
    {
        "$group": {
            "_id": "$exit_date",
            "total_profit_loss": { "$sum": "$profit_loss" }
        }
    },
    {
    "$sort": {
        "_id": 1
    }
},
    {
        "$project":{
            "exit_date":"$_id",
            "profit_loss" :"$total_profit_loss"
        }
    }
]
result=closed_order.aggregate(pipeline)
data=list(result)
# Extract relevant data for PNL chart
dates = [data_point['exit_date'] for data_point in data]
pnl = [ data_point['profit_loss'] for data_point in data]

pnl_cumsum = [sum(pnl[:i + 1]) for i in range(len(pnl))]
# Plot the PNL chart

fig,(ax1,ax2)=plt.subplots(2,1,figsize=(10,8),sharex=True)
ax1.plot(dates,pnl_cumsum,marker='o')
ax1.set_xlabel('Date')
ax1.set_ylabel('PNL')
ax1.set_title('Profit and Loss Chart')

ax1.grid()

pnl_cumsum_series=pd.Series(pnl_cumsum)
drawdown = pnl_cumsum_series - pnl_cumsum_series.cummax()

# Plot the drawdown chart
ax2.plot(dates, drawdown)
ax2.set_xlabel('Date')
ax2.set_ylabel('Drawdown')
ax2.set_title('Drawdown Chart')
plt.tight_layout()
plt.show()
