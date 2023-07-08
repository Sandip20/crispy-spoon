import os
from datetime import date
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pymongo import MongoClient
initial_capital = 400000
brokerage = 200
slippage = 0.01
load_dotenv()
base_url = f"mongodb+srv://{os.environ['MONGO_INITDB_ROOT_USERNAME']}:{os.environ['MONGO_INITDB_ROOT_PASSWORD']}@{os.environ['MONGO_INITDB_HOST']}"
client=MongoClient(base_url)
db = client[f"{os.environ['MONGO_INITDB_DATABASE']}"]
closed_order=db[os.environ['CLOSED_POSITIONS_COLLECTION_NAME']]
start_date=date(2022,10,1)
end_date=date.today()

df=pd.DataFrame(list(closed_order.find({}))).drop(columns=['_id'])

df['pnl']=(df['exit_price']-df['buy_price']) * df['quantity']


print(df[['exit_date','exit_date','pnl']])
print('total pnl:',df['pnl'].sum())
print("max Loss:",round(df['pnl'].min(),2))
print("max Profit:",round(df['pnl'].max(),2))
print("avg pnl",round(df['pnl'].mean()))

pnl_cumsum=np.cumsum(df['pnl']).tolist()
print(f"total_profit or loss:{df['pnl'].sum()}")
fig,(ax1,ax2)=plt.subplots(2,1,figsize=(15,8),sharex=True)
ax1.plot(df['exit_date'],pnl_cumsum,lw=1.5)
# ax1.plot(dates,pnl_cumsum,'ro')
ax1.set_xlabel('Date')
ax1.set_ylabel('PNL')
ax1.set_title('Profit and Loss')

pnl_cumsum_series=pd.Series(pnl_cumsum)
drawdown = pnl_cumsum_series - pnl_cumsum_series.cummax()
# ax1.plot(dates, drawdown,'r',lw=1.5,marker='o')
# Plot the drawdown chart
# ax2.bar(dates, drawdown,width=2,color="r")
ax2.plot(df['exit_date'], drawdown,'r',lw=1.5)
ax2.set_xlabel('Date')
ax2.set_ylabel('Drawdown')
ax2.set_title('Drawdown')
# plt.tight_layout()
plt.show()
