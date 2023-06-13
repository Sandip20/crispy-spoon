# Cheapest Stock Options Straddle

### what is cheapest stock options strddle?
seek atm options contracts with lower premiums. we choose the stocks that is having lower premium as compared to other stocks.
also we look back  two months minimum coverage by the atm straddle 

1) download the futures OHLC of the  190 stocks
2) based on closing price of each stock futures we decide the  ATM strikes CE and PE
3)Download the ATM CE and PE
4) perform the calculations of ADDing the same date CE PE and inserts into straddles collection of mongo
5) update week minimum of the last two months
6) get the list of cheapest stocks options straddle for the given day as input 
7) create backtest strategy  to run the simulation


https://www.nseindia.com/all-reports-derivatives#cr_deriv_equity_archives


requirements
we need to have mongodb instance running either it will be locally or on remote server

install all the required packages to run this script 
download strikes information: https://archives.nseindia.com/content/fo/sos_scheme.xls
Download lots information: https://archives.nseindia.com/content/fo/fo_mktlots.csv
Buying opportunity:
Look for cheapeast as well as  wait for the move if
first standard deviation jaise hi move aati hai we need to enter 
first_standard_deviation =Volatility(IV) / 16.2
for example stocks iv is around 30
wait for 1.5% or 2% then buy and hold the stock


todo:
1) create UI to see the top n cheapest straddles
2) dockerize the script
3) create web server to host  it as application

/api/v1/options?symbol=ABCAPITAL&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
/api/v1/straddles?symbol=ABCAPITAL&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
/api/v1/straddles?symbol=JKCEMENT&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
/api/v1/futures?symbol=JKCEMENT&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
https://www.nseindia.com/products-services/equity-derivatives-list-underlyings-information

https://www.nseindia.com/products-services/equity-derivatives-nifty50
