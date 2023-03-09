requirements
we need to have mongodb instance running either it will be locally or on remote server

install all the required packages to run this script 
download strikes information: https://archives.nseindia.com/content/fo/sos_scheme.xls
Download lots information: https://archives.nseindia.com/content/fo/fo_mktlots.csv

todo:
1) create UI to see the top n cheapest straddles
2) dockerize the script
3) create web server to host  it as application

http://127.0.0.1:5000/api/v1/options?symbol=ABCAPITAL&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
http://127.0.0.1:5000/api/v1/straddles?symbol=ABCAPITAL&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
http://127.0.0.1:5000/api/v1/straddles?symbol=JKCEMENT&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5
http://127.0.0.1:5000/api/v1/futures?symbol=JKCEMENT&from_date=2022-12-30&to_date=2023-02-23&weeks_to_expiry=week5