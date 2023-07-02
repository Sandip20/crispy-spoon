"""
In this module  we have all the constants required for the current project
"""
MONTHS_IN_YEAR = 12
DATE_FORMAT = '%d-%m-%Y'  # it will give you date in the format 01-03-2023 DD-MM-YYYY
# it will give you date in the format 01-MAR-2023 DD-MMM-YYYY
DATE_FORMAT_B = '%d-%b-%Y'
NSE_HOST = 'https://www.nseindia.com'
EXCLUSIONS = ["Saturday", "Sunday"]
EXCEPTION_DATE='29-JUNE-2023'# The expiry date for NSE and BSE contracts was shifted from June 29th to June 28th. Only the day on which the expiry occurred was changed; the expiry date of contract remained the same.
CLOSE_POSITION_AFTER=14 #  7 ,14 next position after 14 days
NO_OF_TRADES=2# max number of trades 1=6%,2=25%,3=23%,4=31%,5=27%,6=
NO_OF_WORKING_DAYS_END_CALCULATION=9 # 4 days for one week and 9 days for two week
MAX_LOSS_PER_POSITION=-50000#max loss of the single position if it hits 8000 then close the 
# https://www.nseindia.com/api/historical/fo/derivatives?symbol=CROMPTON&from=17-04-2023&to=28-04-2023&expiryDate=27-Apr-2023&instrumentType=OPTSTK&strikePrice=295.00&optionType=PE