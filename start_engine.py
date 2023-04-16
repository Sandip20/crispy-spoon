# pylint: disable=missing-module-docstring
from magic_engine_v2 import OptionWizard

option_wizard=OptionWizard()
option_wizard.connect_mongo()

# Filter out only the method names
# method_names = [attr for attr in class_attributes if callable(getattr(OptionWizard, attr)) and not attr.startswith("__")]

# # Print the method names
# print(method_names)
"""
It will download the files
fo_mklots
sos_schemes which contains strike information and lot size it will updated in to the collection stocks_step
run this every quarter to keep updated files
"""
# option_wizard.update_stocks_info()


# option_wizard. get_month_fut_history('COFORGE',2022,11)
# option_wizard.update_to_latest()
option_wizard.update_to_latest_v3()
# start_date=pd.to_datetime(date(2023,3,15))#  this will include Feb month expiry  data
# end_date=pd.to_datetime(date(2023,3,16))# this will include March month expiry data
# option_wizard.download_historical_v3(start_date,end_date)
# option_wizard.update_current_vs_prev_two_months(start_date=start_date,end_date=end_date)
# option_wizard.download_historical(start_date=start_date,end_date=end_date)

# print(option_wizard.map_symbol_name('LTIM'))
# option_wizard.get_oneday_options_history('NIITTECH','CE', date.today()-timedelta(days=1),option_wizard.get_expiry(2023,3),4300)
# df=option_wizard.get_month_fut_history('RELIANCE',2023,3)
# print(df.shape)
# start_month_date=pd.to_datetime(date(2022,11,1))
# end_month_date=pd.to_datetime(date.today())
# days=(end_month_date-start_month_date).days
# of_date=pd.to_datetime(date(2022,10,31))
record = option_wizard.find_cheapest_options(n=15)
option_wizard.send_to_telegram(cheapest_records=record['cheapest_options'], today=record['day'])
# option_wizard.place_orders(cheapest_records=record['cheapest_options'], trade_date=record['day'])

# option_wizard.download_options_for_pnl(back_test=False)
# portfolio = option_wizard.get_portfolio_pnl()
# if portfolio['total_capital'] > 0:
#             pnl = round(portfolio['pnl'], 2)
#             total_capital = portfolio['total_capital']
#             returns = round((pnl / total_capital) * 100, 2)
#             print(f"Your Portfolio P&L: {pnl}")
#             print(f"Capital used: {total_capital}")
#             print(f"Your total returns: {returns}%")
def backtest_strategy(start_month_date, end_month_date):
    days = (end_month_date - start_month_date).days
    pnl_history = []

    while days > 0:
        record = option_wizard.find_cheapest_options(n=15, no_of_days_back=days)
        trade_date = record['day'] + timedelta(days=1)
        print(f"Trade Date--------------{trade_date}------------")
        option_wizard.place_orders(cheapest_records=record['cheapest_options'], trade_date=trade_date)
        option_wizard.download_options_for_pnl(back_test=True)
        portfolio = option_wizard.get_portfolio_pnl()

        if portfolio['total_capital'] > 0:
            pnl = round(portfolio['pnl'], 2)
            total_capital = portfolio['total_capital']
            returns = round((pnl / total_capital) * 100, 2)
            print(f"Your Portfolio P&L: {pnl}")
            print(f"Capital used: {total_capital}")
            print(f"Your total returns: {returns}%")
            pnl_history.append(pnl)
        else:
            pnl_history.append(0)
        option_wizard.close_week_orders()
        days -= 7

    pnl_cumsum = [sum(pnl_history[:i+1]) for i in range(len(pnl_history))]
    print(sum(pnl_history))
    # Plot backtest results
    plt.plot(pnl_cumsum)
    plt.xlabel('Weeks')
    plt.ylabel('Profit/Loss')
    plt.title('Backtest Results')
    plt.show()
def backtest_strategy_mine(start_month_date,end_month_date):
    days=(end_month_date-start_month_date).days
    pnl_history = []
    while days>0:
            record=option_wizard.find_cheapest_options(n=15,no_of_days_back=days)
            # option_wizard.send_to_telegram(cheapest_records= record['cheapest_options'],today=record['day'])
            trade_date=(record['day']+timedelta(days=1))
            print(f"trade Date--------------{trade_date}------------")
            option_wizard.place_orders(cheapest_records= record['cheapest_options'],trade_date=trade_date)
            option_wizard.download_options_for_pnl(back_test=True)
            portfolio=option_wizard.get_portfolio_pnl()
            if(portfolio['total_capital']>0 ):
                pnl=round(portfolio['pnl'],2)
                total_capital=portfolio['total_capital']
                returns= round((portfolio['pnl']/portfolio['total_capital'])*100,2)
                print(f"Your Portfolio P&L:{pnl}")
                print(f"Capital used: {total_capital}")
                print(f"Your total returns:{returns}")
                pnl_history.append(pnl)
            else:
                pnl_history.append(0)
            option_wizard.close_week_orders()
            days-=7
    print(sum(pnl_history))
# backtest_strategy(start_month_date,end_month_date)