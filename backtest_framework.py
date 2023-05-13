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
from datetime import timedelta,date

from matplotlib import pyplot as plt
from data.constants import CLOSE_POSITION_AFTER, NO_OF_TRADES

from magic_engine import OptionWizard


# def backtest_strategy(start_month_date, end_month_date):
#     days = (end_month_date - start_month_date).days
#     pnl_history = []
#     while days > 0:
#         record = option_wizard.find_cheapest_options(n=15, no_of_days_back=days)
#         trade_date = record['day'] + timedelta(days=1)
#         print(f"Trade Date--------------{trade_date}------------")
#         option_wizard.place_orders(cheapest_records=record['cheapest_options'], trade_date=trade_date)
#         option_wizard.download_options_for_pnl(back_test=True)
#         portfolio = option_wizard.get_portfolio_pnl()

#         if portfolio['total_capital'] > 0:
#             pnl = round(portfolio['pnl'], 2)
#             total_capital = portfolio['total_capital']
#             returns = round((pnl / total_capital) * 100, 2)
#             print(f"Your Portfolio P&L: {pnl}")
#             print(f"Capital used: {total_capital}")
#             print(f"Your total returns: {returns}%")
#             pnl_history.append(pnl)
#         else:
#             pnl_history.append(0)
#         option_wizard.close_week_orders()
#         days -= 7

#     pnl_cumsum = [sum(pnl_history[:i+1]) for i in range(len(pnl_history))]
#     print(sum(pnl_history))
#     # Plot backtest results
#     plt.plot(pnl_cumsum)
#     plt.xlabel('Weeks')
#     plt.ylabel('Profit/Loss')
#     plt.title('Backtest Results')
#     plt.show()

def backtest_strategy_mine(option_wizard:OptionWizard,start_month_date:date,end_month_date:date):
    
    current_date=start_month_date
    pnl_history = []
    trade_dates=[]
    while (end_month_date-current_date).days >0:
        record=option_wizard.find_cheapest_options(n=NO_OF_TRADES,input_date=current_date,back_test=True)
        record['cheapest_options']=[d for d in record['cheapest_options'] if d['expiry']> d['Date']]
        if len(record['cheapest_options'])==0:
            current_date+=timedelta(days=1)
            continue
        
        trade_date = option_wizard.get_trade_date(record['day'])
        print(f"trade Date--------------{trade_date}------------")
        option_wizard.place_orders(cheapest_records= record['cheapest_options'],trade_date=trade_date)
        option_wizard.download_options_for_pnl(back_test=True)
        portfolio=option_wizard.get_portfolio_pnl()
        if portfolio['total_capital']>0      :
            pnl=round(portfolio['pnl'],2)
            total_capital=portfolio['total_capital']
            returns= round((portfolio['pnl']/portfolio['total_capital'])*100,2)
            print(f"Your Portfolio P&L:{pnl}")
            print(f"Capital used: {total_capital}")
            print(f"Your total returns:{returns}")
            pnl_history.append(pnl)
            trade_dates.append(trade_date)
        else:
            pnl_history.append(0)
        option_wizard.close_week_orders()
        current_date=current_date+timedelta(days=CLOSE_POSITION_AFTER)
    total_profits=sum(pnl_history)
    
    option_wizard.telegram.telegram_bot(f"total_profit={total_profits}")
    pnl_cumsum = [sum(pnl_history[:i+1]) for i in range(len(pnl_history))]
    plt.plot(trade_dates,pnl_cumsum)
    plt.xlabel('Trade Date')
    plt.ylabel('Profit/Loss')
    plt.title('Backtest Results')
    plt.show()
# backtest_strategy(start_month_date,end_month_date)
def backtest_me(option_wizard:OptionWizard,start_month_date,end_month_date):
     backtest_strategy_mine(option_wizard,start_month_date,end_month_date)
     