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
    days=(end_month_date-start_month_date).days
    pnl_history = []
    while days>0:
            record=option_wizard.find_cheapest_options(n=15,no_of_days_back=days)
            # option_wizard.send_to_telegram(cheapest_records= record['cheapest_options'],today=record['day'])
            trade_date = option_wizard.get_trade_date(record['day'])
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
def backtest_me(option_wizard:OptionWizard,start_month_date,end_month_date):
     backtest_strategy_mine(option_wizard,start_month_date,end_month_date)
     