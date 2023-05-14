from datetime import timedelta,date
from matplotlib import pyplot as plt
from data.constants import CLOSE_POSITION_AFTER, NO_OF_TRADES
from magic_engine import OptionWizard
initial_capital=400000
def backtest_strategy_mine(option_wizard:OptionWizard,start_month_date:date,end_month_date:date):
    '''
    This function backtests a given option trading strategy.

    Parameters:
    option_wizard (OptionWizard): An instance of the OptionWizard class.
    start_month_date (date): A date object representing the start month.
    end_month_date (date): A date object representing the end month.
    initial_capital (float): Initial capital to start trading.

    Returns:
    None
    '''
    current_date = start_month_date
    pnl_history = []
    trade_dates = []
    total_capital = initial_capital

    while (end_month_date - current_date).days > 0:
        record = option_wizard.find_cheapest_options(n=NO_OF_TRADES, input_date=current_date, back_test=True)
        record['cheapest_options'] = [d for d in record['cheapest_options'] if d['expiry'] > d['Date']]
        if len(record['cheapest_options']) == 0:
            current_date += timedelta(days=1)
            continue
        
        trade_date = option_wizard.get_trade_date(record['day'])
        print(f"trade Date--------------{trade_date}------------")
        option_wizard.order_manager.place_orders(cheapest_records=record['cheapest_options'], trade_date=trade_date)
        option_wizard.fno_downloader.download_options_for_pnl()
        portfolio = option_wizard.get_portfolio_pnl(total_capital)
        
        if portfolio['total_capital'] > 0:
            pnl = round(portfolio['pnl'], 2)
            returns = round((portfolio['pnl'] / total_capital) * 100, 2)
            print(f"Your Portfolio P&L: {pnl}")
            print(f"Capital used: {portfolio['used_capital']}")
            print(f"Your total returns: {returns}%")
            # pnl_history.append(pnl)
            trade_dates.append(trade_date)
            total_capital += pnl
            pnl_history.append(total_capital)
        else:
            pnl_history.append(0)
            
        option_wizard.order_manager.close_week_orders()
        current_date = current_date + timedelta(days=CLOSE_POSITION_AFTER)
        
    total_profits = total_capital-initial_capital
    option_wizard.telegram.telegram_bot(
        f"total_profit = {total_profits},\n total_returns={round((total_profits / initial_capital) * 100, 2)}\n initial_Capital={initial_capital}\nfinal_capital={total_capital}")
    # pnl_cumsum = [total_capital + sum(pnl_history[:i + 1]) for i in range(len(pnl_history))]
    plt.plot(trade_dates, pnl_history)
    plt.xlabel('Trade Date')
    plt.ylabel('Profit/Loss')
    plt.title('Backtest Results')
    plt.show()
    
def backtest_me(option_wizard:OptionWizard,start_month_date,end_month_date):
     backtest_strategy_mine(option_wizard,start_month_date,end_month_date)
     