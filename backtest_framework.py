from datetime import timedelta, date
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
from data.constants import CLOSE_POSITION_AFTER, NO_OF_TRADES
from magic_engine import OptionWizard

brokerage = 200
slippage = 0.01
def backtest_strategy_mine(option_wizard: OptionWizard, start_month_date: date, end_month_date: date, initial_capital: float):
    """
    Backtests a given option trading strategy and displays the results.

    Parameters:
    option_wizard (OptionWizard): An instance of the OptionWizard class.
    start_month_date (date): A date object representing the start month.
    end_month_date (date): A date object representing the end month.
    initial_capital (float): Initial capital to start trading.

    Returns:
    None
    """

    current_date = start_month_date
    pnl_history = []
    trade_dates = []
    total_capital = initial_capital
    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_profit = 0
    total_returns = 0
    sharpe_ratio = 0

    while (end_month_date - current_date).days > 0:
        # Find the cheapest options for the current date
        record = option_wizard.find_cheapest_options(n=NO_OF_TRADES, input_date=current_date, back_test=True)

        # Filter out options that have already expired
        record['cheapest_options'] = [
            d for d in record['cheapest_options'] 
            if d['expiry'] > d['Date'] and d['days_to_expiry']>2
            ]

        if len(record['cheapest_options']) == 0:
            current_date += timedelta(days=1)
            continue

        trade_date = option_wizard.get_trade_date(record['day'])

        # Clear trades for the input date only
        option_wizard.order_manager.clear_existing_trades(trade_date=trade_date)

        # Place orders for the selected cheapest options
        option_wizard.order_manager.place_orders(cheapest_records=record['cheapest_options'], trade_date=trade_date)

        # Download options data for P&L calculation
        option_wizard.fno_downloader.download_options_for_pnl()

        # Get portfolio P&L and calculate returns
        portfolio = option_wizard.get_portfolio_pnl_v2(total_capital, slippage=slippage, brokerage=brokerage)
        current_date += timedelta(days=CLOSE_POSITION_AFTER)
        # current_date=add_working_days(pd.to_datetime(trade_date), NO_OF_WORKING_DAYS_END_CALCULATION, option_wizard.holidays).date()
     
        
        # Close week orders
        option_wizard.order_manager.close_week_orders(portfolio['symbols'])
        
        if portfolio['total_capital'] > 0:
            pnl = round(portfolio['pnl'], 2)
            returns = round((portfolio['pnl'] / total_capital) * 100, 2)
            pnl_history.append(pnl)
            trade_dates.append(trade_date)
            
            total_trades += 1
            total_profit += pnl
            total_returns += returns
            
            if pnl > 0:
                total_wins += 1
            total_capital += portfolio['pnl']
        else:
            pnl_history.append(0)

    
     
    if total_trades==0:
        return
    total_losses = total_trades - total_wins
    total_profit = round(total_capital - initial_capital, 2)
    total_returns = round((total_profit / initial_capital) * 100, 2)

    # Calculate performance metrics
    win_rate = (total_wins / total_trades) * 100
    returns_series = pd.Series(pnl_history)
    daily_returns = returns_series.pct_change().fillna(0)
    sharpe_ratio = np.sqrt(252) * (daily_returns.mean() / daily_returns.std())

    print("Total Trades:", total_trades)
    print("Total Wins:", total_wins)
    print("Total Losses:", total_losses)
    print("Total Profit:", total_profit)
    print("Total Returns:", total_returns)
    print("Win Rate:", win_rate)
    print("Sharpe Ratio:", sharpe_ratio)
    open_positions=""

    pnl=0
    open_position_count=0
    for _,position in portfolio['symbols'].items():
        if position['status']=='OPEN':
            open_position_count+=1
            open_positions+=f""" 
            { position['created_at']} | {position['symbol']} | {position['strike']} | {position['pnl']} \n
            """  
            pnl+=position['pnl']

    open_positions+=f""" 
        Trade Profit_Loss:{pnl} \n
        """
    option_wizard.telegram.telegram_bot(
        f""" ------------ Open Positions: {open_position_count}------------------ \n
{open_positions if open_position_count > 0 else " "}
        ------------------------------------------------- \n
        Total Trades: {total_trades}\n Total Wins:{total_wins}\n Total Losses:{total_losses}\n Total Returns:{total_returns}\n Win Rate:{win_rate}\n Sharpe Ratio:{sharpe_ratio}\n """)
    # Plotting the backtest results
    # plt.plot(trade_dates, pnl_history)
    # plt.xlabel('Trade Date')
    # plt.ylabel('Profit/Loss')
    # plt.title('Backtest Results')
    # plt.show()


def backtest_me(option_wizard: OptionWizard, start_month_date, end_month_date):
    initial_capital = 1000000
    backtest_strategy_mine(option_wizard, start_month_date, end_month_date,initial_capital)
