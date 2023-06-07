# pylint: disable=missing-module-docstring
from backtest_framework import backtest_me
from magic_engine import OptionWizard
from datetime import date
option_wizard = OptionWizard()
# option_wizard.connect_mongo()

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
option_wizard.update_daily()
record = option_wizard.find_cheapest_options(n=15)
option_wizard.send_to_telegram(record['cheapest_options'], record['day'])
start_month_date = date(2022,10, 1)
end_month_date = date.today()
backtest_me(option_wizard, start_month_date, end_month_date)
