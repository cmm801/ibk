import datetime
import pathlib


IB_PATH = pathlib.Path(__file__).parent.absolute()  # The path to this module

PORT_PROD = 7496   # The port used by the production account
PORT_PAPER = 7497  # The port used by paper trading account

# The letter codes used in futures symbols for each month (from January through Dec.)
FUTURES_MONTH_SYMBOLS = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

# Different time zones
TIMEZONE_EST = 'US/Eastern'
TIMEZONE_CET = 'Europe/Zurich'
TIMEZONE_UTC = 'UTC'
TIMEZONE_LOC = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo

# The timezone specified at login to TWS. All historical data refer to this timezone.
TWS_TIMEZONE = 'US/Eastern'