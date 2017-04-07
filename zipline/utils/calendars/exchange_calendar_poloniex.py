from datetime import time
from itertools import chain

from pandas.tslib import Timestamp
from pytz import timezone

from zipline.utils.calendars import TradingCalendar
from zipline.utils.calendars.trading_calendar import HolidayCalendar

class POLONIEXExchangeCalendar(TradingCalendar):
    """
    Exchange calendar for Poloniex US.

    Open Time: 12am, US/Eastern
    Close Time: 11:59pm, US/Eastern

    """
    @property
    def name(self):
        return "POLONIEX"

    @property
    def tz(self):
        return timezone("US/Eastern")

    @property
    def open_time(self):
        return time(0, 1)

    @property
    def close_time(self):
        return time(23,59)