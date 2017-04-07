from zipline.data.bundles import register
from zipline.data.bundles.poloniex import viacsv

eqSym = {
    'USDT_ETC',
    'USDT_ETH',
    'USDT_BTC',
    'USDT_XMR',
    'USDT_XRP',
    'USDT_ZEC',
    'USDT_LTC',
    'USDT_REP',
    'USDT_NXT',
    'USDT_STR',
    'USDT_DASH',
}

register(
    'poloniex',    # name this whatever you like
    viacsv(eqSym),
    calendar_name='POLONIEX',
)
