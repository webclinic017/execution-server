from datetime import date, datetime, time, timedelta, timezone
from tqdm import tqdm

from common.data.constants import CRYPTOCURRENCIES
from common.data.bitfinex import get_public_trades
from ....eikon import get_timeseries


def download(ric, start_date, end_date):
    print(ric)
    delta = end_date - start_date
    for i in tqdm(range(delta.days + 1)):
        day = start_date + timedelta(days=i)
        for hour in range(24):
            start_datetime = datetime.combine(
                day, time(hour, 0, 0)).isoformat()
            end_datetime = datetime.combine(
                day, time(hour, 59, 59, 999)).isoformat()
            if ric in CRYPTOCURRENCIES:
                get_timeseries(rics=ric,
                               fields=['BID', 'BIDSIZE',
                                       'ASK', 'ASKSIZE'],
                               start_date=start_datetime,
                               end_date=end_datetime,
                               interval='taq')
                for minutes in [0, 15, 30, 45]:
                    start_datetime_min = datetime.combine(
                        day, time(hour, minutes, 0, tzinfo=timezone.utc))
                    end_datetime_min = datetime.combine(
                        day, time(hour, minutes + 14, 59, 999, tzinfo=timezone.utc))
                    start = int(1000 * start_datetime_min.timestamp())
                    end = int(1000 * end_datetime_min.timestamp())
                    ticker = ric.replace('=', '')
                    symbol = f't{ticker}USD'
                    get_public_trades(
                        symbol=symbol, start=start, end=end, limit='10000', sort=1)

            else:
                get_timeseries(rics=ric,
                               fields=['TRDPRC_1', 'COUNT'],
                               start_date=start_datetime,
                               end_date=end_datetime,
                               interval='tas')
                get_timeseries(rics=ric,
                               fields=['BID', 'BIDSIZE',
                                       'ASK', 'ASKSIZE'],
                               start_date=start_datetime,
                               end_date=end_datetime,
                               interval='taq')


if __name__ == '__main__':
    start_date = date(2020, 5, 1)
    end_date = date(2020, 5, 29)
    rics = [
        # 'AXAF.PA',
        # 'SPY', 'EWJ', 'VNQ', 'IEF.O', 'DBC', 'VGK', 'VWO', 'VNQI.O', 'TLT.O', 'GLD',
        *CRYPTOCURRENCIES
    ]
    for ric in rics:
        download(ric, start_date, end_date)
