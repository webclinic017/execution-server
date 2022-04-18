import click
from datetime import date, datetime, time, timedelta, timezone
import numpy as np
import pandas as pd
from pprint import pprint
from tqdm import tqdm

from common.data.constants import CRYPTOCURRENCIES, FUTURES
from common.data.database import json_data_to_df
from common.data.bitfinex import convert_bitfinex_trades, get_public_trades
from ....eikon import get_timeseries

MINIMUM_EVENT_NUMBER = 30


def get_quotes(ric, day):
    quotes_frames = []
    for hour in range(24):
        start_datetime = datetime.combine(
            day, time(hour, 0, 0)).isoformat()
        end_datetime = datetime.combine(
            day, time(hour, 59, 59, 999)).isoformat()
        r = get_timeseries(rics=ric,
                           fields=['BID', 'BIDSIZE',
                                   'ASK', 'ASKSIZE'],
                           start_date=start_datetime,
                           end_date=end_datetime,
                           interval='taq')
        if r['error'] is None:
            quotes_frame = json_data_to_df(r['data'])
            quotes_frames.append(quotes_frame)
    if len(quotes_frames) == 0:
        return None
    quotes = pd.concat(quotes_frames)
    quotes.sort_index(inplace=True)
    return quotes


def get_trades(ric, day):
    trades_frames = []
    for hour in range(24):
        start_datetime = datetime.combine(
            day, time(hour, 0, 0)).isoformat()
        end_datetime = datetime.combine(
            day, time(hour, 59, 59, 999)).isoformat()
        if ric in CRYPTOCURRENCIES:
            for minutes in [0, 15, 30, 45]:
                start_datetime_min = datetime.combine(
                    day, time(hour, minutes, 0, tzinfo=timezone.utc))
                end_datetime_min = datetime.combine(
                    day, time(hour, minutes + 14, 59, 999, tzinfo=timezone.utc))
                start = int(1000 * start_datetime_min.timestamp())
                end = int(1000 * end_datetime_min.timestamp())
                ticker = ric.replace('=', '')
                symbol = f't{ticker}USD'
                trades_list = get_public_trades(
                    symbol=symbol, start=start, end=end, limit='10000', sort=1)
                trades_frame = convert_bitfinex_trades(trades_list)
                if trades_frame is not None:
                    trades_frame['COUNT'] = trades_frame['COUNT'].abs()
                    trades_frames.append(trades_frame)
        else:
            r = get_timeseries(rics=ric,
                               fields=['TRDPRC_1', 'COUNT'],
                               start_date=start_datetime,
                               end_date=end_datetime,
                               interval='tas')
            if r['error'] is None:
                trades_frame = json_data_to_df(r['data'])
                trades_frames.append(trades_frame)
    if len(trades_frames) == 0:
        return None
    trades = pd.concat(trades_frames)
    trades.sort_index(inplace=True)
    return trades


def filter_opening_hours(ric, data):
    if ric.endswith('.PA'):
        opening_time = time(9, 0, 0)
        closing_time = time(17, 30, 0)
        data = data.tz_localize('UTC').tz_convert('Europe/Paris')
        data = data.between_time(opening_time, closing_time)
    elif ric in CRYPTOCURRENCIES:
        pass
    else:
        opening_time = time(9, 30, 0)
        closing_time = time(16, 0, 0)
        data = data.tz_localize('UTC').tz_convert('America/New_York')
        data = data.between_time(opening_time, closing_time)
    return data


def get_average_trading_size(trades):
    return np.nanmean(trades['COUNT'].tolist())


def get_volatility(quotes, tick_size):
    aggr_quotes = quotes[['BID', 'ASK']].groupby(level=0).median()
    mid = (aggr_quotes['BID'] + aggr_quotes['ASK']) / 2
    mid_1s = mid.resample('1S').pad()
    return np.nanstd(np.diff(mid_1s.tolist())) / tick_size


def get_cost_per_share(quotes, tick_size, market_impact_factor=3):
    spread = quotes['ASK'] - quotes['BID']
    return np.nanmean(spread.tolist()) / tick_size * market_impact_factor


def get_arrival_rate(quotes, trades, tick_size, average_trading_size, b):
    agg_quotes = quotes[['BID', 'ASK']].groupby(level=0).median()
    agg_trades_price = trades['TRDPRC_1'].groupby(level=0).median()
    agg_trades_quantity = trades['COUNT'].groupby(level=0).sum()
    trades_and_quotes = pd.concat([agg_quotes, agg_trades_price,
                                   agg_trades_quantity], axis=1, sort=False)
    x = []
    y = []
    for offset in np.linspace(-b, 10, 5):
        prev_timestamp = None
        limit_order = None
        executed_quantity = 0
        last_ask = None
        executed_quantities = []
        for index, row in trades_and_quotes.iterrows():
            timestamp = index.round('min')
            if not np.isnan(row['ASK']):
                last_ask = row['ASK']
            if prev_timestamp is None:
                prev_timestamp = timestamp
                continue
            if timestamp != prev_timestamp:
                executed_quantities.append(executed_quantity)
                executed_quantity = 0
                prev_timestamp = timestamp
                if last_ask is not None:
                    limit_order = last_ask + offset * tick_size
            if executed_quantity == 0 and limit_order is not None and not np.isnan(row['TRDPRC_1']) and row['TRDPRC_1'] > limit_order:
                executed_quantity = row['COUNT']
        x.append(offset)
        y.append(np.mean(executed_quantities))
    index = np.array(y) > 0
    x = np.array(x)[index]
    y = np.array(y)[index]
    if len(x) < 2:
        return np.NaN, np.NaN
    coefficients = tuple(np.polyfit(x, np.log(y), 1))
    k = -coefficients[0]
    sixty_seconds = 60
    A = np.exp(coefficients[1]) / sixty_seconds / average_trading_size
    return A, k


def get_estimators(ric, day, tick_size):
    quotes = get_quotes(ric, day)
    trades = get_trades(ric, day)
    if quotes is None or trades is None:
        return
    quotes = filter_opening_hours(ric, quotes)
    trades = filter_opening_hours(ric, trades)
    if quotes.shape[0] < MINIMUM_EVENT_NUMBER or trades.shape[0] < MINIMUM_EVENT_NUMBER:
        return
    average_trading_size = get_average_trading_size(trades)
    b = get_cost_per_share(quotes, tick_size)
    A, k = get_arrival_rate(quotes, trades,
                            tick_size, average_trading_size, b)
    estimators = {
        'ats': average_trading_size,
        'sigma': get_volatility(quotes, tick_size),
        'b': b,
        'A': A,
        'k': k,
    }
    return estimators


def get_tick_size(ric):
    if ric in ['BTC=']:
        return 1
    elif ric in ['ETH=']:
        return 0.1
    elif ric in ['LTC=']:
        return 0.01
    elif ric in ['XRP=']:
        return 0.0001
    else:
        return 0.01


@click.command()
@click.option('--stems', default=','.join(list(FUTURES.keys())))
def main(stems):
    start_date = date.today() - timedelta(days=30)
    end_date = date.today() - timedelta(days=1)
    stems = stems.split(',')
    delta = end_date - start_date
    for stem in stems:
        print(stem)
        data = []
        for i in tqdm(range(delta.days + 1)):
            day = start_date + timedelta(days=i)
            if day.weekday in [5, 6]:
                continue
            ric_suffix = 'c1'
            ric = FUTURES[stem].get('Stem', {}).get('Reuters', '') + ric_suffix
            if ric == ric_suffix:
                continue
            tick_size = FUTURES[stem]['TickSize']
            data.append(get_estimators(ric, day, tick_size))
        data = [d for d in data if d is not None]
        dfm = pd.DataFrame(data=data)
        pprint(dfm.median(axis=0, skipna=True).to_dict())


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
