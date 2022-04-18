import re
from ib_insync import *
from pprint import pprint

from common.data.constants import FUTURES
from common.data.database import ric_to_stem
from common.data.gdrive import get_positions
from dateutil import tz
from dateutil.parser import parse
import pytz
from tqdm import tqdm


util.startLoop()

CURRENCIES = ['EUR']
STRATEGIES = [
    'Buy And Hold',
    # 'Harvester',
    'Momentum',
    #'Trend Following'
]
PORT_LIVE = 7496


def get_month_letter(month):
    if month == 'JAN':
        return 'F'
    if month == 'FEB':
        return 'G'
    if month == 'MAR':
        return 'H'
    if month == 'APR':
        return 'J'
    if month == 'MAY':
        return 'K'
    if month == 'JUN':
        return 'M'
    if month == 'JUL':
        return 'N'
    if month == 'AUG':
        return 'Q'
    if month == 'SEP':
        return 'U'
    if month == 'OCT':
        return 'V'
    if month == 'NOV':
        return 'X'
    if month == 'DEC':
        return 'Z'
    raise Exception('Month does not exist')


def convert_local_symbol(local_symbol):
    if ' ' in local_symbol:
        tokens = local_symbol.split(' ')
        return tokens[0] + get_month_letter(tokens[-2]) + tokens[-1][1] + '^2'
    elif ' ' not in local_symbol and '.' not in local_symbol:
        return local_symbol + '^2'
    return local_symbol


def get_prefix(symbol):
    if symbol in ['BO', 'C', 'HG', 'O', 'RR', 'SI', 'W']:
        return '1'
    return ''


def get_positions_from_ib():
    ib = IB()
    ib.connect('127.0.0.1', PORT_LIVE, clientId=1)
    positions = {}
    for position in ib.positions():
        symbol = position.contract.symbol
        if symbol in CURRENCIES:
            continue
        local_symbol = convert_local_symbol(position.contract.localSymbol)
        prefix = get_prefix(symbol)
        ric = prefix + local_symbol
        ric = re.sub('^GOIL', 'LGO', ric)
        ric = re.sub('^ETH', 'HTE', ric)
        positions[ric] = positions.get(ric, 0) + position.position
    return positions


def ric_to_ib_ticker(ric):
    stem = ric_to_stem(ric)
    ib_stem = FUTURES[stem]['Stem']['InteractiveBrokers']
    if stem == 'BO':
        return re.sub('^1BO', ib_stem, ric)
    if stem == 'O':
        return re.sub('^1O', ib_stem, ric)
    if stem == 'RR':
        ticker = re.sub('^1RR', ib_stem, ric)
        return re.sub('^RR', ib_stem, ticker)
    if stem == 'S':
        return re.sub('^S', ib_stem, ric)
    if stem == 'SI':
        return re.sub('^1SIRT', "1" + ib_stem, ric)
    if stem == 'W':
        return re.sub('^W', '1W', ric)
    return ric


def get_positions_from_airflow():
    positions = {}
    for strategy in tqdm(STRATEGIES):
        positions_of_strategy = get_positions(strategy) \
            .set_index('Date').tail(1).to_dict(orient='records')[0]
        for key, value in positions_of_strategy.items():
            value = round(value)
            if value == 0:
                continue
            rics = key.split('-')
            if len(rics) == 2:
                rics[1] = rics[0][:-2] + rics[1]
            values = [-value, value] if len(rics) == 2 else [value]
            rics = [r if '^' in r else r + '^2' for r in rics]
            for index, ric in enumerate(rics):
                ticker = ric_to_ib_ticker(ric)
                positions[ticker] = positions.get(ticker, 0) + values[index]
    return positions


def convert_time(stem):
    tzinfos = {
        'CT': tz.gettz('US/Central'),
        'ET': tz.gettz('US/Eastern')
    }
    local_tz = pytz.timezone('Europe/Paris')
    hours = FUTURES[stem]['Hours']
    from_time, to_time = hours.split(' - ')
    from_time += ' ' + hours.split(' ')[-1]
    from_time = parse(from_time, tzinfos=tzinfos).astimezone(local_tz).time()
    to_time = parse(to_time, tzinfos=tzinfos).astimezone(local_tz).time()
    return f'{from_time} - {to_time}'


def main():
    positions_ib = get_positions_from_ib()
    positions_airflow = get_positions_from_airflow()
    keys = sorted(list(set(list(positions_ib.keys()) +
                           list(positions_airflow.keys()))))
    for key in keys:
        position_ib = positions_ib.get(key, 0)
        position_airflow = positions_airflow.get(key, 0)
        if position_ib == position_airflow:
            continue
        ric = re.sub('^ZL', 'BO', key)
        ric = re.sub('^ZO', 'O', ric)
        ric = re.sub('^ZS', 'S', ric)
        stem = ric_to_stem(ric)
        open_time = convert_time(stem)
        print(f'({open_time}) {stem}: {key}: {position_ib} -> {position_airflow}')


if __name__ == '__main__':
    main()
