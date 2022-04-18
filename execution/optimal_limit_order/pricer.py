import math

import click
import matplotlib.pyplot as plt
import numpy as np
from scipy.integrate import odeint

from common.data.constants import FUTURES
from common.execution.optimal_limit_order.estimators import get_tick_size


def optimal_limit_order_formula(q_max, t_max, mu, sigma, A, k, gamma, b, is_plot=False):
    """
    q_max : Quantity in ATS to execute
    t_max : Time in seconds remaining to execute
    mu : Trend in tick per second
    sigma : Volatility in tick per second squared
    A : arrival rate at best quote
    k : exponential decreasing parameter of arrival rate
    gamma : absolute risk aversion
    b : cost per share to liquidate the remaining position in ticks
    """

    alpha = k / 2 * gamma * np.power(sigma, 2)
    beta = k * mu
    eta = A * np.power(1 + gamma / k, -(1 + k / gamma))
    w_0 = 1

    def w_T(q):
        return np.exp(-k * q * b)

    def linear_ode(w_q, w_q_1, q):
        return (alpha * np.power(q, 2) - beta * q) * w_q - eta * w_q_1

    def linear_ode_system(y, t):
        w = [w_0, *y]
        dydt = [linear_ode(w[q], w[q-1], q) for q in range(1, q_max + 1)]
        return dydt

    w_T = [w_T(q) for q in range(1, q_max + 1)]
    t = np.linspace(0, -t_max, 100)

    w = odeint(linear_ode_system, w_T, t, args=())

    delta = {}
    for q in range(1, q_max + 1):
        if q == 1:
            delta[q] = 1 / k * np.log(w[:, q-1] / w_0) + \
                1 / gamma * np.log(1 + gamma / k)
        else:
            delta[q] = 1 / k * np.log(w[:, q-1] / w[:, q-2]) + \
                1 / gamma * np.log(1 + gamma / k)

    if is_plot:
        for q in range(1, q_max + 1):
            plt.plot(t, delta[q], 'b', label=f'delta_{q}(t)')
        plt.legend(loc='best')
        plt.xlabel('t')
        plt.grid()
        plt.show()

    return delta[q_max][-1]


def get_optimal_quote(stem, quantity, time_in_seconds):
    parameters = FUTURES[stem]['ExecutionParameters']
    average_trading_size = parameters['ats']
    tick_size = FUTURES[stem]['TickSize']
    gamma = 5e-4 / tick_size
    quote = optimal_limit_order_formula(
        q_max=math.ceil(quantity / average_trading_size),
        t_max=time_in_seconds,
        mu=0,
        sigma=parameters['sigma'],
        A=parameters['A'],
        k=parameters['k'],
        gamma=gamma,
        b=parameters['b'])
    return quote * tick_size


@click.command()
@click.option('--stem', default=None)
@click.option('--quantity', default=1)
@click.option('--seconds', default=300)
def main(stem, quantity, seconds):
    quote = get_optimal_quote(
        stem=stem, quantity=quantity, time_in_seconds=seconds)
    currency = FUTURES[stem]['Currency']
    buy_sign = '-' if np.sign(-1 * quote) < 0 else '+'
    sell_sign = '-' if np.sign(quote) < 0 else '+'
    print(f'buy@mid {buy_sign} {np.abs(quote)} {currency}')
    print(f'sell@mid {sell_sign} {np.abs(quote)} {currency}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
