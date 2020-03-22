__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2019 Altertech'
__license__ = 'MIT'

__version__ = '0.4.16'

import rapidtables
import neotermcolor
import datetime

from functools import partial
from collections import OrderedDict

from finac.core import init, config

# exceptions
from finac.core import ResourceNotFound, RateNotFound
from finac.core import OverdraftError, OverlimitError
from finac.core import ResourceAlreadyExists

# asset methods
from finac.core import asset_create, asset_delete
from finac.core import asset_set_rate, asset_rate
from finac.core import asset_delete_rate

from finac.core import asset_update
from finac.core import asset_precision
from finac.core import asset_list, asset_list_rates

# account methods
from finac.core import account_create, account_delete
from finac.core import account_info

from finac.core import account_update

# transaction methods
from finac.core import transaction_create, transaction_complete
from finac.core import transaction_move, transaction_delete
from finac.core import transaction_copy

from finac.core import transaction_update, transaction_apply

# balance methods
from finac.core import account_credit, account_debit, account_balance
from finac.core import account_balance_range

# statements
from finac.core import account_statement, account_statement_summary
from finac.core import account_list, account_list_summary

# purges
from finac.core import purge, transaction_purge

# caches
from finac.core import preload

# plots
from finac.plot import account_plot as plot
from finac.plot import account_pie as pie

# tools
from finac.core import parse_number, parse_date, get_version

tr = transaction_create
tc = transaction_copy
mv = transaction_move
rm = transaction_delete
cp = transaction_copy
apply = transaction_apply
complete = transaction_complete
rate = asset_rate

stmt = account_statement_summary


def check_version(warn=False):
    core_version = get_version()
    if __version__ != core_version:
        if warn:
            print('WARNING: client version: {}, core version: {}'.format(
                __version__, core_version))
        return False
    else:
        return True


def balance(account=None,
            asset=None,
            tp=None,
            passive=None,
            base=None,
            date=None):
    if account and account.find('%') == -1:
        return account_balance(account, tp=tp, base=base, date=date)
    else:
        return account_list_summary(asset=asset,
                                    tp=tp,
                                    passive=passive,
                                    code=account,
                                    date=date,
                                    base=base)['total']


balance_range = partial(account_balance_range, return_timestamp=False)


def format_money(amnt, precision=2):
    """
    Format output for money values

    Finac doesn't use system locale, in the interactive mode all numbers are
    formatted with this function. Override it to set the number format you wish
    """
    # return '{:,.2f}'.format(amnt)
    return ('{:,.' + str(precision) + 'f}').format(amnt).replace(',', ' ')


neotermcolor.set_style('finac:title', color='blue')
neotermcolor.set_style('finac:separator', color='grey')
neotermcolor.set_style('finac:sum', attrs='bold')
neotermcolor.set_style('finac:debit', color='green')
neotermcolor.set_style('finac:credit', color='red')
neotermcolor.set_style('finac:passive', color='magenta')
neotermcolor.set_style('finac:debit_sum', color='green', attrs='bold')
neotermcolor.set_style('finac:credit_sum', color='red', attrs='bold')


def ls(account=None,
       asset=None,
       tp=None,
       passive=None,
       start=None,
       end=None,
       tag=None,
       pending=True,
       hide_empty=False,
       order_by=['tp', 'asset', 'account', 'balance'],
       group_by=None,
       base=None):
    """
    Primary interactive function. Prints account statement if account code
    is specified, otherwise prints summary for all accounts

    Account code may contain '%' symbol as a wildcard.

    Args:
        account: account code
        asset: filter by asset code
        tp: filter by account type (or types)
        passive: list passive, active or all (if None) accounts
        start: start date (for statement), default: first day of current month
        end: end date (or balance date for summary)
        tag: filter transactions by tag (for statement)
        pending: include pending transactions
        hide_empty: hide empty accounts (for summary)
        order_by: column ordering (ordering by base is not supported)
        base: specify base asset
    """
    if account and account.find('%') != -1:
        code = account
        account = None
    else:
        code = None
    if account:
        result = account_statement_summary(
            account=account,
            start=start if start else datetime.datetime.today().replace(
                day=1, hour=0, minute=0, second=0, microsecond=0).timestamp(),
            end=end,
            tag=tag,
            pending=pending, datefmt=True)
        stmt = result['statement'].copy()
        acc_info = account_info(account=account)
        precision = asset_precision(asset=acc_info['asset'])
        for i, r in enumerate(stmt):
            r = r.copy()
            del r['is_completed']
            r['amount'] = format_money(r['amount'], precision)
            stmt[i] = r
        ft = rapidtables.format_table(
            stmt,
            fmt=rapidtables.FORMAT_GENERATOR,
            align=(rapidtables.ALIGN_LEFT, rapidtables.ALIGN_RIGHT,
                   rapidtables.ALIGN_LEFT, rapidtables.ALIGN_LEFT,
                   rapidtables.ALIGN_LEFT, rapidtables.ALIGN_LEFT,
                   rapidtables.ALIGN_LEFT))
        rcur = base.upper() if base else acc_info['asset']
        if ft:
            h, tbl = ft
            neotermcolor.cprint(h, '@finac:title')
            neotermcolor.cprint('-' * len(h), '@finac:separator')
            for t, s in zip(tbl, result['statement']):
                neotermcolor.cprint(
                    t,
                    '@finac:credit' if s['amount'] < 0 else '@finac:debit',
                    attrs='')
            neotermcolor.cprint('-' * len(h), '@finac:separator')
            print('Debit turnover: ', end='')
            neotermcolor.cprint(format_money(result['debit'], precision),
                                style='finac:debit_sum',
                                end=', ')
            print('credit turnover: ', end='')
            neotermcolor.cprint(format_money(result['credit'], precision),
                                style='finac:credit_sum')
            print()
            if base:
                precision = asset_precision(base)
            print('Net profit/loss: ', end='')
            pl = result['debit'] - result['credit']
            if base:
                pl = pl * asset_rate(acc_info['asset'], base, date=end)
            neotermcolor.cprint('{} {}'.format(format_money(pl, precision),
                                               rcur),
                                attrs='bold',
                                end='')
            print(', balance', end='')
        else:
            print('Balance', end='')
        balance = account_balance(account=account, date=end)
        if base:
            balance = balance * asset_rate(acc_info['asset'], base, date=end)
        print('{}: '.format(' to date' if end else ''), end='')
        neotermcolor.cprint('{} {}'.format(format_money(balance, precision),
                                           rcur),
                            attrs='bold',
                            end='')
        print()
    else:
        if not base:
            base = config.base_asset
        base = base.upper()
        result = account_list_summary(asset=asset,
                                      tp=tp,
                                      passive=passive,
                                      code=code,
                                      date=end,
                                      order_by=order_by,
                                      group_by=group_by,
                                      hide_empty=hide_empty,
                                      base=base)
        if not group_by:
            kf = 'accounts'
            rt_align = (rapidtables.ALIGN_LEFT, rapidtables.ALIGN_LEFT,
                        rapidtables.ALIGN_CENTER, rapidtables.ALIGN_CENTER,
                        rapidtables.ALIGN_RIGHT, rapidtables.ALIGN_RIGHT)
        elif group_by == 'asset':
            kf = 'assets'
            rt_align = (rapidtables.ALIGN_LEFT, rapidtables.ALIGN_RIGHT,
                        rapidtables.ALIGN_RIGHT)
        else:
            kf = 'account_types'
            rt_align = (rapidtables.ALIGN_LEFT, rapidtables.ALIGN_RIGHT)
        res = result[kf]
        data = res.copy()
        bcp = asset_precision(asset=base)
        for i, r in enumerate(res):
            r = r.copy()
            if group_by not in ['type', 'tp']:
                r['balance'] = format_money(r['balance'],
                                            asset_precision(asset=r['asset']))
            r['balance ' + base] = format_money(r['balance_bc'], bcp)
            del r['balance_bc']
            if not group_by:
                del r['note']
            if 'passive' in r:
                r['passive'] = 'P' if r['passive'] else ''
            res[i] = r
        ft = rapidtables.format_table(res,
                                      fmt=rapidtables.FORMAT_GENERATOR,
                                      align=rt_align)
        if not ft:
            return
        h, tbl = ft
        neotermcolor.cprint(h, '@finac:title')
        neotermcolor.cprint('-' * len(h), '@finac:separator')
        for t, s in zip(tbl, data):
            if s.get('passive'):
                style = 'finac:passive'
            else:
                style = 'finac:credit' if s['balance_bc'] < 0 else None
            neotermcolor.cprint(t, style=style, attrs='')
        neotermcolor.cprint('-' * len(h), '@finac:separator')
        neotermcolor.cprint('Total: ', end='')
        neotermcolor.cprint('{} {}'.format(format_money(result['total'], bcp),
                                           base),
                            style='finac:sum')
        print()


def lsa(asset=None, start=None, end=None):
    """
    Print list of assets or asset rates for the specified one

    Currency filter can be specified either as code, or as pair "code/code"

    If asset == '*' - print rates table

    Args:
        asset: asset code
        start: start date (for rates), default: first day of current month
        end: end date (for rates)
    """
    if not asset:
        ft = rapidtables.format_table(asset_list(),
                                      fmt=rapidtables.FORMAT_GENERATOR,
                                      align=(rapidtables.ALIGN_LEFT,
                                             rapidtables.ALIGN_RIGHT))
        if not ft:
            return
        h, tbl = ft
        neotermcolor.cprint(h, '@finac:title')
        neotermcolor.cprint('-' * len(h), '@finac:separator')
        for t in tbl:
            neotermcolor.cprint(t)
        neotermcolor.cprint('-' * len(h), '@finac:separator')
        print('Base asset: ', end='')
        neotermcolor.cprint(config.base_asset.upper(), style='finac:sum')
    else:
        rr = []
        for r in asset_list_rates(
                asset=(asset if asset != '*' else None),
                start=start if start else datetime.datetime.today().replace(
                    day=1, hour=0, minute=0, second=0,
                    microsecond=0).timestamp(),
                end=end, datefmt=True):
            row = OrderedDict()
            row['pair'] = '{}/{}'.format(r['asset_from'], r['asset_to'])
            row['date'] = r['date']
            row['value'] = r['value']
            rr.append(row)
        ft = rapidtables.format_table(rr, fmt=rapidtables.FORMAT_GENERATOR)
        if not ft:
            return
        h, tbl = ft
        neotermcolor.cprint(h, '@finac:title')
        neotermcolor.cprint('-' * len(h), '@finac:separator')
        for t in tbl:
            neotermcolor.cprint(t)
