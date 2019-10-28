__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2019 Altertech'
__license__ = 'MIT'

__version__ = '0.1.0'

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

from finac.core import transaction_update, transaction_apply

# balance methods
from finac.core import account_credit, account_debit, account_balance
from finac.core import account_balance_range

# statements
from finac.core import account_statement, account_statement_summary
from finac.core import account_list, account_list_summary

# purges
from finac.core import purge, transaction_purge

# plots
from finac.plot import account_plot as plot
from finac.plot import account_pie as pie

tr = transaction_create
mv = transaction_move
rm = transaction_delete
apply = transaction_apply
complete = transaction_complete

stmt = account_statement_summary
balance = account_balance

balance_range = partial(account_balance_range, return_timestamp=False)


def format_money(amnt, precision):
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
neotermcolor.set_style('finac:debit_sum', color='green', attrs='bold')
neotermcolor.set_style('finac:credit_sum', color='red', attrs='bold')


def ls(account=None,
       asset=None,
       tp=None,
       start=None,
       end=None,
       tag=None,
       pending=False,
       hide_empty=False,
       order_by=['tp', 'asset', 'account', 'balance'],
       base=None):
    """
    Primary interactive function. Prints account statement if account code
    is specified, otherwise prints summary for all accounts

    Account code may contain '%' symbol as a wildcard.

    Args:
        account: account code
        asset: filter by asset code
        tp: filter by account type (or types)
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
            start=start if start else datetime.datetime.today().replace(day=1),
            end=end,
            tag=tag,
            pending=pending)
        stmt = result['statement'].copy()
        acc_info = account_info(account)
        precision = asset_precision(acc_info['asset'])
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
        if not ft:
            return
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
        print('Net profit/loss: ', end='')
        neotermcolor.cprint('{} {}'.format(
            format_money(result['debit'] - result['credit'], precision),
            acc_info['asset']),
                            attrs='bold')
        print()
    else:
        if not base:
            base = config.base_asset
        base = base.upper()
        result = account_list_summary(asset=asset,
                                      tp=tp,
                                      code=code,
                                      date=end,
                                      order_by=order_by,
                                      hide_empty=hide_empty,
                                      base=base)
        accounts = result['accounts']
        data = accounts.copy()
        bcp = asset_precision(base)
        for i, r in enumerate(accounts):
            r = r.copy()
            r['balance'] = format_money(r['balance'],
                                        asset_precision(r['asset']))
            r['balance ' + base] = format_money(r['balance_bc'], bcp)
            del r['balance_bc']
            del r['note']
            accounts[i] = r
        ft = rapidtables.format_table(
            accounts,
            fmt=rapidtables.FORMAT_GENERATOR,
            align=(rapidtables.ALIGN_LEFT, rapidtables.ALIGN_LEFT,
                   rapidtables.ALIGN_CENTER, rapidtables.ALIGN_RIGHT,
                   rapidtables.ALIGN_RIGHT))
        if not ft:
            return
        h, tbl = ft
        neotermcolor.cprint(h, '@finac:title')
        neotermcolor.cprint('-' * len(h), '@finac:separator')
        for t, s in zip(tbl, data):
            neotermcolor.cprint(t,
                                '@finac:credit' if s['balance'] < 0 else None,
                                attrs='')
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
                asset,
                start=start if start else datetime.datetime.today().replace(
                    day=1),
                end=end):
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
