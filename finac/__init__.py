import rapidtables, neotermcolor

from functools import partial

from finac.core import init, config

# exceptions
from finac.core import ResourceNotFound, RateNotFound
from finac.core import OverdraftError, OverlimitError
from finac.core import ResourceAlreadyExists

# currency methods
from finac.core import currency_create, currency_delete
from finac.core import currency_set_rate, currency_rate
from finac.core import currency_delete_rate

from finac.core import currency_update
from finac.core import currency_precision

# account methods
from finac.core import account_create, account_delete
from finac.core import account_info

from finac.core import account_update

# transaction methods
from finac.core import transaction_create, transaction_complete
from finac.core import transaction_move, transaction_delete

from finac.core import transaction_update

# balance methods
from finac.core import account_credit, account_debit, account_balance
from finac.core import account_balance_range

# statements
from finac.core import account_statement, account_statement_summary
from finac.core import account_list, account_list_summary

# purges
from finac.core import purge, transaction_purge

tr = transaction_create
mv = transaction_move
rm = transaction_delete

stmt = account_statement_summary
balance = account_balance

balance_range = partial(account_balance_range, return_timestamp=False)

lsaccs = account_list_summary


def format_money(amnt, precision):
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
       currency=None,
       code=None,
       tp=None,
       start=None,
       end=None,
       tag=None,
       pending=False,
       hide_empty=False,
       order_by=['tp', 'currency', 'account', 'balance'],
       base=None):
    if account and account.find('%') != -1:
        code = account
        account = None
    if account:
        result = account_statement_summary(account=account,
                                           start=start,
                                           end=end,
                                           tag=tag,
                                           pending=pending)
        stmt = result['statement'].copy()
        acc_info = account_info(account)
        precision = currency_precision(acc_info['currency'])
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
                            style='@finac:debit_sum',
                            end=', ')
        print('credit turnover: ', end='')
        neotermcolor.cprint(format_money(result['credit'], precision),
                            style='finac:credit_sum')
        print()
        print('Net profit/loss: ', end='')
        neotermcolor.cprint('{} {}'.format(
            format_money(result['debit'] - result['credit'], precision),
            acc_info['currency']),
                            attrs='bold')
        print()
    else:
        if not base:
            base = config.base_currency
        base = base.upper()
        result = account_list_summary(currency=currency,
                                      tp=tp,
                                      code=code,
                                      date=end,
                                      order_by=order_by,
                                      hide_empty=hide_empty,
                                      base=base)
        accounts = result['accounts']
        data = accounts.copy()
        bcp = currency_precision(base)
        for i, r in enumerate(accounts):
            r = r.copy()
            r['balance'] = format_money(r['balance'],
                                        currency_precision(r['currency']))
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
                            style='@finac:sum')
        print()
