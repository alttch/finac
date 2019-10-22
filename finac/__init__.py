import rapidtables, neotermcolor

from finac.core import init, config

# exceptions
from finac.core import ResourceNotFound, RateNotFound
from finac.core import OverdraftError, OverlimitError

# currency methods
from finac.core import currency_create, currency_delete
from finac.core import currency_set_rate, currency_rate
from finac.core import currency_delete_rate

# account methods
from finac.core import account_create, account_delete
from finac.core import account_info

# transaction methods
from finac.core import transaction_create, transaction_complete
from finac.core import transaction_move, transaction_delete

# balance methods
from finac.core import account_credit, account_debit, account_balance

# statements
from finac.core import account_statement, account_statement_summary

# purges
from finac.core import purge, transaction_purge

tr = transaction_create
mv = transaction_move
rm = transaction_delete

stmt = account_statement_summary
balance = account_balance


def format_money(amnt):
    # return '{:,.2f}'.format(amnt)
    return '{:,.2f}'.format(amnt).replace(',', ' ')


def ls(account, start=None, end=None, pending=False):
    result = account_statement_summary(account=account,
                                       start=start,
                                       end=end,
                                       pending=pending)
    stmt = result['statement'].copy()
    acc_info = account_info(account)
    for i, r in enumerate(stmt):
        r = r.copy()
        del r['is_completed']
        r['amount'] = format_money(r['amount'])
        stmt[i] = r
    ft = rapidtables.format_table(stmt, fmt=rapidtables.FORMAT_GENERATOR)
    if not ft:
        return
    h, tbl = ft
    neotermcolor.cprint(h, 'blue')
    neotermcolor.cprint('-' * len(h), 'grey')
    for t, s in zip(tbl, result['statement']):
        neotermcolor.cprint(t, 'red' if s['amount'] < 0 else 'green', attrs='')
    neotermcolor.cprint('-' * len(h), 'grey')
    print('Debit turnover: ', end='')
    neotermcolor.cprint(format_money(result['debit']),
                        color='green',
                        attrs='bold',
                        end=', ')
    print('credit turnover: ', end='')
    neotermcolor.cprint(format_money(result['credit']),
                        color='red',
                        attrs='bold')
    print()
    print('Net profit/loss: ', end='')
    neotermcolor.cprint('{} {}'.format(
        format_money(result['debit'] - result['credit']), acc_info['currency']),
                        attrs='bold',
                        end=' ')
