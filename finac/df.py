__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2019 Altertech'
__license__ = 'MIT'

__version__ = '0.4.17'

from . import core


def df(fn, *args, **kwargs):
    """
    Get Finac DB data as Pandas DataFrame

    Converts Finac data to Pandas DataFrame. Requires pandas Python module.

    rate - asset_rate
    asset - asset_list
    account - account_list
    statement - account_statement
    balance - account_balance_range

    Args:
        fn: rate, asset, account, statement or balance
        other arguments: passed to called function as-is
    Returns:
        formatted Pandas dataframe
    Raises:
        ValueError: if invalid function has been specified
    """
    import pandas as pd
    if fn == 'rate':
        result = pd.DataFrame(core.asset_list_rates(*args, **kwargs))
        if not result.empty:
            result['date'] = pd.to_datetime(result['date'])
            return result
        else:
            return result
    elif fn == 'asset':
        return pd.DataFrame(core.asset_list(*args, **kwargs))
    elif fn == 'account':
        return pd.DataFrame(core.account_list(
            *args, **kwargs)).set_index('account').reset_index()
    elif fn == 'statement':
        result = pd.DataFrame(core.account_statement(*args, **kwargs))
        result['created'] = pd.to_datetime(result['created'])
        return result
    elif fn == 'balance':
        r = core.account_balance_range(*args, return_timestamp=False, **kwargs)
        result = pd.DataFrame(
            [dict(date=r[0][i], balance=r[1][i]) for i in range(len(r[0]))])
        result['date'] = pd.to_datetime(result['date'])
        return result
    else:
        raise ValueError('Invalid function')
