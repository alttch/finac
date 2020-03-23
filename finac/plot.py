__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2019 Altertech'
__license__ = 'MIT'

__version__ = '0.4.17'

from . import core


def account_plot(account=None,
                 tp=None,
                 start=None,
                 end=None,
                 step=1,
                 base=None,
                 **kwargs):
    """
    Plot account balance chart for the specified time range

    Either account code or account types must be specified

    Args:
        account: account code
        tp: account type (or types)
        start: start date/time, default: first day of current month
        end: end date/time, if not specified, current time is used
        step: chart step in days
        base: base currency
        **kwargs: passed as-is to matplotlib.pyplot.plot
    """
    from matplotlib import pyplot as plt
    import datetime
    plt.plot(
        *core.account_balance_range(
            account=account,
            tp=tp,
            start=start if start else datetime.datetime.today().replace(
                day=1, hour=0, minute=0, second=0, microsecond=0).timestamp(),
            end=end,
            step=step,
            return_timestamp=False,
            base=base), **kwargs)


def account_pie(tp=None,
                asset=None,
                mb=0,
                base=None,
                passive=None,
                group_by=None,
                shadow=True,
                autopct='%1.1f%%',
                **kwargs):
    """
    Plot pie chart of the account balances

    Args:
        tp: account types to include
        mb: min balace (or section goes to "other")
        base: base asset to recalc amounts (default: usd)
        shadow:
        autopct:
        **kwargs: passed as-is to matplotlib.pyplot.pie
    """
    from matplotlib import pyplot as plt
    if base is None:
        base = core.config.base_asset
    x = core.account_list_summary(base=base,
                                  hide_empty=True,
                                  tp=tp,
                                  passive=passive,
                                  asset=asset,
                                  group_by=group_by)
    if group_by == 'asset':
        kf = 'assets'
        kfp = 'asset'
    elif group_by in ['type', 'tp']:
        kf = 'account_types'
        kfp = 'type'
    else:
        kf = 'accounts'
        kfp = 'account'
    sizes = [z['balance_bc'] for z in x[kf] if z['balance_bc'] >= mb]
    othersum = sum(z['balance_bc'] for z in x[kf] if z['balance_bc'] < mb)
    labels = [z[kfp] for z in x[kf] if z['balance_bc'] >= mb]
    if othersum:
        labels.append('other')
        sizes.append(othersum)
    plt.pie(sizes, labels=labels, shadow=shadow, autopct=autopct, **kwargs)
