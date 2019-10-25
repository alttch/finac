from . import core


def account_plot(account, start, end=None, step=1, **kwargs):
    from matplotlib import pyplot as plt
    plt.plot(
        *core.account_balance_range(
            account, start, end=end, step=step, return_timestamp=False),
        **kwargs)


def account_pie(tp=None,
                mb=0,
                base='usd',
                shadow=True,
                autopct='%1.1f%%',
                **kwargs):
    """
    Args:
        tp: account types to include
        mb: min balace (or account go to "other")
        base: base currency to recalc amounts (default: usd)
        shadow, autopct, **kwargs: options for plt.pie
    """
    from matplotlib import pyplot as plt
    x = core.account_list_summary(base=base, hide_empty=True, tp=tp)
    sizes = [z['balance_bc'] for z in x['accounts'] if z['balance_bc'] >= mb]
    othersum = sum(
        z['balance_bc'] for z in x['accounts'] if z['balance_bc'] < mb)
    labels = [z['account'] for z in x['accounts'] if z['balance_bc'] >= mb]
    if othersum:
        labels.append('other')
        sizes.append(othersum)
    plt.pie(sizes, labels=labels, shadow=shadow, autopct=autopct, **kwargs)
