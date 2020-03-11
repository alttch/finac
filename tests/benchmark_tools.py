import finac
import random


def generate_transactions(account_id, n, total_accs):
    ct = f'account-{account_id}'
    tag = f'trans {account_id}'
    for i in range(n):
        dt_id = account_id
        while dt_id == account_id:
            dt_id = random.randint(1, total_accs)
        finac.mv(dt=f'account-{dt_id}',
                 ct=ct,
                 amount=random.randint(1000, 10000) / 1000,
                 tag=tag)
