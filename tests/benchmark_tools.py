import finac
import random


def generate_transactions(account_id, n, total_accs):
    ct = f'account-{account_id}'
    tag = f'trans {account_id}'
    for i in range(n):
        dt_id = account_id + n
        if dt_id > total_accs:
            dt_id = total_accs - dt_id
            if dt_id < 1:
                dt_id = random.randint(1, total_accs)
        while dt_id == account_id:
            dt_id = random.randint(1, total_accs)
        finac.mv(dt=f'account-{dt_id}', ct=ct, amount=n / 100, tag=tag)
