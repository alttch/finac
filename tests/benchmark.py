#!/usr/bin/env python3

from pathlib import Path
import sys
import os
from tqdm import tqdm

sys.path.insert(0, Path(__file__).absolute().parent.parent.as_posix())
import finac

import unittest
import logging
import rapidtables
import random
import time

from types import SimpleNamespace
from concurrent.futures import ProcessPoolExecutor

TEST_DB = '/tmp/finac-test.db'

dir_me = Path(__file__).absolute().parent.as_posix()

if __name__ == '__main__':
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument('-a',
                    '--account-number',
                    help='accounts to create',
                    type=int,
                    default=100)
    ap.add_argument('-n',
                    '--transaction-number',
                    help='transactions per account',
                    type=int,
                    default=100)
    ap.add_argument('--no-keeper',
                    help='disable built-in integrity keeper',
                    action='store_true')
    ap.add_argument('--benchmark-only',
                    help='benchmark on pre-generated database',
                    action='store_true')
    ap.add_argument('--dbconn',
                    help='DB connection string (WARNING! ALL DATA WILL BE LOST',
                    metavar='DBCONN',
                    default=TEST_DB)
    ap.add_argument('-w', '--workers', help='Max finac workers', type=int)
    a = ap.parse_args()
    pool = ProcessPoolExecutor(max_workers=a.workers)
    if not a.benchmark_only:
        if a.dbconn == TEST_DB:
            try:
                os.unlink(TEST_DB)
            except:
                pass
    finac.init(db=a.dbconn, keep_integrity=not a.no_keeper, multiplier=100)
    finac.core.rate_cache = None
    futures = []

    def wait_futures():
        for f in futures:
            f.result()
        futures.clear()

    if not a.benchmark_only:
        print('Cleaning up...')
        # cleanup
        for tbl in ['account', 'transact', 'asset_rate']:
            finac.core.get_db().execute('delete from {}'.format(tbl))
        finac.core.get_db().execute(
            """delete from asset where code != 'EUR' and code != 'USD'""")
        print('Creating accounts...')
        # create accounts
        for x in tqdm(range(1, a.account_number + 1), leave=True):
            futures.append(
                pool.submit(finac.account_create, f'account-{x}', 'USD'))
        wait_futures()
        # generate transactions
        print('Generating transactions...')
        for i in tqdm(range(a.transaction_number), leave=True):
            for x in range(1, a.account_number + 1):
                dt_id = x
                while dt_id == x:
                    dt_id = random.randint(1, a.account_number)
                futures.append(
                    pool.submit(finac.mv,
                                dt=f'account-{dt_id}',
                                ct=f'account-{x}',
                                amount=random.randint(1000, 10000) / 1000.0,
                                tag=f'trans {x}'))
            wait_futures()
    print('Testing...')
    t = time.time()
    for x in tqdm(range(1, a.account_number + 1), leave=True):
        dt_id = x
        while dt_id == x:
            dt_id = random.randint(1, a.account_number)
        futures.append(
            pool.submit(finac.mv,
                        dt=f'account-{dt_id}',
                        ct=f'account-{x}',
                        amount=random.randint(1000, 10000) / 1000.0,
                        tag=f'trans {x}'))
    wait_futures()
    print('Average transaction time: {:.3f}ms'.format(
        (time.time() - t) / a.account_number * 1000))
    t = time.time()
    for x in tqdm(range(1, a.account_number + 1), leave=True):
        futures.append(pool.submit(finac.account_statement_summary,
                       f'account-{x}',
                       start='2019-01-01'))
    wait_futures()
    print('Average statement time: {:.3f}ms'.format(
        (time.time() - t) / a.account_number * 1000))
    if not a.benchmark_only:
        if a.dbconn == TEST_DB:
            os.unlink(TEST_DB)
    sys.exit()
