#!/usr/bin/env python3

from pathlib import Path
import sys
import os

sys.path.insert(0, Path().absolute().parent.as_posix())
import finac

import unittest
import logging
import rapidtables
import random

from types import SimpleNamespace

TEST_DB = '/tmp/finac-test.db'

result = SimpleNamespace()


class Test(unittest.TestCase):

    def run(self, result=None):
        if not result.errors:
            super(Test, self).run(result)

    def test002_create_account(self):
        finac.account_create('TEST.TEST', 'eur', 'Test account', 'current')
        finac.account_create('TEST2.TEST', 'eur', 'Test account 2', 'current')

    def test003_create_transaction(self):
        result.transaction1_id = finac.transaction_create('test.test',
                                                          100,
                                                          'test',
                                                          'Test balance import',
                                                          mark_completed=False)
        self.assertEqual(finac.account_balance('TEST.TEST'), 0)
        statement = list(
            finac.account_statement('TEST.TEST', '2019-01-01', pending=False))
        self.assertEqual(len(statement), 0)
        statement = list(
            finac.account_statement('TEST.TEST', '2019-01-01', pending=True))
        self.assertEqual(len(statement), 1)

    def test004_transaction_complete(self):
        finac.transaction_complete(result.transaction1_id)
        self.assertEqual(finac.account_balance('TEST.TEST'), 100)

    def test005_transaction_move(self):
        result.transaction2_id = finac.transaction_move('TEST2.TEST',
                                                        'TEST.TEST', 25, 'test',
                                                        'Test move')
        self.assertEqual(finac.account_balance('TEST.TEST'), 75)
        self.assertEqual(finac.account_balance('TEST2.TEST'), 25)

    def test006_statement_tests(self):
        statement = list(finac.account_statement('test.test', '2019-01-01'))
        self.assertEqual(len(statement), 2)
        statement = list(finac.account_statement('TEST2.TEST'))
        self.assertEqual(len(statement), 1)
        statement = list(finac.account_statement('TEST.TEST', '2119-01-01'))
        self.assertEqual(len(statement), 0)
        statement = list(
            finac.account_statement('TEST.TEST', '2019-01-01', '2119-05-22'))
        self.assertEqual(len(statement), 2)
        statement = list(finac.account_statement('TEST.TEST', end='2119-05-22'))
        self.assertEqual(len(statement), 2)
        ss = finac.account_statement_summary('TEST.TEST', end='2119-05-22')
        self.assertEqual(ss['credit'], 25)
        self.assertEqual(ss['debit'], 100)
        self.assertEqual(ss['net'], 75)

    def test020_transaction_delete(self):
        finac.transaction_delete(result.transaction2_id)
        self.assertEqual(finac.account_balance('TEST.TEST'), 100)
        self.assertEqual(finac.account_balance('TEST2.TEST'), 0)

    def test021_transaction_purge(self):
        self.assertEqual(finac.purge()['transaction'], 1)

    def test030_delete_account_and_movements(self):
        finac.transaction_move('TEST2.TEST', 'TEST.TEST', 60, 'test',
                               'Test move')
        finac.account_delete('TEST.TEST')
        self.assertEqual(finac.account_balance('TEST2.TEST'), 60)

    def test040_overdraft(self):

        # allow overdraft
        finac.account_create('TEST3.TEST',
                             'EUR',
                             'Test account',
                             'current',
                             max_overdraft=900)
        finac.transaction_create('TEST3.TEST', 100)
        finac.transaction_move('TEST2.TEST', 'TEST3.TEST', 1000)
        self.assertEqual(finac.account_balance('TEST3.TEST'), -900)

        # forbid overdraft
        finac.account_create('TEST4.TEST',
                             'EUR',
                             'Test account',
                             'current',
                             max_overdraft=200)
        finac.transaction_create('TEST3.TEST', 1200)
        try:
            finac.transaction_move('TEST2.TEST', 'TEST3.TEST', 2000)
            raise RuntimeError('OverdraftError not raised')
        except finac.OverdraftError:
            self.assertEqual(finac.account_balance('TEST3.TEST'), 300)

    def test041_max_balance(self):
        finac.account_create('TEST5.TEST', 'EUR', max_balance=100)
        finac.account_create('TEST6.TEST', 'EUR', max_balance=100)
        finac.transaction_create('TEST5.TEST', 10)
        try:
            finac.transaction_create('TEST5.TEST', 101)
            raise RuntimeError('OverlimitError not raised')
        except finac.OverlimitError:
            self.assertEqual(finac.account_balance('TEST5.TEST'), 10)
        finac.transaction_create('TEST6.TEST', 100)
        try:
            finac.transaction_move('TEST6.TEST', 'TEST5.TEST', 10)
            raise RuntimeError('OverlimitError not raised')
        except finac.OverlimitError:
            self.assertEqual(finac.account_balance('TEST5.TEST'), 10)
            self.assertEqual(finac.account_balance('TEST6.TEST'), 100)

    def test042_overdraft_and_delete(self):
        finac.account_create('TEST42.TEST', 'EUR', max_overdraft=100)
        finac.transaction_create('TEST42.TEST', 10)
        tid = finac.transaction_create('TEST42.TEST',
                                       -100,
                                       mark_completed=False)
        finac.transaction_delete(tid)
        tid = finac.transaction_create('TEST42.TEST',
                                       -100,
                                       mark_completed=False)
        finac.transaction_delete(tid)
        self.assertEqual(finac.account_balance('TEST42.TEST'), 10)

    def test050_hack_overdraft(self):
        finac.account_create('TEST.HO', 'EUR', max_overdraft=100)
        tid = finac.transaction_create('TEST.HO', -100, mark_completed=False)
        self.assertEqual(finac.account_balance('TEST.HO'), -100)
        try:
            finac.transaction_create('TEST.HO', -10, mark_completed=False)
            raise RuntimeError('Overdraft hacked')
        except finac.OverdraftError:
            pass
        finac.transaction_complete(tid)
        self.assertEqual(finac.account_balance('TEST.HO'), -100)

    def test051_hack_overlimit(self):
        finac.account_create('TEST.HL', 'EUR', max_balance=100)
        t1 = finac.transaction_create('TEST.HL', 100, mark_completed=False)
        try:
            t2 = finac.transaction_create('TEST.HL', 100, mark_completed=False)
            finac.transaction_complete(t1)
            finac.transaction_complete(t2)
            raise RuntimeError('Overlimit hacked')
        except finac.OverlimitError:
            return

    def test060_currency_rate_set(self):
        finac.currency_create('AUD')
        finac.currency_set_rate('EUR', 'USD', 1.5, date='2019-01-01')
        finac.currency_set_rate('EUR/USD', value=2)
        try:
            finac.currency_rate('EUR', 'USD', date='2018-01-01')
            raise RuntimeError('Rate not found not raised')
        except finac.RateNotFound:
            pass
        self.assertEqual(finac.currency_rate('EUR', 'USD', date='2019-01-05'),
                         1.5)
        self.assertEqual(finac.currency_rate('EUR', 'USD'), 2)

    def test061_currency_rate_easyget(self):
        finac.config.rate_allow_reverse = False
        try:
            finac.currency_rate('USD', 'EUR', date='2019-01-05')
            raise RuntimeError('Rate not found not raised')
        except:
            pass
        finac.config.rate_allow_reverse = True
        self.assertEqual(finac.currency_rate('USD', 'EUR', date='2019-01-05'),
                         1 / 1.5)
        self.assertEqual(finac.currency_rate('USD', 'EUR'), 1 / 2)

    def test062_currency_rate_delete(self):
        finac.currency_set_rate('EUR', 'USD', value=1.8, date='2018-12-01')
        self.assertEqual(finac.currency_rate('EUR', 'USD', date='2019-01-05'),
                         1.5)
        finac.currency_delete_rate('EUR', 'USD', date='2019-01-01')
        self.assertEqual(finac.currency_rate('EUR', 'USD', date='2019-01-05'),
                         1.8)

    def test070_test_targets_and_tags(self):
        finac.account_create('TT1', 'EUR', tp='credit')
        finac.account_create('TT2', 'EUR', tp='saving')
        finac.transaction_create('TT1', 1000)
        finac.transaction_create('TT2', 1000)
        self.assertEqual(finac.account_balance('TT1'), 1000)
        self.assertEqual(finac.account_balance('TT2'), 1000)
        finac.transaction_create('TT1', target=1500)
        finac.transaction_create('TT2', target=800)
        self.assertEqual(finac.account_balance('TT1'), 1500)
        self.assertEqual(finac.account_balance('TT2'), 800)
        finac.transaction_move('TT1', 'TT2', target_ct=700, tag='loans')
        self.assertEqual(finac.account_balance('TT1'), 1600)
        self.assertEqual(finac.account_balance('TT2'), 700)
        finac.transaction_move('TT2', 'TT1', target_dt=2000, tag='loans')
        self.assertEqual(finac.account_balance('TT1'), 300)
        self.assertEqual(finac.account_balance('TT2'), 2000)
        self.assertEqual(len(list(finac.account_statement('TT1', tag='loans'))),
                         2)
        print()
        finac.ls('TT2')
        print()

    def test071_test_list(self):
        for c in list(finac.account_list()):
            if c['account'] == 'TT1':
                self.assertEqual(c['balance'], 300)
            elif c['account'] == 'TT2':
                self.assertEqual(c['balance'], 2000)
        print()
        finac.ls()
        print()

    def test099_delete_currency(self):
        finac.currency_delete('NZD')


if __name__ == '__main__':
    try:
        if sys.argv[1] == 'debug':
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    except:
        pass
    finac.init(db=TEST_DB, keep_integrity=True)
    test_suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    test_result = unittest.TextTestRunner().run(test_suite)
    sys.exit(not test_result.wasSuccessful())
