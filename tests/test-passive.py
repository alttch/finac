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
import time

from types import SimpleNamespace

# TEST_DB = '/tmp/finac-test.db'
TEST_DB = 'mysql+pymysql://admin:admin@localhost/my_finac'

result = SimpleNamespace()
config = SimpleNamespace(remote=False)


class Test(unittest.TestCase):

    def set_balance(self, account, balance):
        finac.tr(account, target=balance)
        self.assertEqual(finac.account_balance(account), balance)

    def run(self, result=None):
        if not result.errors:
            super(Test, self).run(result)

    def test700_passive_prepare(self):
        for a in finac.asset_list():
            finac.asset_delete(a['asset'])
        finac.asset_create('usd')
        finac.asset_create('eur')
        finac.asset_set_rate('eur/usd', value=1.1)
        finac.account_create('active1', 'usd')
        finac.account_create('active2', 'eur')
        finac.account_create('passive1', 'usd', tp='finagent')
        finac.account_create('passive3', 'usd', tp='finagent')
        finac.account_create('passive2', 'eur', tp='finagent')

    def test701_passive_tr_debit(self):
        finac.tr('passive1', 100)
        self.assertEqual(finac.account_balance('passive1'), 100)

    def test702_passive_tr_credit(self):
        finac.tr('passive1', -80)
        self.assertEqual(finac.account_balance('passive1'), 20)

    def test703_passive_mv_from_active(self):
        self.set_balance('active1', 100)
        self.set_balance('passive1', 10)
        finac.mv(dt='passive1', ct='active1', amount=10)
        self.assertEqual(finac.account_balance('passive1'), 0)
        self.assertEqual(finac.account_balance('active1'), 90)

    def test704_passive_mv_to_active(self):
        self.set_balance('active1', 100)
        self.set_balance('passive1', 10)
        finac.mv(dt='active1', ct='passive1', amount=10)
        self.assertEqual(finac.account_balance('passive1'), 20)
        self.assertEqual(finac.account_balance('active1'), 110)

    def test705_target_ct_from_active(self):
        self.set_balance('active1', 100)
        self.set_balance('passive1', 10)
        finac.mv(dt='passive1', ct='active1', target_ct=90)
        self.assertEqual(finac.account_balance('passive1'), 0)
        self.assertEqual(finac.account_balance('active1'), 90)

    def test705_target_dt_from_active(self):
        self.set_balance('active1', 100)
        self.set_balance('passive1', 10)
        finac.mv(dt='passive1', ct='active1', target_dt=0)
        self.assertEqual(finac.account_balance('passive1'), 0)
        self.assertEqual(finac.account_balance('active1'), 90)

    def test706_target_ct_to_active(self):
        self.set_balance('active1', 100)
        self.set_balance('passive1', 10)
        finac.mv(dt='active1', ct='passive1', target_ct=20)
        self.assertEqual(finac.account_balance('passive1'), 20)
        self.assertEqual(finac.account_balance('active1'), 110)

    def test706_target_dt_to_active(self):
        self.set_balance('active1', 100)
        self.set_balance('passive1', 10)
        finac.mv(dt='active1', ct='passive1', target_dt=110)
        self.assertEqual(finac.account_balance('passive1'), 20)
        self.assertEqual(finac.account_balance('active1'), 110)

    def test710_passive_mv_from_active_crosscur(self):
        self.set_balance('active1', 100)
        self.set_balance('passive2', 10)
        finac.mv(dt='passive2', ct='active1', amount=5.5, xdt=False)
        self.assertEqual(finac.account_balance('passive2'), 5)
        self.assertEqual(finac.account_balance('active1'), 94.5)
        finac.mv(dt='passive2', ct='active1', amount=5, xdt=True)
        self.assertEqual(finac.account_balance('passive2'), 0)
        self.assertEqual(finac.account_balance('active1'), 89)

    def test711_passive_mv_to_active_crosscur(self):
        self.set_balance('active1', 100)
        self.set_balance('passive2', 10)
        finac.mv(dt='active1', ct='passive2', amount=5, xdt=False)
        self.assertEqual(finac.account_balance('passive2'), 15)
        self.assertEqual(finac.account_balance('active1'), 105.5)
        finac.mv(dt='active1', ct='passive2', amount=5.5, xdt=True)
        self.assertEqual(finac.account_balance('passive2'), 20)
        self.assertEqual(finac.account_balance('active1'), 111)

    def test720_mv_between_passive(self):
        self.set_balance('passive1', 100)
        self.set_balance('passive3', 100)
        finac.mv(dt='passive3', ct='passive1', amount=30)
        self.assertEqual(finac.account_balance('passive1'), 70)
        self.assertEqual(finac.account_balance('passive3'), 130)

    def test721_mv_target_dt_between_passive(self):
        self.set_balance('passive1', 100)
        self.set_balance('passive3', 100)
        finac.mv(dt='passive3', ct='passive1', target_dt=140)
        self.assertEqual(finac.account_balance('passive3'), 140)
        self.assertEqual(finac.account_balance('passive1'), 60)

    def test722_mv_target_ct_between_passive(self):
        self.set_balance('passive1', 100)
        self.set_balance('passive3', 100)
        finac.mv(dt='passive3', ct='passive1', target_ct=80)
        self.assertEqual(finac.account_balance('passive3'), 120)
        self.assertEqual(finac.account_balance('passive1'), 80)

    def test730_mv_btw_passive_crosscur(self):
        finac.account_create('pass.supplier', 'eur', tp='supplier')
        finac.account_create('pass.finagent', 'usd', tp='finagent')
        self.set_balance('pass.supplier', 100)
        self.set_balance('pass.finagent', 500)
        finac.mv(dt='pass.finagent', ct='pass.supplier', amount=10, xdt=False)
        self.assertEqual(finac.account_balance('pass.supplier'), 90)
        self.assertEqual(finac.account_balance('pass.finagent'), 511)

    def test731_mv_btw_passive_target_dt_crosscur(self):
        finac.mv(dt='pass.finagent', ct='pass.supplier', target_dt=522)
        self.assertEqual(finac.account_balance('pass.supplier'), 80)
        self.assertEqual(finac.account_balance('pass.finagent'), 522)

    def test732_mv_btw_passive_target_ct_crosscur(self):
        finac.mv(dt='pass.finagent', ct='pass.supplier', target_ct=60)
        self.assertEqual(finac.account_balance('pass.supplier'), 60)
        self.assertEqual(finac.account_balance('pass.finagent'), 544)

    def test752_passive_mv_taget_ct_from_active(self):
        finac.account_create('active.supplier', 'usd')
        self.set_balance('active.supplier', 50)
        finac.mv(dt='pass.supplier', ct='active.supplier', target_ct=40)
        self.assertEqual(finac.account_balance('pass.supplier'), 50.91)
        self.assertEqual(finac.account_balance('active.supplier'), 40)

    def test753_passive_mv_taget_dt_from_active(self):
        finac.mv(dt='pass.supplier', ct='active.supplier', target_dt=40)
        self.assertEqual(finac.account_balance('pass.supplier'), 40)
        self.assertEqual(finac.account_balance('active.supplier'), 28)

    def test754_passive_mv_taget_dt_to_active(self):
        finac.mv(dt='active.supplier', ct='pass.supplier', target_dt=40)
        self.assertEqual(finac.account_balance('pass.supplier'), 50.91)
        self.assertEqual(finac.account_balance('active.supplier'), 40)

    def test755_passive_mv_taget_ct_to_active(self):
        finac.mv(dt='active.supplier', ct='pass.supplier', target_ct=70)
        self.assertEqual(finac.account_balance('pass.supplier'), 70)
        self.assertEqual(finac.account_balance('active.supplier'), 61)

    def test755_passive_mv_targets_from_active_minus(self):
        finac.account_create('active.sup', 'usd')
        finac.account_create('pass.sup', 'eur', tp='tax')
        self.set_balance('active.sup', -50)
        self.set_balance('pass.sup', 0)
        finac.mv(dt='pass.sup', ct='active.sup', target_ct=-60)
        self.assertEqual(finac.account_balance('pass.sup'), -9.09)
        self.assertEqual(finac.account_balance('active.sup'), -60)
        finac.mv(dt='pass.sup', ct='active.sup', target_dt=-20)
        self.assertEqual(finac.account_balance('pass.sup'), -20)
        self.assertEqual(finac.account_balance('active.sup'), -72)

    def test756_passive_mv_targets_to_active_minus(self):
        finac.mv(dt='active.sup', ct='pass.sup', target_dt=-52)
        self.assertEqual(finac.account_balance('pass.sup'), -1.82)
        self.assertEqual(finac.account_balance('active.sup'), -52)
        finac.mv(dt='active.sup', ct='pass.sup', target_ct=-1)
        self.assertEqual(finac.account_balance('pass.sup'), -1)
        self.assertEqual(finac.account_balance('active.sup'), -51.10)


if __name__ == '__main__':
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument('--debug', help='Debug mode', action='store_true')
    ap.add_argument('--remote', help='Test remote API', action='store_true')
    a = ap.parse_args()
    try:
        if a.debug:
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    except:
        pass
    config.remote = a.remote
    if a.remote:
        finac.init(api_uri='http://localhost:5000/jrpc', api_key='secret')
    else:
        try:
            os.unlink(TEST_DB)
        except:
            pass
        finac.init(db=TEST_DB, keep_integrity=True)
        finac.core.rate_cache = None
    test_suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    test_result = unittest.TextTestRunner().run(test_suite)
    sys.exit(not test_result.wasSuccessful())
