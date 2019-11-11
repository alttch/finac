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

TEST_DB = '/tmp/finac-test.db'

result = SimpleNamespace()
config = SimpleNamespace(remote=False)


class Test(unittest.TestCase):

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
        finac.tr('active1', 100)
        finac.tr('active2', 100)

    def test701_passive_tr_debit(self):
        finac.tr('passive1', 100)
        self.assertEqual(finac.account_balance('passive1'), 100)

    def test702_passive_tr_credit(self):
        finac.tr('passive1', -80)
        self.assertEqual(finac.account_balance('passive1'), 20)

    def test703_passive_mv_from_active(self):
        finac.mv(dt='passive1', ct='active1', amount=10)
        self.assertEqual(finac.account_balance('passive1'), 10)
        self.assertEqual(finac.account_balance('active1'), 90)

    def test704_passive_mv_to_active(self):
        finac.mv(dt='active1', ct='passive1', amount=10)
        self.assertEqual(finac.account_balance('passive1'), 20)
        self.assertEqual(finac.account_balance('active1'), 100)

    def test705_target_ct_from_active(self):
        finac.mv(dt='passive1', ct='active1', target_ct=90)
        self.assertEqual(finac.account_balance('passive1'), 10)
        self.assertEqual(finac.account_balance('active1'), 90)

    def test705_target_dt_from_active(self):
        finac.tr('passive1', 10)
        finac.mv(dt='passive1', ct='active1', target_dt=0)
        self.assertEqual(finac.account_balance('passive1'), 0)
        self.assertEqual(finac.account_balance('active1'), 70)

    def test706_target_ct_to_active(self):
        finac.mv(dt='active1', ct='passive1', target_ct=30)
        self.assertEqual(finac.account_balance('passive1'), 30)
        self.assertEqual(finac.account_balance('active1'), 100)

    def test706_target_dt_to_active(self):
        finac.mv(dt='active1', ct='passive1', target_dt=200)
        self.assertEqual(finac.account_balance('passive1'), 130)
        self.assertEqual(finac.account_balance('active1'), 200)

    def test710_passive_mv_from_active_crosscur(self):
        finac.tr('passive2', 10)
        self.assertEqual(finac.account_balance('passive2'), 10)
        finac.mv(dt='passive2', ct='active1', amount=5.5, xdt=False)
        self.assertEqual(finac.account_balance('passive2'), 5)
        self.assertEqual(finac.account_balance('active1'), 194.5)
        finac.mv(dt='passive2', ct='active1', amount=5, xdt=True)
        self.assertEqual(finac.account_balance('passive2'), 0)
        self.assertEqual(finac.account_balance('active1'), 189)

    def test711_passive_mv_to_active_crosscur(self):
        # TODO
        pass

    def test720_mv_between_passive(self):
        finac.mv(dt='passive3', ct='passive1', amount=30)
        self.assertEqual(finac.account_balance('passive1'), 100)
        self.assertEqual(finac.account_balance('passive3'), 30)

    def test721_mv_target_dt_between_passive(self):
        finac.mv(dt='passive3', ct='passive1', target_dt=40)
        self.assertEqual(finac.account_balance('passive3'), 40)
        self.assertEqual(finac.account_balance('passive1'), 90)

    def test722_mv_target_ct_between_passive(self):
        finac.mv(dt='passive3', ct='passive1', target_ct=80)
        self.assertEqual(finac.account_balance('passive3'), 50)
        self.assertEqual(finac.account_balance('passive1'), 80)

    def test730_mv_btw_passive_crosscur(self):
        # DOESN'T WORK
        # TODO
        pass

    def test731_mv_btw_passive_target_dt_crosscur(self):
        # TODO
        pass

    def test732_mv_btw_passive_target_ct_crosscur(self):
        # TODO
        pass

    def test752_passive_mv_taget_ct_from_active(self):
        # TODO
        pass

    def test753_passive_mv_taget_dt_from_active(self):
        # TODO
        pass

    def test754_passive_mv_taget_dt_to_active(self):
        # TODO
        pass

    def test755_passive_mv_taget_ct_to_active(self):
        # TODO
        pass


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
