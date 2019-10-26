__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2019 Altertech'
__license__ = 'MIT'

__version__ = '0.0.12'

from sqlalchemy.exc import IntegrityError

from functools import lru_cache

# assets
ACCOUNT_CREDIT = 0
ACCOUNT_CASH = 1
ACCOUNT_CURRENT = 2
ACCOUNT_SAVING = 100

ACCOUNT_GS = 200
ACCOUNT_SUPPLIER = 201
ACCOUNT_CUSTOMER = 202
ACCOUNT_FINAGENT = 203

ACCOUNT_HOLDING = 300

# taxes
ACCOUNT_TAXES = 1000

# special
ACCOUNT_TRANSIT = 2000
ACCOUNT_ESCROW = 2001
ACCOUNT_VIRTUAL = 2002
ACCOUNT_TEMP = 2003

# service
ACCOUNT_EXCHANGE = 5000

ACCOUNT_TYPE_NAMES = {
    ACCOUNT_CREDIT: 'credit',
    ACCOUNT_CASH: 'cash',
    ACCOUNT_CURRENT: 'current',
    ACCOUNT_SAVING: 'saving',
    ACCOUNT_TRANSIT: 'transit',
    ACCOUNT_ESCROW: 'escrow',
    ACCOUNT_HOLDING: 'holding',
    ACCOUNT_VIRTUAL: 'virtual',
    ACCOUNT_TEMP: 'temp',
    ACCOUNT_EXCHANGE: 'exchange',
    ACCOUNT_GS: 'gs',
    ACCOUNT_SUPPLIER: 'supplier',
    ACCOUNT_CUSTOMER: 'customer',
    ACCOUNT_FINAGENT: 'finagent',
    ACCOUNT_TAXES: 'taxes'
}

ACCOUNT_TYPE_IDS = {v: k for k, v in ACCOUNT_TYPE_NAMES.items()}

LOCK_DELAY = 0.1

import sqlalchemy as sa
import dateutil.parser
import datetime
import time
import string
import random
import os
import logging

from sqlalchemy import text as sql

from types import SimpleNamespace
from collections import OrderedDict

import threading

from .db_set import init_db

logger = logging.getLogger('finac')

_db = SimpleNamespace(engine=None)

config = SimpleNamespace(db=None,
                         keep_integrity=True,
                         lazy_exchange=True,
                         rate_allow_reverse=True,
                         colorize=True,
                         rate_allow_cross=True,
                         base_currency='USD',
                         date_format='%Y-%m-%d %H:%M:%S')

lock_purge = threading.Lock()
lock_account_token = threading.Lock()

account_lockers = {}


def gen_random_str(length=64):
    symbols = string.ascii_letters + '0123456789'
    return ''.join(random.choice(symbols) for i in range(length))


def format_date(d):
    if d is not None:
        if config.date_format is None: return d
        else:
            return datetime.datetime.strftime(
                datetime.datetime.fromtimestamp(d), config.date_format)


def parse_date(d, return_timestamp=True):
    try:
        d = float(d)
        if d > 3000:
            return d
        else:
            d = int(d)
    except:
        pass
    if not isinstance(d, datetime.datetime):
        dt = dateutil.parser.parse(str(d))
    else:
        dt = d
    return dt.timestamp() if return_timestamp else dt


@lru_cache(maxsize=256)
def currency_precision(currency):
    """
    Get precision (digits after comma) for the currency
    Note: currency precision is cached, so process restart required if changed
    """
    d = get_db().execute(sql('select precs from currency where code=:code'),
                         code=currency.upper()).fetchone()
    if not d:
        raise ResourceNotFound
    return d.precs


def format_amount(i, currency):
    """
    Format amount for values and exchange operations. Default: apply currency
    precision
    """
    return round(i, currency_precision(currency))


class ResourceNotFound(Exception):
    """
    Raised when accessed resource is not found
    """
    pass


class RateNotFound(Exception):
    """
    Raised when accessed currency rate is not found
    """
    pass


class OverdraftError(Exception):
    """
    Raised when transaction is trying to break account max overdraft
    """
    pass


class OverlimitError(Exception):
    """
    Raised when transaction is trying to break account max balance
    """
    pass


class ResourceAlreadyExists(Exception):
    """
    Raised when trying to create already existing resource
    """
    pass


class AccountLocker:

    def __init__(self):
        self.token = None
        self.counter = 0
        self._lock = threading.Lock()

    def acquire(self, token=None):
        if token:
            with self._lock:
                if token == self.token:
                    self.counter += 1
                    return token
        while True:
            with self._lock:
                if not self.counter:
                    self.token = gen_random_str() if not token else token
                    self.counter = 1
                    return self.token
            time.sleep(LOCK_DELAY)

    def release(self, token):
        with self._lock:
            if token != self.token: raise RuntimeError('Invalid token')
            if self.counter < 1: raise RuntimeError('Resource not locked')
            self.counter -= 1
            if not self.counter:
                self.token = None


class ForeignKeysListener(sa.interfaces.PoolListener):

    def connect(self, dbapi_con, con_record):
        try:
            dbapi_con.execute('pragma foreign_keys=ON')
        except:
            pass


def get_db_engine(db_uri):
    return sa.create_engine(db_uri, listeners=[ForeignKeysListener()])


def get_db():
    l = threading.local()
    try:
        l.db.execute('select 1')
    except:
        if not _db.engine:
            raise RuntimeError('finac not initialized')
        l.db = _db.engine.connect()
        return l.db


def init(db, **kwargs):
    """
    Initialize finac database and configuration

    Args:
        db: SQLAlchemy DB URI or sqlite file name
        keep_integrity: finac should keep database integrity (lock accounts,
            watch overdrafts, overlimits etc. Default is True
        lazy_exchange: allow direct exchange operations betwen accounts.
            Default: True
        rate_allow_reverse: allow reverse rates for lazy exchange (e.g. if
            "EUR/USD" pair exists but no USD/EUR, use 1 / "EUR/USD"
        rate_allow_cross: if exchange rate is not found, allow finac to look
            for the nearest cross-currency rate (NOT IMPLEMENTED YET)
        base_currency: default base currency. Default is "USD"
        date_format: default date format in statements
    """
    config.db = db
    for k, v in kwargs.items():
        if not hasattr(config, k):
            raise RuntimeError('Parameter {} is invalid'.format(k))
        setattr(config, k, v)
    db_uri = db
    if db_uri.find('.db') != -1:
        db_uri = 'sqlite:///' + os.path.expanduser(db_uri)
    elif not db_uri.__contains__('mysql+pymysql'):
        raise RuntimeError(
            'DB URI configuration: mysql+pymysql://user:pass@host/db_name')
    _db.engine = get_db_engine(db_uri)
    init_db(_db.engine)


def currency_create(currency, precision=2):
    """
    Create currency

    Args:
        currency: currency code (e.g. "CAD", "AUD")
        precision: precision (digits after comma) for statements and exchange
            operations. Default is 2 digits
    """
    currency = currency.upper()
    logger.info('Creating currency {}'.format(currency))
    try:
        get_db().execute(sql("""
        insert into currency(code, precs) values(:code, :precision)"""),
                         code=currency,
                         precision=precision)
    except IntegrityError:
        raise ResourceAlreadyExists(currency)


def currency_list():
    """
    List currencies
    """
    r = get_db().execute(
        sql("""
        select code, precs from currency order by code"""))
    while True:
        d = r.fetchone()
        if not d: break
        row = OrderedDict()
        row['currency'] = d.code
        row['precision'] = d.precs
        yield row


def currency_list_rates(currency, start=None, end=None):
    """
    List currency rates

    Currency can be specified either as code, or as pair "code/code"
    """
    cond = ''
    currency = currency.upper()
    if start:
        dts = parse_date(start)
        cond += (' and ' if cond else '') + 'd >= {}'.format(dts)
    if end:
        dte = parse_date(end)
        cond += (' and ' if cond else '') + 'd <= {}'.format(dte)
    if currency.find('/') != -1:
        currency_from, currency_to = currency.split('/')
        cond += (' and '
                 if cond else '') + 'cf.code = "{}" and ct.code = "{}"'.format(
                     currency_from, currency_to)
    else:
        cond += (' and ' if cond else
                 '') + '(cf.code = "{code}" or ct.code = "{code}")'.format(
                     code=currency)
    r = get_db().execute(
        sql("""
        select cf.code as currency_from,
                ct.code as currency_to,
                d, value
        from currency_rate
            join currency as cf on currency_from_id = cf.id
            join currency as ct on currency_to_id = ct.id
                where {cond}
    """.format(cond=cond)))
    while True:
        d = r.fetchone()
        if not d: break
        row = OrderedDict()
        row['currency_from'] = d.currency_from
        row['currency_to'] = d.currency_to
        row['date'] = format_date(d.d)
        row['value'] = d.value
        yield row


def currency_delete(currency):
    """
    Delete currency

    Warning: all accounts linked to this currency will be deleted as well
    """
    logger.warning('Deleting currency {}'.format(currency.upper()))
    if not get_db().execute(sql("""
    delete from currency where code=:code"""),
                            code=currency.upper()).rowcount:
        logger.error('Currency {} not found'.format(currency.upper()))
        raise ResourceNotFound


def currency_set_rate(currency_from, currency_to=None, value=None, date=None):
    """
    Set currency rate

    Args:
        currency_from: currency from code
        currency_to: currency to code
        value: exchange rate value
        date: date/time exchange rate is set on (default: now)

    Function can be also called as e.g. currency_set_rate('EUR/USD', value=1.1)
    """
    if value is None:
        raise ValueError('Currency rate value is not specified')
    if date is None:
        date = time.time()
    else:
        date = parse_date(date)
    if currency_from.find('/') != -1 and currency_to is None:
        currency_from, currency_to = currency_from.split('/')
    logging.info('Setting rate for {}/{} to {} for {}'.format(
        currency_from.upper(), currency_to.upper(), value, format_date(date)))
    get_db().execute(sql("""
    insert into currency_rate (currency_from_id, currency_to_id, d, value)
    values
    (
        (select id from currency where code=:f),
        (select id from currency where code=:t),
        :d,
        :value
    )
    """),
                     f=currency_from.upper(),
                     t=currency_to.upper(),
                     d=date,
                     value=value)


def currency_delete_rate(currency_from, currency_to, date):
    """
    Delete currrency rate
    """
    if currency_from.find('/') != -1 and currency_to is None:
        currency_from, currency_to = currency_from.split('/')
    date = parse_date(date)
    logging.info('Deleting rate for {}/{} for {}'.format(
        currency_from.upper(), currency_to.upper(), format_date(date)))
    if not get_db().execute(sql("""
    delete from currency_rate where
        currency_from_id=(select id from currency where code=:f)
        and
        currency_to_id=(select id from currency where code=:t)
        and d=:d
        """),
                            f=currency_from.upper(),
                            t=currency_to.upper(),
                            d=date).rowcount:
        logger.error('Currency rate {}/{} for {} not found'.format(
            currency_from.upper(), currency_to.upper(), format_date(date)))
        raise ResourceNotFound


def currency_rate(currency_from, currency_to=None, date=None):
    """
    Get currency rate for the specified date

    If no date is specified, get currency rate for now

    Function can be also called as e.g. currency_rate('EUR/USD')
    """
    if date is None:
        date = time.time()
    else:
        date = parse_date(date)
    if currency_from.find('/') != -1 and currency_to is None:
        currency_from, currency_to = currency_from.split('/')
    if currency_from.upper() == currency_to.upper():
        return 1
    db = get_db()

    def _get_rate(cf, ct):
        r = db.execute(sql("""
            select value from currency_rate
                join currency as cfrom on currency_from_id=cfrom.id
                join currency as cto on currency_to_id=cto.id
            where d <= :d and cfrom.code = :f and cto.code = :t
            order by d desc limit 1
            """),
                       d=date,
                       f=cf.upper(),
                       t=ct.upper())
        return r.fetchone()

    d = _get_rate(currency_from, currency_to)
    if not d:
        if config.rate_allow_reverse:
            d = _get_rate(currency_to, currency_from)
            if not d:
                raise RateNotFound('{}/{} for {}'.format(
                    currency_from.upper(), currency_to.upper(),
                    format_date(date)))
            value = 1 / d.value
        else:
            raise RateNotFound
    else:
        value = d.value
    return value


def account_create(account,
                   currency,
                   tp='current',
                   note=None,
                   max_overdraft=None,
                   max_balance=None):
    """
    Args:
        currency: currency code
        account: account code
        note: account notes
        tp: account type (credit, current, saving, cash etc.)
        max_overdraft: maximum allowed overdraft (set to negative to force
            account to have minimal positive balance), default is None
            (unlimited)
        max_balance: max allowed account balance, default is None (unlimited)
    """
    if isinstance(tp, int):
        tp_id = tp
    else:
        tp_id = ACCOUNT_TYPE_IDS[tp]
    db = get_db()
    dbt = db.begin()
    logger.info('Creating account {}, currency: {}'.format(
        account.upper(), currency.upper()))
    try:
        r = db.execute(sql("""
        insert into account(code, note, tp, currency_id, max_overdraft,
        max_balance) values
        (:code, :note, :tp,
            (select id from currency where code=:currency),
            :max_overdraft, :max_balance)"""),
                       code=account.upper(),
                       note=note,
                       tp=tp_id,
                       currency=currency.upper(),
                       max_overdraft=max_overdraft,
                       max_balance=max_balance)
        db.execute(sql("""
            insert into transact(account_debit_id, amount, d_created, d) values
            (:account_id, 0, 0, 0)
            """),
                   account_id=r.lastrowid)
        db.execute(sql("""
            insert into transact(account_credit_id, amount, d_created, d) values
            (:account_id, 0, 0, 0)
            """),
                   account_id=r.lastrowid)
        dbt.commit()
    except IntegrityError:
        dbt.rollback()
        raise ResourceAlreadyExists(account.upper())
    except:
        logger.error('Unable to create account {}'.format(account.upper()))
        dbt.rollback()
        raise


def account_info(account):
    """
    Get dict with account info
    """
    r = get_db().execute(sql("""
            select account.code as account_code, account.note, account.tp,
            currency.code as currency, max_overdraft, max_balance
            from account join
            currency on account.currency_id = currency.id
            where account.code = :account"""),
                         account=account.upper())
    d = r.fetchone()
    if not d: raise ResourceNotFound
    return {
        'code': d.account_code,
        'note': d.note,
        'type': ACCOUNT_TYPE_NAMES[d.tp],
        'tp': d.tp,
        'currency': d.currency,
        'max_overdraft': d.max_overdraft,
        'max_balance': d.max_balance
    }


def transaction_info(transaction_id):
    """
    Get dict with transaction info
    """
    r = get_db().execute(sql("""
            select transact.amount as amount, transact.tag as tag,
            transact.note as note,
            transact.d_created as d_created,
            transact.d as d,
            chain_transact_id,
            dt.code as debit,
            ct.code as credit
            from transact left join
            account as dt on
                transact.account_debit_id = dt.id
            left join
            account as ct on
                transact.account_credit_id = ct.id
            where transact.id = :transaction_id"""),
                         transaction_id=transaction_id)
    d = r.fetchone()
    if not d: raise ResourceNotFound
    return {
        'id': transaction_id,
        'amount': d.amount,
        'tag': d.tag,
        'note': d.note,
        'created': d.d_created,
        'completed': d.d,
        'chain_transact_id': d.chain_transact_id,
        'dt': d.debit if hasattr(d, 'debit') else None,
        'ct': d.credit if hasattr(d, 'credit') else None,
    }


def transaction_apply(fname):
    """
    Apply transaction yaml file

    File format example:

    transactions:
      - account: acc1
	amount: 500
	tag: test
      - dt: acc2
	ct: acc1
	amount: 200
	tag: moving

    If "account" is specified, function transaction_create is called, otherwise
    transaction_move. All arguments are passed to the functions as-is
    """
    import yaml
    try:
        yaml.warnings({'YAMLLoadWarning': False})
    except:
        pass
    with open(fname) as fh:
        transactions = yaml.load(fh)['transactions']
    for t in transactions:
        if 'account' in t:
            transaction_create(**t)
        else:
            transaction_move(**t)


def account_delete(account, lock_token=None):
    """
    Delete account
    """
    account = account.upper()
    logger.warning('Deleting account {}'.format(account))
    token = account_lock(account, lock_token)
    try:
        if not get_db().execute(sql("""
        delete from transact where
        account_debit_id=(select id from account where code=:code) or
        account_credit_id=(select id from account where code=:code) and d=0"""),
                                code=account).rowcount:
            raise ResourceNotFound
        if not get_db().execute(sql("""
        delete from account where code=:code"""),
                                code=account).rowcount:
            logger.error('Account {} not found'.format(account))
            raise ResourceNotFound
    finally:
        account_unlock(account, token)


def account_lock(account, token):
    """
    Lock account

    Account locking works similarly to threading.RLock(), but instead of thread
    ID, token is used.

    If token is provided and match the current lock token, lock counter will be
    increased and lock is passed

    When locked, all account transaction operation are freezed until unlocked
    (unless current lock token is provided for the operation)

    Returns:
        specified lock token or new lock token if no token provided
    """
    account = account.upper() if account else account
    if config.keep_integrity:
        with lock_account_token:
            if account in account_lockers:
                l = account_lockers[account]
            else:
                l = AccountLocker()
                account_lockers[account] = l
        return l.acquire(token)


def account_unlock(account, token):
    """
    Unlock account

    Note that if you call account_lock, you must always unlock account,
    otherwise it will be locked until process restart
    """
    if config.keep_integrity:
        with lock_account_token:
            l = account_lockers.get(account.upper())
        if not l: raise ResourceNotFound
        return l.release(token)


def _ckw(kw, allowed):
    for c in kw:
        if c not in allowed:
            raise ValueError('Invalid parameter: {}'.format(c))


def _update(objid, tbl, objidf, kw):
    c = objid
    if isinstance(c, str):
        c = c.upper()
    for k, v in kw.items():
        if k == 'code':
            v = v.upper()
        if not get_db().execute(sql("""
        update {tbl} set {f} = :val where {objidf} = :id
        """.format(tbl=tbl, f=k, objidf=objidf)),
                                val=v,
                                id=c).rowcount:
            raise ResourceNotFound('{} {}'.format(tbl, objid))
        if k == 'code':
            c = v


def account_update(account, **kwargs):
    """
    Update account parameters

    Parameters, allowed to be updated:
        code, note, tp, max_balance, max_overdraft
    """
    _ckw(kwargs, ['code', 'note', 'tp', 'max_balance', 'max_overdraft'])
    kw = kwargs.copy()
    if 'tp' in kw:
        kw['tp'] = ACCOUNT_TYPE_IDS[kw['tp']]
    _update(account, 'account', 'code', kw)


def currency_update(currency, **kwargs):
    """
    Update currency parameters

    Parameters, allowed to be updated:
        code, precision

    Note that currency precision is cached and requires process restart if
    changed
    """
    _ckw(kwargs, ['code', 'precision'])
    kw = kwargs.copy()
    if 'precision' in kw:
        kw['precs'] = kw['precision']
        del kw['precision']
    _update(currency, 'currency', 'code', kw)


def transaction_update(transaction_id, **kwargs):
    """
    Update transaction parameters

    Parameters, allowed to be updated:
        tag, note
    """
    _ckw(kwargs, ['tag', 'note', 'created', 'completed'])
    kw = kwargs.copy()
    if 'created' in kw:
        kw['d_created'] = parse_date(kw['created'])
        del kw['created']
    if 'completed' in kw:
        kw['d'] = parse_date(kw['completed'])
        del kw['completed']
    _update(transaction_id, 'transact', 'id', kw)


def transaction_create(account,
                       amount=None,
                       tag=None,
                       note=None,
                       date=None,
                       completion_date=None,
                       mark_completed=True,
                       target=None,
                       lock_token=None):
    """
    Create new simple transaction on account

    Args:
        account: account code
        amount: tranasction amount (>0 for debit, <0 for credit)
        tag: transaction tag
        note: transaction note
        date: transaction date
        completion_date: transaction completion date
        mark_completed: if no completion_date is specified, set completion date
            equal to creation. Default is True

        target: if no amount but target is specified, calculate transaction
            amount to make final balance equal to target
    """
    if amount is not None and target is not None:
        raise ValueError('Amount and target can not be specified together')
    elif amount is None and target is None:
        raise ValueError('Specify amount or target')
    token = account_lock(account, lock_token)
    try:
        if target is not None:
            balance = account_balance(account)
            if balance > target:
                amount = -1 * (balance - target)
            elif balance < target:
                amount = target - balance
            else:
                return
        if amount < 0:
            return transaction_move(ct=account,
                                    amount=-1 * amount if amount else None,
                                    tag=tag,
                                    note=note,
                                    date=date,
                                    completion_date=completion_date,
                                    mark_completed=mark_completed,
                                    target_ct=target,
                                    credit_lock_token=token)
        else:
            return transaction_move(dt=account,
                                    amount=amount,
                                    tag=tag,
                                    note=note,
                                    date=date,
                                    completion_date=completion_date,
                                    mark_completed=mark_completed,
                                    target_dt=None,
                                    debit_lock_token=token)
    finally:
        account_unlock(account, token)


def _transaction_move(dt=None,
                      ct=None,
                      amount=0,
                      tag=None,
                      note=None,
                      date=None,
                      completion_date=None,
                      chain_transact_id=None,
                      mark_completed=True,
                      target_ct=None,
                      target_dt=None,
                      _ct_info=None,
                      _dt_info=None):
    if ct == dt:
        raise ValueError('Debit and credit account can not be the same')
    if amount is None and target_ct is None and target_dt is None:
        raise ValueError('Specify amount or target')
    if target_ct is not None and target_dt is not None:
        raise ValueError('Target should be specified either for dt or for ct')
    if target_ct is not None and not ct:
        raise ValueError('Target is specified but ct account not')
    elif target_dt is not None and not dt:
        raise ValueError('Target is specified but dt account not')
    db = get_db()
    ct = ct.upper() if ct else None
    dt = dt.upper() if dt else None
    if target_dt is not None:
        amount = target_dt - account_balance(dt)
    elif target_ct is not None:
        amount = account_balance(ct) - target_ct
    if amount == 0: return
    if amount is not None and amount < 0:
        raise ValueError('Amount should be greater than zero')
    if config.keep_integrity:
        ccur = None
        dcur = None
        if ct:
            acc_info = _ct_info
            ctp = acc_info['tp']
            m = acc_info['max_overdraft']
            if m is not None and account_balance(ct) - amount < -1 * m:
                raise OverdraftError
        if dt:
            acc_info = _dt_info
            dtp = acc_info['tp']
            m = acc_info['max_balance']
            if m is not None and account_balance(dt) + amount > m:
                raise OverlimitError
    if date is None:
        date = time.time()
    else:
        date = parse_date(date)
    if completion_date is None:
        if mark_completed:
            completion_date = date
    else:
        completion_date = parse_date(completion_date)
    return db.execute(sql("""
    insert into transact(account_credit_id, account_debit_id, amount, tag,
    note, d_created, d, chain_transact_id) values
    (
    (select id from account where code=:ct),
    (select id from account where code=:dt),
    :amount, :tag, :note, :d_created, :d, :chain_id)
    """),
                      ct=ct,
                      dt=dt,
                      amount=amount,
                      tag=tag,
                      note=note,
                      d_created=date,
                      d=completion_date,
                      chain_id=chain_transact_id).lastrowid


def transaction_move(dt=None,
                     ct=None,
                     amount=0,
                     tag=None,
                     note=None,
                     date=None,
                     completion_date=None,
                     mark_completed=True,
                     target_ct=None,
                     target_dt=None,
                     rate=None,
                     xdt=True,
                     credit_lock_token=None,
                     debit_lock_token=None):
    """
    Create new standard (double-entry bookkeeping) transaction

    Args:
        ct: source (credit) account code
        dt: target (debit) account code
        amount: transaction amount (always >0)
        tag: transaction tag
        descrption: transaction note
        date: transaction creation daate (default: now)
        completion_date: transaction completion date (default: now)
        mark_completed: mark transaction completed (set completion date)
        target_ct: target credit account balance
        target_dt: target debit account balance
        rate: exchange rate (lazy exchange should be on)
        xdt: for lazy exchange:
            True (default): amount is debited and calculate rate for credit
            False: amount is credited and calculate rate for debit

    Returns:
        transaction id, if lazy exchange performed: tuple of two transactions
    """
    try:
        if ct and dt and ct == dt:
            raise ValueError('Credit and debit account can not be equal')
        ctoken = account_lock(ct, credit_lock_token) if ct else None
        dtoken = account_lock(dt, debit_lock_token) if dt else None
        ct_info = account_info(ct) if ct else None
        dt_info = account_info(dt) if dt else None
        if ct and dt and ct_info['currency'] != dt_info['currency']:
            if config.lazy_exchange:
                if not amount:
                    raise ValueError(
                        'Amount is required for exchange operations')
                if not rate:
                    rate = currency_rate(ct_info['currency'],
                                         dt_info['currency'],
                                         date=date)

            else:
                raise ValueError('Currency mismatch')
            tid1 = _transaction_move(
                ct=ct,
                amount=format_amount(amount / rate, ct_info['currency'])
                if xdt else amount,
                tag=tag,
                note=note,
                date=date,
                completion_date=completion_date,
                mark_completed=mark_completed,
                _ct_info=ct_info)
            tid2 = _transaction_move(dt=dt,
                                     amount=amount if xdt else format_amount(
                                         amount * rate, dt_info['currency']),
                                     tag=tag,
                                     note=note,
                                     date=date,
                                     completion_date=completion_date,
                                     mark_completed=mark_completed,
                                     chain_transact_id=tid1,
                                     _dt_info=dt_info)
            return tid1, tid2
        else:
            return _transaction_move(dt=dt,
                                     ct=ct,
                                     amount=amount,
                                     tag=tag,
                                     note=note,
                                     date=date,
                                     completion_date=completion_date,
                                     mark_completed=mark_completed,
                                     target_ct=target_ct,
                                     target_dt=target_dt,
                                     _ct_info=ct_info,
                                     _dt_info=dt_info)
    finally:
        if ctoken: account_unlock(ct, ctoken)
        if dtoken: account_unlock(dt, dtoken)


def transaction_complete(transaction_id, completion_date=None, lock_token=None):
    """
    Args:
        transaction_id: transaction ID
        completion_date: completion date (default: now)
    """
    logging.info('Completing transaction {}'.format(transaction_id))
    if completion_date is None: completion_date = time.time()
    if config.keep_integrity:
        dt = None
        with lock_account_token:
            tinfo = transaction_info(transaction_id)
            dt = tinfo['dt']
            if dt:
                amount = tinfo['amount']
                acc_info = account_info(dt)
        token = account_lock(dt, lock_token)
    try:
        if config.keep_integrity and dt:
            if amount > 0 and acc_info['max_balance'] and account_balance(
                    dt) + amount > acc_info['max_balance']:
                raise OverlimitError
        if not get_db().execute(sql("""
        update transact set d=:d where id=:id"""),
                                d=completion_date,
                                id=transaction_id).rowcount:
            logging.error('Transaction {} not found'.format(transaction_id))
            raise ResourceNotFound
    finally:
        if config.keep_integrity and dt:
            account_unlock(dt, token)


def transaction_delete(transaction_id):
    """
    Delete (mark deleted) transaction
    """
    logging.warning('Deleting transaction {}'.format(transaction_id))
    tinfo = transaction_info(transaction_id)
    if not get_db().execute(sql("""
    update transact set
    deleted=:ts where id=:id or chain_transact_id=:id"""),
                            ts=time.time(),
                            id=transaction_id).rowcount:
        logging.error('Transaction {} not found'.format(transaction_id))
        raise ResourceNotFound
    chid = tinfo.get('chain_transact_id')
    if chid:
        transaction_delete(chid)


def transaction_purge(_lock=True):
    """
    Purge deleted transactions
    """
    if _lock:
        lock_purge.acquire()
    try:
        db = get_db()
        dbt = db.begin()
        logging.info('Purging deleted transactions')
        try:
            db.execute(
                sql("""delete from transact where
                    account_credit_id is null and account_debit_id is null""")
            ).rowcount
            result = db.execute(
                sql("""delete from transact where deleted is not null""")
            ).rowcount
            dbt.commit()
            return result
        except:
            dbt.rollback()
            raise
    finally:
        if _lock: lock_purge.release()


def account_statement(account, start=None, end=None, tag=None, pending=False):
    """
    Args:
        account: account code
        start: statement start date/time
        end: statement end date/time
        tag: filter transactions by tag
        pending: include pending transactions
    Returns:
        generator object
    """
    cond = 'transact.deleted is null and d_created != 0'
    d_field = 'd_created' if pending else 'd'
    if start:
        dts = parse_date(start)
        cond += (' and ' if cond else '') + 'transact.{} >= {}'.format(
            d_field, dts)
    if end:
        dte = parse_date(end)
        cond += (' and ' if cond else '') + 'transact.{} <= {}'.format(
            d_field, dte)
    if tag is not None:
        cond += (' and ' if cond else '') + 'tag = "{}"'.format(tag)
    r = get_db().execute(sql("""
    select transact.id, d_created, d,
            amount, tag, transact.note as note, account.code as cparty
        from transact left join account on
            account_credit_id=account.id where account_debit_id=
                (select id from account where code=:account) and {cond}
    union
    select transact.id, d_created, d,
            amount * -1, tag, transact.note as note, account.code as cparty
        from transact left join account on
            account_debit_id=account.id where account_credit_id=
                (select id from account where code=:account) and {cond}
        order by d_created, d
    """.format(cond=cond)),
                         account=account.upper())
    while True:
        d = r.fetchone()
        if not d: break
        row = OrderedDict()
        for i in ('id', 'amount', 'cparty', 'tag', 'note'):
            row[i] = getattr(d, i)
        row['created'] = format_date(d.d_created)
        row['completed'] = format_date(d.d)
        row['is_completed'] = d.d is not None
        yield row


def account_statement_summary(account,
                              start=None,
                              end=None,
                              tag=None,
                              pending=False):
    """
    Args:
        account: account code
        start: statement start date/time
        end: statement end date/time
        tag: filter transactions by tag
        pending: include pending transactions
    Returns:
        dict with fields:
            debit: debit turnover
            credit: credit turonver
            net: net debit
            statement: list of transactions
    """
    statement = list(
        account_statement(account=account.upper(),
                          start=start,
                          end=end,
                          tag=tag,
                          pending=pending))
    credit = 0
    debit = 0
    for row in statement:
        if row['amount'] > 0:
            if row['completed']:
                debit += row['amount']
        else:
            credit += row['amount']
    return {
        'credit': -1 * credit,
        'debit': debit,
        'net': debit + credit,
        'statement': statement
    }


def purge():
    """
    Purge deleted resources
    """
    logging.info('Purge requested')
    with lock_purge:
        result = {'transaction': transaction_purge(_lock=False)}
        return result


def account_list(currency=None,
                 tp=None,
                 code=None,
                 date=None,
                 order_by=['tp', 'currency', 'account', 'balance'],
                 hide_empty=False):
    """
    List accounts and their balances

    Args:
        currency: filter by currency
        tp: filter by account type (or types)
        code: filter by acocunt code (may contain '%' as a wildcards)
        date: get balances for the specified date
        order_by: list ordering
        hide_empty: hide accounts with zero balance, default is False
    """
    cond = "transact.deleted is null"
    if tp:
        if not isinstance(tp, list) and not isinstance(tp, tuple):
            if isinstance(tp, int):
                tp_id = tp
            else:
                tp_id = ACCOUNT_TYPE_IDS[tp]
            cond += (' and ' if cond else '') + 'account.tp = {}'.format(tp_id)
        else:
            cond += (' and (' if cond else '(')
            cor = ''
            for p in tp:
                if isinstance(p, int):
                    tp_id = p
                else:
                    tp_id = ACCOUNT_TYPE_IDS[p]
                cor = cor + (' or '
                             if cor else '') + 'account.tp = {}'.format(tp_id)
            cond += cor + ')'
    if currency:
        cond += (' and ' if cond else '') + 'currency.code = "{}"'.format(
            currency.upper())
    else:
        cond += (' and ' if cond else '') + 'account.tp < 1000'
    if date:
        dts = parse_date(date)
        cond += (' and '
                 if cond else '') + 'transact.d_created <= "{}"'.format(dts)
    if code:
        cond += (' and '
                 if cond else '') + 'account.code like "{}"'.format(code)
    oby = ''
    if order_by:
        if isinstance(order_by, list):
            oby = ','.join(order_by)
        else:
            oby = order_by
    r = get_db().execute(
        sql("""
            select sum(balance) as balance, account, note, currency, tp from 
                (
                select sum(amount) as balance,
                    account.code as account,
                    account.note as note,
                    currency.code as currency,
                    account.tp as tp
                    from transact
                    left join account on account.id=transact.account_debit_id
                    join currency on currency.id=account.currency_id
                    where d is not null and {cond_d}
                        group by account.code
                union
                select -1*sum(amount) as balance,
                    account.code as account,
                    account.note as note,
                    currency.code as currency,
                    account.tp as tp
                    from transact
                    left join account on account.id=transact.account_credit_id
                    join currency on currency.id=account.currency_id
                    where {cond}
                            group by account.code
                ) as templist
                    group by account, note, templist.currency, templist.tp
            {oby}
            """.format(cond=cond,
                       cond_d=cond.replace('_created', ''),
                       oby=('order by ' + oby) if oby else '')))
    while True:
        d = r.fetchone()
        if not d: break
        if hide_empty is False or d.balance:
            row = OrderedDict()
            for i in ('account', 'type', 'note', 'currency', 'balance'):
                if i == 'type':
                    row['type'] = ACCOUNT_TYPE_NAMES[d.tp]
                else:
                    row[i] = getattr(d, i)
            yield row


def account_list_summary(currency=None,
                         tp=None,
                         code=None,
                         date=None,
                         order_by=['tp', 'currency', 'account', 'balance'],
                         hide_empty=False,
                         base=None):
    """
    List accounts and their balances plus return a total sum

    Args:
        currency: filter by currency
        tp: filter by account type (or types)
        code: filter by acocunt code (may contain '%' as a wildcards)
        date: get balances for the specified date
        order_by: list ordering
        hide_empty: hide accounts with zero balance, default is False
        base: base currency (if not specified, config.base_currency is used)

    Returns:
        accounts: list of accounts
        total: total sum in base currency
    """
    if base is None:
        base = config.base_currency
    accounts = list(
        account_list(currency=currency,
                     tp=tp,
                     code=code,
                     date=date,
                     order_by=order_by,
                     hide_empty=hide_empty))
    for a in accounts:
        a['balance_bc'] = a['balance'] * currency_rate(
            a['currency'], base, date=date)
    return {
        'accounts':
            accounts,
        'total':
            sum(
                format_amount(d['balance'], d['currency']) if d['currency'] ==
                base else format_amount(
                    d['balance'] *
                    currency_rate(d['currency'], base, date=date), d['currency']
                ) for d in accounts)
    }


def account_credit(account=None,
                   currency=None,
                   date=None,
                   tp=None,
                   order_by=['tp', 'account', 'currency'],
                   hide_empty=False):
    """
    Get credit operations for the account

    Args:
        account: filter by account code
        currency: filter by currency code
        date: get balance for specified date/time
        tp: FIlter by account type
        sort: field or list of sorting fields
        hide_empty: don't return zero balances

    Returns:
        generator object
    """
    return _account_summary('credit',
                            account=account,
                            currency=currency,
                            date=date,
                            tp=tp,
                            order_by=order_by,
                            hide_empty=hide_empty)


def account_debit(account=None,
                  currency=None,
                  date=None,
                  tp=None,
                  order_by=['tp', 'account', 'currency'],
                  hide_empty=False):
    """
    Get debit operations for the account

    Args:
        account: filter by account code
        currency: filter by currency code
        date: get balance for specified date/time
        tp: FIlter by account type
        sort: field or list of sorting fields
        hide_empty: don't return zero balances

    Returns:
        generator object
    """
    return _account_summary('debit',
                            account=account,
                            currency=currency,
                            date=date,
                            tp=tp,
                            order_by=order_by,
                            hide_empty=hide_empty)


def _account_summary(balance_type,
                     account=None,
                     currency=None,
                     date=None,
                     tp=None,
                     order_by=['tp', 'account', 'currency'],
                     hide_empty=False):
    cond = 'where {} transact.deleted is null'.format(
        'transact.d is not null and ' if balance_type == 'debit' else '')
    if account:
        cond += (' and '
                 if cond else '') + 'account.code = "{}"'.format(account)
    if currency:
        cond += (' and '
                 if cond else '') + 'currency.code = "{}"'.format(currency)
    if date:
        dts = parse_date(date)
        cond += (' and ' if cond else '') + 'transact.d <= "{}"'.format(dts)
    if tp:
        if isinstance(tp, int):
            tp_id = tp
        else:
            tp_id = ACCOUNT_TYPE_IDS[tp]
        cond += (' and ' if cond else '') + 'account.tp = {}'.format(tp_id)
    oby = ''
    if order_by:
        if isinstance(order_by, list):
            oby = ','.join(order_by)
        else:
            oby = order_by
    r = get_db().execute(
        sql("""select sum(amount) as {btype}_balance, account.id as id,
    account.tp as tp,
    account.code as account, currency.code as currency
    from transact 
    join account on transact.account_{btype}_id = account.id
    join currency on account.currency_id = currency.id {cond}
    group by account.code, currency.code {oby}""".format(
            btype=balance_type,
            cond=cond,
            oby=('order by ' + oby) if oby else '')))
    while True:
        d = r.fetchone()
        if not d: break
        if hide_empty is False or d.balance:
            row = OrderedDict()
            for i in ('account', 'type', 'currency', balance_type + '_balance'):
                if i == 'type':
                    row['type'] = ACCOUNT_TYPE_NAMES[d.tp]
                else:
                    row[i] = getattr(d, i)
            yield row


def account_balance(account, date=None):
    """
    Get account balance

    Args:
        account: account code
        date: get balance for specified date/time
    """
    cond = "transact.deleted is null"
    if date:
        dts = parse_date(date)
        cond += (' and '
                 if cond else '') + 'transact.d_created <= "{}"'.format(dts)
    acc_info = account_info(account)
    r = get_db().execute(sql("""
        select debit-credit as balance from
            (select sum(amount) as debit from transact
                where account_debit_id=
                    (select id from account where code=:account)
                        and d is not null and {cond_d}) as f,
            (select sum(amount) as credit from transact
                where account_credit_id=
                    (select id from account where code=:account) and {cond})
                        as s
            """.format(cond=cond, cond_d=cond.replace('_created', ''))),
                         account=account.upper())
    d = r.fetchone()
    if not d or d.balance is None:
        raise ResourceNotFound
    return format_amount(d.balance, acc_info['currency'])


def account_balance_range(account,
                          start,
                          end=None,
                          step=1,
                          return_timestamp=True):
    """
    Get list of account balances for the specified range

    Args:
        account: account code
        start: start date/time, required
        end: end date/time, if not specified, current time is used
        step: list step in days
        return_timestamp: return dates as timestamps if True, otherwise as
            datetime objects. Default is True

    Returns:
        tuple with time series list and corresponding balance list
    """
    times = []
    data = []
    dt = parse_date(start, return_timestamp=False)
    end_date = parse_date(
        end, return_timestamp=False
    ) if end else datetime.datetime.now() + datetime.timedelta(days=1)
    delta = datetime.timedelta(days=step)
    while dt < end_date:
        times.append(dt.timestamp() if return_timestamp else dt)
        data.append(account_balance(account, date=dt))
        dt += delta
    return times, data
