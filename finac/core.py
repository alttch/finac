ACCOUNT_CREDIT = 0
ACCOUNT_CASH = 10
ACCOUNT_CURRENT = 100
ACCOUNT_SAVING = 300
ACCOUNT_TRANSIT = 400
ACCOUNT_ESCROW = 500
ACCOUNT_HOLDING = 510
ACCOUNT_VIRTUAL = 900
ACCOUNT_TEMP = 901
ACCOUNT_EXCHANGE = 1000

ACCOUNT_GS = 10000
ACCOUNT_SUPPLIER = 10001
ACCOUNT_CUSTOMER = 10002
ACCOUNT_FINAGENT = 10003

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
    ACCOUNT_FINAGENT: 'finagent'
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

logger = logging.getLogger('finac')

_db = SimpleNamespace(engine=None)

config = SimpleNamespace(db=None,
                         keep_integrity=True,
                         easy_exchange=True,
                         rate_allow_reverse=True,
                         colorize=True,
                         rate_allow_cross=True,
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


def parse_date(d):
    try:
        d = float(d)
        if d > 3000:
            return d
        else:
            d = int(d)
    except:
        pass
    return dateutil.parser.parse(str(d)).timestamp()


class ResourceNotFound(Exception):
    pass


class RateNotFound(Exception):
    pass


class OverdraftError(Exception):
    pass


class OverlimitError(Exception):
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
        l.db = _db.engine.connect()
        return l.db


def init(**kwargs):
    for k, v in kwargs.items():
        if not hasattr(config, k):
            raise RuntimeError('Parameter {} is invalid'.format(k))
        setattr(config, k, v)
    db_uri = config.db
    if db_uri.find(':///') == -1:
        db_uri = 'sqlite:///' + os.path.expanduser(db_uri)
    _db.engine = get_db_engine(db_uri)


def currency_create(currency):
    logger.info('Creating currency {}'.format(currency))
    get_db().execute(sql("""
    insert into currency(code) values(:code)"""),
                     code=currency)


def currency_delete(currency):
    logger.warning('Deleting currency {}'.format(currency))
    if not get_db().execute(sql("""
    delete from currency where code=:code"""),
                            code=currency).rowcount:
        logger.error('Currency {} not found'.format(currency))
        raise ResourceNotFound


def currency_set_rate(currency_from, currency_to=None, value=1, date=None):
    if date is None:
        date = time.time()
    else:
        date = parse_date(date)
    if currency_from.find('/') != -1 and currency_to is None:
        currency_from, currency_to = currency_from.split('/')
    logging.info('Setting rate for {}/{} to {} for {}'.format(
        currency_from, currency_to, value, format_date(date)))
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
                     f=currency_from,
                     t=currency_to,
                     d=date,
                     value=value)


def currency_delete_rate(currency_from, currency_to, date):
    if currency_from.find('/') != -1 and currency_to is None:
        currency_from, currency_to = currency_from.split('/')
    date = parse_date(date)
    logging.info('Deleting rate for {}/{} for {}'.format(
        currency_from, currency_to, format_date(date)))
    if not get_db().execute(sql("""
    delete from currency_rate where
        currency_from_id=(select id from currency where code=:f)
        and
        currency_to_id=(select id from currency where code=:t)
        and d=:d
        """),
                            f=currency_from,
                            t=currency_to,
                            d=date).rowcount:
        logger.error('Currency rate {}/{} for {} not found'.format(
            currency_from, currency_to, format_date(date)))
        raise ResourceNotFound


def currency_rate(currency_from, currency_to, date=None):
    if date is None:
        date = time.time()
    else:
        date = parse_date(date)
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
                       f=cf,
                       t=ct)
        return r.fetchone()

    d = _get_rate(currency_from, currency_to)
    if not d:
        if config.rate_allow_reverse:
            d = _get_rate(currency_to, currency_from)
            if not d:
                raise RateNotFound
            value = 1 / d.value
        else:
            raise RateNotFound
    else:
        value = d.value
    return value


def account_create(currency,
                   account,
                   name='',
                   tp='current',
                   max_overdraft=0,
                   max_balance=None):
    """
    Args:
        currency: currency code
        account: account code
        name: account name
        tp: account type (credit, current, saving, cash)
        max_overdraft: maximum allowed overdraft (set to negative to force
            account to have minimal positive balance)
        max_balance: max allowed account balance
    """
    if isinstance(tp, int):
        tp_id = tp
    else:
        tp_id = ACCOUNT_TYPE_IDS[tp]
    db = get_db()
    dbt = db.begin()
    logger.info('Creating account {}, currency: {}'.format(account, currency))
    try:
        r = db.execute(sql("""
        insert into account(code, name, tp, currency_id, max_overdraft,
        max_balance) values
        (:code, :name, :tp,
            (select id from currency where code=:currency),
            :max_overdraft, :max_balance)"""),
                       code=account,
                       name=name,
                       tp=tp_id,
                       currency=currency,
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
    except:
        logger.error('Unable to create account {}'.format(account))
        dbt.rollback()
        raise


def account_info(account):
    r = get_db().execute(sql("""
            select account.code as account_code, account.name, account.tp,
            currency.code as currency, max_overdraft, max_balance
            from account join
            currency on account.currency_id = currency.id
            where account.code = :account"""),
                         account=account)
    d = r.fetchone()
    if not d: raise ResourceNotFound
    return {
        'code': d.account_code,
        'name': d.name,
        'type': ACCOUNT_TYPE_NAMES[d.tp],
        'tp': d.tp,
        'currency': d.currency,
        'max_overdraft': d.max_overdraft,
        'max_balance': d.max_balance
    }


def transaction_info(transaction_id):
    r = get_db().execute(sql("""
            select transact.amount as amount, transact.tag as tag,
            transact.description as description,
            transact.d_created as d_created,
            transact.d as d,
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
        'description': d.description,
        'created': d.d_created,
        'completed': d.d,
        'dt': d.debit if hasattr(d, 'debit') else None,
        'ct': d.credit if hasattr(d, 'credit') else None,
    }


def account_delete(account):
    logger.warning('Deleting account {}'.format(account))
    if not get_db().execute(sql("""
    delete from account where code=:code"""),
                            code=account).rowcount:
        logger.error('Account {} not found'.format(account))
        raise ResourceNotFound
    get_db().execute(sql("""
    delete from transact where
    account_debit_id=:code or account_credit_id=:code and d=0"""),
                     code=account)


def account_lock(account, token):
    if config.keep_integrity:
        with lock_account_token:
            if account in account_lockers:
                l = account_lockers[account]
            else:
                l = AccountLocker()
                account_lockers[account] = l
        return l.acquire(token)


def account_unlock(account, token):
    if config.keep_integrity:
        with lock_account_token:
            l = account_lockers.get(account)
        if not l: raise ResourceNotFound
        return l.release(token)


def transaction_create(account,
                       amount=None,
                       tag=None,
                       description='',
                       creation_date=None,
                       completion_date=None,
                       mark_completed=True,
                       target=None,
                       lock_token=None):
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
                                    description=description,
                                    creation_date=creation_date,
                                    completion_date=completion_date,
                                    mark_completed=mark_completed,
                                    target_ct=target,
                                    credit_lock_token=token)
        else:
            return transaction_move(dt=account,
                                    amount=amount,
                                    tag=tag,
                                    description=description,
                                    creation_date=creation_date,
                                    completion_date=completion_date,
                                    mark_completed=mark_completed,
                                    target_dt=None,
                                    debit_lock_token=token)
    finally:
        account_unlock(account, token)


def transaction_move(dt=None,
                     ct=None,
                     amount=0,
                     tag=None,
                     description='',
                     creation_date=None,
                     completion_date=None,
                     mark_completed=True,
                     target_ct=None,
                     target_dt=None,
                     credit_lock_token=None,
                     debit_lock_token=None):
    """
    Args:
        ct: source (credit) account code
        dt: target (debit) account code
        amount: transaction amount (always >0)
        tag: transaction tag
        descrption: transaction description
        creation_date: transaction creation daate (default: now)
        completion_date: transaction completion date (default: now)
        mark_completed: mark transaction completed (set completion date)
        target_ct: target credit account balance
        target_dt: target debit account balance

    Returns:
        transaction id
    """
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
    try:
        ctoken = account_lock(ct, credit_lock_token) if ct else None
        dtoken = account_lock(dt, debit_lock_token) if dt else None
        if target_dt:
            amount = target_dt - account_balance(dt)
        elif target_ct:
            amount = account_balance(ct) - target_ct
        if amount == 0: return
        if amount is not None and amount < 0:
            raise ValueError('Amount should be greater than zero')
        if config.keep_integrity:
            ccur = None
            dcur = None
            if ct:
                acc_info = account_info(ct)
                ccur = acc_info['currency']
                ctp = acc_info['tp']
                m = acc_info['max_overdraft']
                if m is not None and account_balance(ct) - amount < -1 * m:
                    raise OverdraftError
            if dt:
                acc_info = account_info(dt)
                dcur = acc_info['currency']
                dtp = acc_info['tp']
                m = acc_info['max_balance']
                if m is not None \
                        and account_balance(dt) + amount > m:
                    raise OverlimitError
            if ct and dt and ccur != dcur:
                raise ValueError('Currency mismatch')
        if creation_date is None:
            creation_date = time.time()
        else:
            creation_date = parse_date(creation_date)
        if completion_date is None:
            if mark_completed:
                completion_date = creation_date
        else:
            completion_date = parse_date(completion_date)
        return db.execute(sql("""
        insert into transact(account_credit_id, account_debit_id, amount, tag,
        description, d_created, d) values
        (
        (select id from account where code=:ct),
        (select id from account where code=:dt),
        :amount, :tag, :description, :d_created, :d)
        """),
                          ct=ct,
                          dt=dt,
                          amount=amount,
                          tag=tag,
                          description=description,
                          d_created=creation_date,
                          d=completion_date).lastrowid
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
    db = get_db()
    dbt = db.begin()
    logging.warning('Deleting transaction {}'.format(transaction_id))
    try:
        if not get_db().execute(sql("""
        update transact set
        deleted=true where id=:id or chain_transact_id=:id"""),
                                id=transaction_id).rowcount:
            logging.error('Transaction {} not found'.format(transaction_id))
            raise ResourceNotFound
        dbt.commit()
    except:
        dbt.rollback()
        raise


def transaction_purge(_lock=True):
    if _lock:
        lock_purge.acquire()
    try:
        db = get_db()
        dbt = db.begin()
        logging.info('Purging deleted transactions')
        try:
            db.execute(
                sql("""delete from transact where
                    account_credit_id == null or account_debit_id == null""")
            ).rowcount
            result = db.execute(
                sql("""delete from transact where deleted = true""")).rowcount
            dbt.commit()
            return result
        except:
            dbt.rollback()
            raise
    finally:
        if _lock: lock_purge.release()


def account_statement(account,
                      start=None,
                      end=None,
                      pending=False,
                      lock_token=None):
    """
    Args:
        account: account code
        start: statement start date/time
        end: statement end date/time
        pending: include pending transactions
    Returns:
        generator object
    """
    cond = 'transact.deleted == false and d_created != 0'
    d_field = 'd_created' if pending else 'd'
    if start:
        dts = parse_date(start)
        cond += (' and ' if cond else '') + 'transact.{} >= {}'.format(
            d_field, dts)
    if end:
        dte = parse_date(end)
        cond += (' and ' if cond else '') + 'transact.{} <= {}'.format(
            d_field, dte)
    token = account_lock(account, lock_token)
    try:
        r = get_db().execute(sql("""
        select transact.id, d_created, d,
                amount, tag, description, account.code as cparty
            from transact left join account on
                account_credit_id=account.id where account_debit_id=
                    (select id from account where code=:account) and {cond}
        union
        select transact.id, d_created, d,
                amount * -1, tag, description, account.code as cparty
            from transact left join account on
                account_debit_id=account.id where account_credit_id=
                    (select id from account where code=:account) and {cond}
            order by d, d_created
        """.format(cond=cond)),
                             account=account)
    finally:
        account_unlock(account, token)
    while True:
        d = r.fetchone()
        if not d: break
        row = OrderedDict()
        for i in ('id', 'amount', 'cparty', 'tag', 'description'):
            row[i] = getattr(d, i)
        row['created'] = format_date(d.d_created)
        row['completed'] = format_date(d.d)
        row['is_completed'] = d.d is not None
        yield row


def account_statement_summary(account, start=None, end=None, pending=False):
    """
    Args:
        account: account code
        start: statement start date/time
        end: statement end date/time
        pending: include pending transactions
    Returns:
        dict with fields:
            debit: debit turnover
            credit: credit turonver
            net: net debit
            statement: list of transactions
    """
    statement = list(
        account_statement(account=account,
                          start=start,
                          end=end,
                          pending=pending))
    credit = 0
    debit = 0
    for row in statement:
        if row['amount'] > 0:
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
    logging.info('Purge requested')
    with lock_purge:
        result = {'transaction': transaction_purge(_lock=False)}
        return result


def account_credit(account=None,
                   currency=None,
                   date=None,
                   tp=None,
                   order_by=['tp', 'account', 'currency'],
                   hide_empty=False):
    """
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
    cond = 'where {} transact.deleted == false'.format(
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
            tp_id = ACCOUNT_TYPE_NAMES.index(tp)
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
    from transact join currency on account.currency_id = currency.id join
    account on transact.account_{btype}_id = account.id {cond}
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
    Args:
        account: filter by account code
        date: get balance for specified date/time
    """
    rc = list(account_credit(account=account, date=date, hide_empty=False))
    rd = list(account_debit(account=account, date=date, hide_empty=False))
    if not rd or not rc:
        raise ResourceNotFound
    return rd[0]['debit_balance'] - rc[0]['credit_balance']
