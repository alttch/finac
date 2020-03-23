__author__ = 'Altertech, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2019 Altertech'
__license__ = 'MIT'

__version__ = '0.4.18'

from sqlalchemy.exc import IntegrityError
from cachetools import TTLCache
from itertools import groupby
from .currencies import currencies
from types import SimpleNamespace

_cache = SimpleNamespace(rate=None, rate_list=None)

_CacheRateKeyError = KeyError
_CacheRateListKeyError = KeyError

_asset_precision_cache = {}

restrict_assets_to_currencies = False

# financial assets
ACCOUNT_CREDIT = 0
ACCOUNT_CASH = 1
ACCOUNT_CURRENT = 2
ACCOUNT_SAVING = 100

#
ACCOUNT_GS = 200
ACCOUNT_SUPPLIER = 201
ACCOUNT_CUSTOMER = 202
ACCOUNT_FINAGENT = 203

ACCOUNT_HOLDING = 300

# investment instruments

ACCOUNT_STOCK = 400
ACCOUNT_BOND = 401
ACCOUNT_FUND = 402
ACCOUNT_METAL = 403

ACCOUNT_REALITY = 500

# taxes
ACCOUNT_TAX = 1000

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
    ACCOUNT_STOCK: 'stock',
    ACCOUNT_BOND: 'bond',
    ACCOUNT_FUND: 'fund',
    ACCOUNT_METAL: 'metal',
    ACCOUNT_REALITY: 'reality',
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
    ACCOUNT_TAX: 'tax'
}

PASSIVE_ACCOUNTS = [ACCOUNT_SUPPLIER, ACCOUNT_FINAGENT, ACCOUNT_TAX]

ACCOUNT_TYPE_IDS = {v: k for k, v in ACCOUNT_TYPE_NAMES.items()}

LOCK_DELAY = 0.1

import sqlalchemy as sa
import datetime
import time
import string
import random
import os
import logging
import threading

from sqlalchemy import text as sql

g = threading.local()

from types import SimpleNamespace
from collections import OrderedDict
from functools import wraps

from pyaltt2.crypto import gen_random_str
from pyaltt2.converters import val_to_boolean, parse_date, parse_number

import threading

from .db_set import init_db

logger = logging.getLogger('finac')

_db = SimpleNamespace(engine=None, redis_conn=None)

config = SimpleNamespace(db=None,
                         db_pool_size=10,
                         keep_integrity=True,
                         lazy_exchange=True,
                         full_transaction_update=True,
                         rate_allow_reverse=True,
                         rate_allow_cross=True,
                         base_asset='USD',
                         api_uri=None,
                         api_key=None,
                         api_timeout=5,
                         multiplier=None,
                         redis_host=None,
                         redis_port=6379,
                         redis_db=0,
                         redis_timeout=5,
                         redis_blocking_timeout=5,
                         restrict_deletion=None,
                         date_format='%Y-%m-%d %H:%M:%S %Z',
                         rate_cache_ttl=None,
                         insecure=False)

lock_purge = threading.Lock()
lock_account_token = threading.Lock()

account_lockers = {}

multiply_fields = {
    'asset_rate': ['value'],
    'account': ['max_overdraft', 'max_balance'],
    'transact': ['amount']
}


def _format_ttlcache_key(d, ttl):
    t = d.timestamp()
    return t if t < time.time() - ttl else None


def _multiply(i):
    return round(i * config.multiplier) if config.multiplier and i else i


def _demultiply(i):
    if i is not None:
        return (int(i) / config.multiplier) if config.multiplier else float(i)
    else:
        return None


def core_method(f):
    import inspect
    argspec = inspect.getfullargspec(f)

    @wraps(f)
    def do(*args, **kwargs):
        if config.api_uri is None:
            return f(*args, **kwargs)
        else:
            import requests
            import uuid
            import json
            req_id = str(uuid.uuid4())
            payload = {
                'jsonrpc': '2.0',
                'method': f.__name__,
                'params': kwargs,
                'id': req_id
            }
            if config.api_key is not None:
                payload['params']['_k'] = config.api_key
            for i, a in enumerate(args):
                payload['params'][argspec.args[i]] = a
            logger.debug('API request {} {} {}'.format(req_id,
                                                       payload['method'],
                                                       payload['params']))
            r = requests.post(config.api_uri,
                              json=payload,
                              timeout=config.api_timeout)
            if not r.ok:
                raise RuntimeError('Finac server error: {}'.format(
                    r.status_code))
            result = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(
                r.text)
            if 'error' in result:
                raise _exceptions.get(result['error']['code'], RuntimeError)(
                    result['error'].get('message'))
            logging.debug('API response {} {}'.format(result['id'],
                                                      result['result']))
            return result['result']

    return do


def deletion_method(f):

    @wraps(f)
    def do(*args, **kwargs):
        if config.restrict_deletion == 2:
            raise RuntimeError('deletion methods forbidden by server config')
        else:
            return f(*args, **kwargs)

    return do


def format_date(d, force=False):
    if d is not None:
        if config.date_format is None:
            return d
        elif isinstance(d, int):
            return datetime.datetime.strftime(
                datetime.datetime.fromtimestamp(d), config.date_format)
        elif isinstance(d, datetime.datetime) and not force:
            return d
        else:
            return datetime.datetime.strftime(
                parse_date(d, return_timestamp=False), config.date_format)


def preload():
    """
    Preload static data
    """
    for a in _asset_precision():
        _asset_precision_cache[a['asset']] = a['precision']


def asset_precision(asset):
    """
    Get precision (digits after comma) for the asset
    Note: asset precision is cached, so process restart required if changed
    """
    return _asset_precision_cache[
        asset] if asset in _asset_precision_cache else _asset_precision(
            asset=asset)


@core_method
def get_version():
    return __version__


@core_method
def _asset_precision(asset=None):
    if asset:
        if asset in _asset_precision_cache:
            return _asset_precision_cache[asset]
        d = get_db().execute(sql('select precs from asset where code=:code'),
                             code=asset.upper()).fetchone()
        if not d:
            raise ResourceNotFound
        precs = int(d.precs)
        _asset_precision_cache[asset] = precs
        return precs
    else:

        def all_precs():
            d = get_db().execute(
                sql('select code, precs from asset order by code'))
            while True:
                r = d.fetchone()
                if not r: break
                row = OrderedDict()
                row['asset'] = r.code
                row['precision'] = int(r.precs)
                yield row

        return all_precs()


def format_amount(i, asset, passive=False):
    """
    Format amount for values and exchange operations. Default: apply asset
    precision
    """
    return round(i, asset_precision(asset)) * (-1 if passive else 1)


class ResourceNotFound(Exception):
    """
    Raised when accessed resource is not found

    JRPC code: -32001
    """
    pass


class RateNotFound(Exception):
    """
    Raised when accessed asset rate is not found

    JRPC code: -32002
    """
    pass


class OverdraftError(Exception):
    """
    Raised when transaction is trying to break account max overdraft

    JRPC code: -32003
    """
    pass


class OverlimitError(Exception):
    """
    Raised when transaction is trying to break account max balance

    JRPC code: -32004
    """
    pass


class ResourceAlreadyExists(Exception):
    """
    Raised when trying to create already existing resource

    JRPC code: -32005
    """
    pass


_exceptions = {
    -32001: ResourceNotFound,
    -32002: RateNotFound,
    -32003: OverdraftError,
    -32004: OverlimitError,
    -32005: ResourceAlreadyExists,
    -32601: AttributeError,
    -32602: TypeError,
    -32603: ValueError
}


class AccountLocker:

    def __init__(self):
        self.token = None
        self.counter = 0
        self._lock = threading.Lock()

    def acquire(self, token=None, account=None):
        if token:
            with self._lock:
                if token == self.token:
                    self.counter += 1
                    return token
        if _db.redis_conn:
            with self._lock:
                self.token = _db.redis_conn.lock(
                    account,
                    blocking_timeout=config.redis_blocking_timeout,
                    thread_local=False)
                if not self.token.acquire():
                    raise RuntimeError('Unable to acquire account lock')
                self.counter = 1
                return self.token
        else:
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
                if _db.redis_conn:
                    self.token.release()
                self.token = None


class ForeignKeysListener(sa.interfaces.PoolListener):

    def connect(self, dbapi_con, con_record):
        try:
            dbapi_con.execute('pragma foreign_keys=ON')
        except:
            pass


def get_db_engine(db_uri):
    if db_uri.startswith('sqlite:///'):
        return sa.create_engine(db_uri, listeners=[ForeignKeysListener()])
    else:
        return sa.create_engine(db_uri,
                                pool_size=config.db_pool_size,
                                max_overflow=config.db_pool_size * 2)


def get_db():
    try:
        g.db.execute('select 1')
        return g.db
    except AttributeError:
        pass
    except:
        try:
            g.db.close()
        except:
            pass
    if not _db.engine:
        raise RuntimeError('finac not initialized')
    g.db = _db.engine.connect()
    return g.db


def init(db=None, **kwargs):
    """
    Initialize finac database and configuration

    Args:
        db: SQLAlchemy DB URI or sqlite file name
        db_pool_size: DB pool size (default: 10)
        keep_integrity: finac should keep database integrity (lock accounts,
            watch overdrafts, overlimits etc. Default is True
        lazy_exchange: allow direct exchange operations betwen accounts.
            Default: True
        rate_allow_reverse: allow reverse rates for lazy exchange (e.g. if
            "EUR/USD" pair exists but no USD/EUR, use 1 / "EUR/USD"
        rate_allow_cross: if exchange rate is not found, allow finac to look
            for the nearest cross-asset rate
        rate_cache_size: set rate cache size (default: 1024)
        rate_cache_ttl: set rate cache ttl (default: 5 sec)
        full_transaction_update: allow updating transaction date and amount
        base_asset: default base asset. Default is "USD"
        date_format: default date format in statements
        multiplier: use data multiplier
        restrict_deletion: 1 - forbid purge, 2 - forbid delete functions
        redis_host: Redis host
        redis_port: Redis port (default: 6379)
        redis_db: Redis database (default: 0)
        redis_timeout: Redis server timeout
        redis_blocking_timeout: Redis lock acquisition timeout

    Note: if Redis server is specified, Finac will use it for integrity locking
          (if enabled). In this case, lock tokens become Redis lock objects.
    """
    rate_cache_ttl = 5
    rate_cache_size = 1024
    for k, v in kwargs.items():
        if k == 'rate_cache_ttl':
            rate_cache_ttl = v
        elif k == 'rate_cache_size':
            rate_cache_size = v
        elif not hasattr(config, k):
            raise RuntimeError('Parameter {} is invalid'.format(k))
        setattr(config, k, v)
    _cache.rate = TTLCache(maxsize=rate_cache_size, ttl=rate_cache_ttl)
    _cache.rate_list = TTLCache(maxsize=rate_cache_size, ttl=rate_cache_ttl)
    config.rate_cache_ttl = rate_cache_ttl
    if config.multiplier:
        config.multiplier = float(config.multiplier)
    if db is not None:
        config.db = db
        db_uri = db
        if db_uri.find('://') == -1:
            db_uri = 'sqlite:///' + os.path.expanduser(db_uri)
        _db.engine = get_db_engine(db_uri)
        _db.use_lastrowid = db_uri.startswith('sqlite') or db_uri.startswith(
            'mysql')
        init_db(_db.engine)
    if config.redis_host is not None:
        import redis
        _db.redis_conn = redis.Redis(host=config.redis_host,
                                     port=config.redis_port,
                                     db=config.redis_db,
                                     socket_timeout=config.redis_timeout)


@core_method
def config_set(prop, value):
    """
    Set configuration property on-the-fly (config.insecure = True required)

    Recommended to use for testing only
        
    Args:
        prop: property name
        value: property value
    """
    if not config.insecure:
        raise RuntimeError('access denied')
    if not hasattr(config, prop):
        raise RuntimeError('property {} is invalid'.format(prop))
    setattr(config, prop, value)


@core_method
def asset_create(asset, precision=2):
    """
    Create asset

    Args:
        asset: asset code (e.g. "CAD", "AUD")
        precision: precision (digits after comma) for statements and exchange
            operations. Default is 2 digits
    """
    asset = asset.upper()
    if restrict_assets_to_currencies:
        if asset not in [c['cc'] for c in currencies]:
            logger.error('Currency {} not found'.format(asset))
            raise ResourceNotFound
    logger.info('Creating asset {}'.format(asset))
    try:
        get_db().execute(sql("""
        insert into asset(code, precs) values(:code, :precision)"""),
                         code=asset,
                         precision=precision)
    except IntegrityError:
        raise ResourceAlreadyExists(asset)


@core_method
def asset_list():
    """
    List assets
    """
    r = get_db().execute(
        sql("""
        select code, precs from asset order by code"""))
    while True:
        d = r.fetchone()
        if not d: break
        row = OrderedDict()
        row['asset'] = d.code
        row['precision'] = d.precs
        yield row


@core_method
def asset_list_rates(asset=None, start=None, end=None, datefmt=False):
    """
    List asset rates

    Asset can be specified either as code, or as pair "code/code"

    If asset is not specified, "end" is used as date to get rates for all
    assets
    """
    if asset:
        cond = ''
        asset = _safe_format(asset.upper())
        if start:
            dts = parse_date(start, return_timestamp=False)
            cond += (' and ' if cond else '') + 'd >= \'{}\''.format(dts)
        dte = parse_date(end, return_timestamp=False) if end else parse_date(
            return_timestamp=False)
        cond += (' and ' if cond else '') + 'd <= \'{}\''.format(dte)
        if asset.find('/') != -1:
            asset_from, asset_to = _safe_format(asset.split('/'))
            cond += (' and ' if cond else
                     '') + 'cf.code = \'{}\' and ct.code = \'{}\''.format(
                         asset_from, asset_to)
        else:
            cond += (' and ' if cond else ''
                    ) + '(cf.code = \'{code}\' or ct.code = \'{code}\')'.format(
                        code=asset)
        r = get_db().execute(
            sql("""
            select cf.code as asset_from,
                    ct.code as asset_to,
                    d, value
            from asset_rate
                join asset as cf on asset_from_id = cf.id
                join asset as ct on asset_to_id = ct.id
                    where {cond} order by d
        """.format(cond=cond)))
    else:
        d = parse_date(end, return_timestamp=False) if end else parse_date(
            return_timestamp=False)
        r = get_db().execute(sql("""
            select
                a1.code as asset_from,
                a2.code as asset_to,
                ar.value as value, m as d
            from
                (select
                    as1.asset_from_id as fr,
                    as1.asset_to_id as t,
                    max(as1.d) as m
                from asset_rate as as1
                where as1.d<=:d group by fr, t)
            as s1
                join asset as a1 on a1.id=fr
                join asset as a2 on a2.id=t
                join asset_rate as ar on ar.asset_from_id=fr
                    and ar.asset_to_id=t and ar.d=m
                order by asset_from, asset_to
                    """),
                             d=d)
    while True:
        d = r.fetchone()
        if not d: break
        row = OrderedDict()
        row['asset_from'] = d.asset_from
        row['asset_to'] = d.asset_to
        row['date'] = format_date(d.d, force=datefmt)
        row['value'] = _demultiply(d.value)
        yield row


@deletion_method
@core_method
def asset_delete(asset):
    """
    Delete asset

    Warning: all accounts linked to this asset will be deleted as well
    """
    logger.warning('Deleting asset {}'.format(asset.upper()))
    if not get_db().execute(sql("""
    delete from asset where code=:code"""),
                            code=asset.upper()).rowcount:
        logger.error('Asset {} not found'.format(asset.upper()))
        raise ResourceNotFound


@core_method
def asset_set_rate(asset_from, asset_to=None, value=None, date=None):
    """
    Set asset rate

    Args:
        asset_from: asset from code
        asset_to: asset to code
        value: exchange rate value
        date: date/time exchange rate is set on (default: now)

    Function can be also called as e.g. asset_set_rate('EUR/USD', value=1.1)
    """
    if (isinstance(asset_to, float) or
            isinstance(asset_to, int)) and value is None:
        value = asset_to
        asset_to = None
    if value is None:
        raise ValueError('Asset rate value is not specified')
    else:
        value = parse_number(value)
    if date is None:
        date = parse_date(return_timestamp=False)
    else:
        date = parse_date(date, return_timestamp=False)
    if asset_from.find('/') != -1 and asset_to is None:
        asset_from, asset_to = asset_from.split('/')
    logging.info('Setting rate for {}/{} to {} for {}'.format(
        asset_from.upper(), asset_to.upper(), value, format_date(date)))
    get_db().execute(sql("""
    insert into asset_rate (asset_from_id, asset_to_id, d, value)
    values
    (
        (select id from asset where code=:f),
        (select id from asset where code=:t),
        :d,
        :value
    )
    """),
                     f=asset_from.upper(),
                     t=asset_to.upper(),
                     d=date,
                     value=_multiply(value))


@deletion_method
@core_method
def asset_delete_rate(asset_from, asset_to=None, date=None):
    """
    Delete currrency rate
    """
    if not date:
        raise ValueError('asset rate date not specified')
    if asset_from.find('/') != -1 and asset_to is None:
        asset_from, asset_to = asset_from.split('/')
    date = parse_date(date, return_timestamp=False)
    logging.info('Deleting rate for {}/{} for {}'.format(
        asset_from.upper(), asset_to.upper(), format_date(date)))
    if not get_db().execute(sql("""
    delete from asset_rate where
        asset_from_id=(select id from asset where code=:f)
        and
        asset_to_id=(select id from asset where code=:t)
        and d=:d
        """),
                            f=asset_from.upper(),
                            t=asset_to.upper(),
                            d=date).rowcount:
        logger.error('Asset rate {}/{} for {} not found'.format(
            asset_from.upper(), asset_to.upper(), format_date(date)))
        raise ResourceNotFound


@core_method
def asset_rate(asset_from, asset_to=None, date=None):
    """
    Get asset rate for the specified date

    If no date is specified, get asset rate for now

    Function can be also called as e.g. asset_rate('EUR/USD')
    """
    if date is None:
        date = parse_date(return_timestamp=False)
    else:
        date = parse_date(date, return_timestamp=False)
    if asset_from.find('/') != -1 and asset_to is None:
        asset_from, asset_to = asset_from.split('/')
    asset_from = asset_from.upper()
    asset_to = asset_to.upper()
    if asset_from == asset_to:
        return 1
    db = get_db()

    def _get_rate(cf, ct, d):
        key = _format_ttlcache_key(d, config.rate_cache_ttl)
        try:
            return _cache.rate[(cf, ct, key)]
        except _CacheRateKeyError:
            r = db.execute(sql("""
                select value from asset_rate
                    join asset as cfrom on asset_from_id=cfrom.id
                    join asset as cto on asset_to_id=cto.id
                where d <= :d and cfrom.code = :f and cto.code = :t
                order by d desc limit 1
                """),
                           d=date,
                           f=cf,
                           t=ct)
            d = r.fetchone()
            if d:
                value = _demultiply(d.value)
                _cache.rate[(cf, ct, key)] = value
                return value
            else:
                return None

    def _get_crossrate(asset_from, asset_to, d):

        def _find_path(graph, start, end, path=[]):
            path = path + [start]
            if start == end:
                return [path]
            paths = []
            for node in graph[start]:
                if node not in path:
                    paths += _find_path(graph, node, end, path)
            return paths

        graph = {}
        rates = {}
        key = _format_ttlcache_key(d, config.rate_cache_ttl)
        try:
            ratelist = _cache.rate_list[key]
        except _CacheRateListKeyError:
            ratelist = list(asset_list_rates(end=d))
            _cache.rate_list[key] = ratelist
        for r in ratelist:
            rates[(r['asset_from'], r['asset_to'])] = r['value']
            graph.setdefault(r['asset_from'], []).append(r['asset_to'])
        for k, v in rates.copy().items():
            if (k[1], k[0]) not in rates:
                rates[(k[1], k[0])] = 1 / v
                graph.setdefault(k[1], []).append(k[0])
        try:
            path = min(_find_path(graph, asset_from, asset_to), key=len)
        except KeyError:
            return None
        if not path:
            return None
        rate = 1
        for i in range(0, len(path) - 1):
            rate *= rates[path[i], path[i + 1]]
        return rate

    value = _get_rate(asset_from, asset_to, date)
    if not value:
        if config.rate_allow_reverse is True:
            value = _get_rate(asset_to, asset_from, date)
            if not value:
                if config.rate_allow_cross:
                    value = _get_crossrate(asset_from, asset_to, date)
                    if value: return value
                raise RateNotFound('{}/{} for {} (base asset: {})'.format(
                    asset_from, asset_to, format_date(date), config.base_asset))
            value = 1 / value
        else:
            raise RateNotFound
    return value


@core_method
def account_create(account,
                   asset,
                   tp='current',
                   note=None,
                   passive=None,
                   max_overdraft=None,
                   max_balance=None):
    """
    Args:
        asset: asset code
        account: account code
        note: account notes
        passive: if True, account is considered as passive
        tp: account type (credit, current, saving, cash etc.)
        max_overdraft: maximum allowed overdraft (set to negative to force
            account to have minimal positive balance), default is None
            (unlimited)
        max_balance: max allowed account balance, default is None (unlimited)

    Accounts of type 'tax', 'supplier' and 'finagent' are passive by default
    """
    if isinstance(tp, int):
        tp_id = tp
    else:
        tp_id = ACCOUNT_TYPE_IDS[tp]
    if passive is None and tp_id in PASSIVE_ACCOUNTS:
        passive = True
    else:
        passive = val_to_boolean(passive)
    db = get_db()
    account = account.upper()
    asset = asset.upper()
    if not db.execute(sql("""select id from asset where code=:code"""),
                      code=asset).fetchone():
        raise ResourceNotFound('asset {}'.format(asset))
    dbt = db.begin()
    logger.info('Creating account {}, asset: {}'.format(account, asset))
    try:
        r = db.execute(sql("""
        insert into account(code, note, tp, passive, asset_id, max_overdraft,
        max_balance) values
        (:code, :note, :tp, :passive,
            (select id from asset where code=:asset),
            :max_overdraft, :max_balance) {}""".format(
            '' if _db.use_lastrowid else 'returning id')),
                       code=account,
                       note=note,
                       tp=tp_id,
                       passive=passive,
                       asset=asset,
                       max_overdraft=_multiply(max_overdraft),
                       max_balance=_multiply(max_balance))
        acc_id = r.lastrowid if _db.use_lastrowid else r.fetchone().id
        db.execute(sql("""
            INSERT INTO transact
                (account_debit_id, amount, d_created, d, service)
            VALUES
                (:account_id, 0, :d, :d, :s)
            """),
                   account_id=acc_id,
                   d=datetime.datetime.fromtimestamp(0),
                   s=True)
        db.execute(sql("""
            INSERT INTO transact
                (account_credit_id, amount, d_created, d, service)
            VALUES
                (:account_id, 0, :d, :d, :s)
            """),
                   account_id=acc_id,
                   d=datetime.datetime.fromtimestamp(0),
                   s=True)
        dbt.commit()
    except IntegrityError:
        dbt.rollback()
        raise ResourceAlreadyExists(account)
    except:
        logger.error('Unable to create account {}'.format(account))
        dbt.rollback()
        raise


@core_method
def account_info(account):
    """
    Get dict with account info
    """
    r = get_db().execute(sql("""
            select account.code as account_code, account.note, account.tp,
            account.passive, asset.code as asset, max_overdraft, max_balance
            from account join
            asset on account.asset_id = asset.id
            where account.code = :account"""),
                         account=account.upper())
    d = r.fetchone()
    if not d: raise ResourceNotFound
    return {
        'code': d.account_code,
        'note': d.note,
        'type': ACCOUNT_TYPE_NAMES[d.tp],
        'tp': d.tp,
        'passive': d.passive,
        'asset': d.asset,
        'max_overdraft': _demultiply(d.max_overdraft),
        'max_balance': _demultiply(d.max_balance)
    }


@core_method
def transaction_info(transaction_id):
    """
    Get dict with transaction info
    """
    r = get_db().execute(sql("""
            select transact.amount as amount, transact.tag as tag,
            transact.note as note,
            transact.d_created as d_created,
            transact.d as d,
            transact.service as service,
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
        'amount': _demultiply(d.amount),
        'tag': d.tag,
        'note': d.note,
        'created': d.d_created,
        'completed': d.d,
        'chain_transact_id': d.chain_transact_id,
        'service': d.service,
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

    Returns:
        list of transaction IDs
    """
    import yaml
    try:
        yaml.warnings({'YAMLLoadWarning': False})
    except:
        pass
    result = []
    with open(fname) as fh:
        transactions = yaml.load(fh)['transactions']
    for t in transactions:
        if 'account' in t:
            result.append(transaction_create(**t))
        else:
            result.append(transaction_move(**t))
    return result


@deletion_method
@core_method
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
        account_credit_id=(select id from account where code=:code) and d=:d"""
                                   ),
                                code=account,
                                d=datetime.datetime.fromtimestamp(0)).rowcount:
            raise ResourceNotFound
        if not get_db().execute(sql("""
        delete from account where code=:code"""),
                                code=account).rowcount:
            logger.error('Account {} not found'.format(account))
            raise ResourceNotFound
    finally:
        account_unlock(account, token)


@core_method
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
    account = account.upper()
    if config.keep_integrity:
        with lock_account_token:
            if account in account_lockers:
                l = account_lockers[account]
            else:
                l = AccountLocker()
                account_lockers[account] = l
        return l.acquire(token, account)


@core_method
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
        if not get_db().execute(
                sql("""
        update {tbl} set {f} = :val where {objidf} = :id
        """.format(tbl=tbl, f=k, objidf=objidf)),
                val=_multiply(v) if k in multiply_fields.get(tbl, []) else v,
                id=c).rowcount:
            raise ResourceNotFound('{} {}'.format(tbl, objid))
        if k == 'code':
            c = v


@core_method
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


@core_method
def asset_update(asset, **kwargs):
    """
    Update asset parameters

    Parameters, allowed to be updated:
        code, precision

    Note that asset precision is cached and requires process restart if
    changed
    """
    _ckw(kwargs, ['code', 'precision'])
    kw = kwargs.copy()
    if 'precision' in kw:
        kw['precs'] = kw['precision']
        del kw['precision']
    _update(asset, 'asset', 'code', kw)


@core_method
def transaction_update(transaction_id, **kwargs):
    """
    Update transaction parameters

    Parameters, allowed to be updated:
        tag, note
    """
    a = ['tag', 'note']
    if config.full_transaction_update:
        a += ['created', 'completed', 'amount']
    _ckw(kwargs, a)
    kw = kwargs.copy()
    if 'created' in kw:
        kw['d_created'] = parse_date(kw['created'], return_timestamp=False)
        del kw['created']
    if 'completed' in kw:
        kw['d'] = parse_date(kw['completed'], return_timestamp=False)
        del kw['completed']
    if 'amount' in kw:
        kw['amount'] = parse_number(kw['amount'])
        if kw['amount'] <= 0:
            raise ValueError('Amount should be greater than zero')
    _update(transaction_id, 'transact', 'id', kw)


@core_method
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
    acc_info = account_info(account)
    try:
        if target is not None:
            target = parse_number(target)
            balance = account_balance(account)
            if balance > target:
                amount = -1 * (balance - target)
            elif balance < target:
                amount = target - balance
            else:
                return
        else:
            amount = parse_number(amount)
        if acc_info['passive']:
            amount *= -1
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
        if _ct_info and _dt_info['passive'] and _ct_info['passive']:
            amount = parse_number(target_dt) - account_balance(dt)
            ct, dt = dt, ct
        else:
            if _dt_info['passive']:
                target_dt *= -1
            amount = parse_number(target_dt) - account_balance(dt,
                                                               _natural=True)
    elif target_ct is not None:
        if _dt_info and _dt_info['passive'] and _ct_info['passive']:
            amount = account_balance(ct) - parse_number(target_ct)
            ct, dt = dt, ct
        else:
            if _ct_info['passive']:
                target_ct *= -1
            amount = account_balance(ct,
                                     _natural=True) - parse_number(target_ct)
    else:
        amount = parse_number(amount)
        if _ct_info is not None and _dt_info is not None and _ct_info[
                'passive'] and _dt_info['passive']:
            ct, dt = dt, ct
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
        date = parse_date(return_timestamp=False)
    else:
        date = parse_date(date, return_timestamp=False)
    if completion_date is None:
        if mark_completed:
            completion_date = date
    else:
        completion_date = parse_date(completion_date, return_timestamp=False)
    r = db.execute(sql("""
    insert into transact(account_credit_id, account_debit_id, amount, tag,
    note, d_created, d, chain_transact_id) values
    (
    (select id from account where code=:ct),
    (select id from account where code=:dt),
    :amount, :tag, :note, :d_created, :d, :chain_id)
    {}
    """.format('' if _db.use_lastrowid else 'returning id')),
                   ct=ct,
                   dt=dt,
                   amount=_multiply(amount),
                   tag=tag,
                   note=note,
                   d_created=date,
                   d=completion_date,
                   chain_id=chain_transact_id)
    return r.lastrowid if _db.use_lastrowid else r.fetchone().id


@core_method
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
        note: transaction note
        date: transaction creation date (default: now)
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
    if ct and dt and ct == dt:
        raise ValueError('Credit and debit account can not be equal')
    try:
        ctoken = account_lock(ct, credit_lock_token) if ct else None
        dtoken = account_lock(dt, debit_lock_token) if dt else None
        ct_info = account_info(ct) if ct else None
        dt_info = account_info(dt) if dt else None
        if ct and dt and ct_info['asset'] != dt_info['asset']:
            amount = parse_number(amount)
            if not amount:
                if target_ct is None and target_dt is None:
                    raise ValueError(
                        'Target should be specified either for dt or for ct')
                elif target_ct is not None:
                    amount = abs(parse_number(target_ct) - account_balance(ct))
                    xdt = False
                elif target_dt is not None:
                    if dt_info['passive'] and not ct_info['passive']:
                        amount = abs(
                            account_balance(dt) - parse_number(target_dt))
                    else:
                        current_balance = account_balance(dt)
                        if current_balance > parse_number(target_dt):
                            raise ValueError(
                                'The current balance is higher than target')
                        amount = parse_number(target_dt) - current_balance
            if ct_info['passive'] and dt_info['passive']:
                ct, dt = dt, ct
                ctoken, dtoken = dtoken, ctoken
                ct_info, dt_info = dt_info, ct_info
                xdt = False if xdt else True
            if config.lazy_exchange:
                if not amount:
                    raise ValueError(
                        'Amount is required for exchange operations')
                if not rate:
                    rate = asset_rate(ct_info['asset'],
                                      dt_info['asset'],
                                      date=date)

            else:
                raise ValueError('Asset mismatch')
            tid1 = _transaction_move(
                ct=ct,
                amount=format_amount(amount /
                                     rate, ct_info['asset']) if xdt else amount,
                tag=tag,
                note=note,
                date=date,
                completion_date=completion_date,
                mark_completed=mark_completed,
                _ct_info=ct_info)
            tid2 = _transaction_move(
                dt=dt,
                amount=amount if xdt else format_amount(amount *
                                                        rate, dt_info['asset']),
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


@core_method
def transaction_complete(transaction_ids, completion_date=None,
                         lock_token=None):
    """
    Args:
        transaction_ids: single or list/tuple of transaction ID
        completion_date: completion date (default: now)
    """
    logging.info('Completing transaction {}'.format(transaction_ids))
    if completion_date is None:
        completion_date = parse_date(return_timestamp=False)
    if config.keep_integrity:
        ids = transaction_ids if isinstance(transaction_ids,
                                            (list,
                                             tuple)) else [transaction_ids]
        for transaction_id in ids:
            dt = None
            with lock_account_token:
                tinfo = transaction_info(transaction_id)
                dt = tinfo['dt']
                if dt:
                    amount = tinfo['amount']
                    acc_info = account_info(dt)
            if dt:
                token = account_lock(dt, lock_token)
            try:
                if config.keep_integrity and dt:
                    if amount > 0 and acc_info[
                        'max_balance'] and account_balance(dt) + amount > \
                            acc_info['max_balance']:
                        raise OverlimitError
                if not get_db().execute(sql("""
                update transact set d=:d where id=:id"""),
                                        d=completion_date,
                                        id=transaction_id).rowcount:
                    logging.error(
                        'Transaction {} not found'.format(transaction_id))
                    raise ResourceNotFound
            finally:
                if config.keep_integrity and dt:
                    account_unlock(dt, token)


@deletion_method
@core_method
def transaction_delete(transaction_ids):
    """
    Delete (mark deleted) transaction
    """
    logging.warning('Deleting transaction {}'.format(transaction_ids))
    ids = transaction_ids if isinstance(transaction_ids,
                                        (list, tuple)) else [transaction_ids]
    for transaction_id in ids:
        tinfo = transaction_info(transaction_id)
        if not get_db().execute(sql("""
        UPDATE transact SET
            deleted=:ts WHERE (id=:id or chain_transact_id=:id) AND
            service IS null"""),
                                ts=parse_date(return_timestamp=False),
                                id=transaction_id).rowcount:
            logging.error('Transaction {} not found'.format(transaction_id))
            raise ResourceNotFound
        chid = tinfo.get('chain_transact_id')
        if chid:
            transaction_delete(chid)


@core_method
def transaction_purge(_lock=True):
    """
    Purge deleted transactions
    """
    if config.restrict_deletion:
        raise RuntimeError('transaction purge forbidden by server config')
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


@core_method
def transaction_copy(transaction_ids,
                     date=None,
                     completion_date=None,
                     mark_completed=None,
                     amount=None):
    """
    Copy transaction
    :param transaction_ids: one or list/tuple of transaction id
    :param date: transaction date
    :param completion_date: transaction completion date
    :param mark_completed: if no completion_date is specified, set completion
        date equal to creation. Default is True
    :param amount: new amount, if old transaction has chain_transact_id will be
            exception
    :return: list with id/ids new transaction
    """
    res = {}
    ids = transaction_ids if isinstance(transaction_ids,
                                        (list, tuple)) else [transaction_ids]
    for i, transaction_id in enumerate(ids):
        all_tr = [
            i[0] for i in (get_db().execute(sql("""
        select id from 
        transact where id=(select chain_transact_id from transact
            where id=:tr_id and chain_transact_id is not null)
        or id=(select id from transact where chain_transact_id=:tr_id);"""),
                                            tr_id=transaction_id)).fetchall()
        ]
        if all_tr and amount:
            raise ValueError(
                'Unable to change amount for transaction {}'.format(
                    transaction_id))
        all_tr.append(transaction_id)
        tr = []
        for t_id in sorted(all_tr):
            tinfo = transaction_info(t_id)
            ct_info = account_info(tinfo['ct']) if tinfo['ct'] is not None \
                                                                    else None
            dt_info = account_info(tinfo['dt']) if tinfo['dt'] is not None \
                                                                    else None
            if tinfo['chain_transact_id'] != t_id:
                all_tr.insert(
                    (all_tr.index(t_id) - 1 if all_tr.index(t_id) != 0 else 0),
                    tinfo['chain_transact_id'])
            chain = {'chain_transact_id': tr[0]} if tinfo[
                                            'chain_transact_id'] is not None\
                                            else {'chain_transact_id': None}
            params = {
                'ct': tinfo['ct'],
                'dt': tinfo['dt'],
                '_ct_info': ct_info,
                '_dt_info': dt_info,
                'amount': parse_number(amount) if amount else tinfo['amount'],
                'tag': tinfo['tag'],
                'note': tinfo['note'],
                'date': date if date is not None else None,
                'completion_date':
                    (completion_date
                     if completion_date is not None else tinfo['completed']),
                'mark_completed': mark_completed,
                **chain
            }
            new_tr = _transaction_move(**params)
            tr.append(new_tr)
            res[i] = tr
    result = [i for x in res.values() for i in x]
    return result if len(result) > 1 else result[0]


@core_method
def account_statement(account,
                      start=None,
                      end=None,
                      tag=None,
                      pending=True,
                      datefmt=False):
    """
    Args:
        account: account code
        start: statement start date/time
        end: statement end date/time
        tag: filter transactions by tag
        pending: include pending transactions
        datefmt: format date according to configuration
    Returns:
        generator object
    """
    acc_info = account_info(account)
    cond = 'transact.deleted is null and transact.service is null'
    d_field = 'd_created' if pending else 'd'
    if start:
        dts = parse_date(start, return_timestamp=False)
        cond += (' and ' if cond else '') + 'transact.{} >= \'{}\''.format(
            d_field, dts)
    dte = parse_date(end, return_timestamp=False) if end else parse_date(
        return_timestamp=False)
    cond += (' and ' if cond else '') + 'transact.{} <= \'{}\''.format(
        d_field, dte)
    if tag is not None:
        tag = _safe_format(tag) if isinstance(tag,
                                              (list,
                                               tuple)) else [_safe_format(tag)]
        tf = ['tag = \'{}\''.format(t) for t in tag]
        tags = ' or '.join(tf)
        cond += (' and ' if cond else '') + '({tags})'.format(tags=tags)
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
        row['created'] = format_date(d.d_created, force=datefmt)
        row['completed'] = format_date(d.d, force=datefmt)
        row['is_completed'] = d.d is not None
        row['amount'] = _demultiply(row['amount'])
        if acc_info['passive'] and row['amount']:
            row['amount'] *= -1
        yield row


@core_method
def account_statement_summary(account,
                              start=None,
                              end=None,
                              tag=None,
                              pending=True,
                              datefmt=False):
    """
    Args:
        account: account code
        start: statement start date/time
        end: statement end date/time
        tag: filter transactions by tag
        pending: include pending transactions
        datefmt: format date according to configuration
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
                          pending=pending,
                          datefmt=datefmt))
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


@core_method
def purge():
    """
    Purge deleted resources
    """
    logging.info('Purge requested')
    with lock_purge:
        result = {'transaction': transaction_purge(_lock=False)}
        return result


@core_method
def account_list(asset=None,
                 tp=None,
                 passive=None,
                 code=None,
                 date=None,
                 base=None,
                 order_by=['tp', 'asset', 'account', 'balance'],
                 hide_empty=False):
    """
    List accounts and their balances

    Args:
        asset: filter by asset
        tp: filter by account type (or types)
        passive: list passive, active or all (if None) accounts
        code: filter by acocunt code (may contain '%' as a wildcards)
        date: get balances for the specified date
        base: convert account balances to base currency
        order_by: list ordering
        hide_empty: hide accounts with zero balance, default is False
    """
    cond = "transact.deleted is null"
    if tp:
        tp = _safe_format(tp)
        if not isinstance(tp, (list, tuple)):
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
    if asset:
        first = True
        cond += (' and ' if cond else '') + '('
        for a in asset if isinstance(asset, list) or isinstance(
                asset, tuple) else [asset]:
            if first:
                first = False
            else:
                cond += ' or '
            cond += 'asset.code = \'{}\''.format(_safe_format(a.upper()))
        cond += ')'
    else:
        cond += (' and ' if cond else '') + 'account.tp <= 1000'
    dts = parse_date(date, return_timestamp=False) if date else parse_date(
        return_timestamp=False)
    cond += (' and '
             if cond else '') + 'transact.d_created <= \'{}\''.format(dts)
    if code:
        cond += (' and ' if cond else '') + 'account.code like \'{}\''.format(
            _safe_format(code.upper()))
    passive = val_to_boolean(passive)
    if passive is True:
        cond += (' and ' if cond else '') + 'account.passive is True'
    elif passive is False:
        cond += (' and ' if cond else '') + 'account.passive is not True'
    oby = ''
    if order_by:
        order_by = _safe_format(order_by)
        if isinstance(order_by, (list, tuple)):
            oby = ','.join(order_by)
        else:
            oby = order_by
    r = get_db().execute(
        sql("""
            select sum(balance) as balance, account, note, passive,
                asset, tp from 
                (
                select sum(amount) as balance,
                    account.code as account,
                    account.note as note,
                    account.passive as passive,
                    asset.code as asset,
                    account.tp as tp
                    from transact
                    left join account on account.id=transact.account_debit_id
                    join asset on asset.id=account.asset_id
                    where d is not null and {cond_d}
                        group by account.code, account.note,
                            account.passive, asset.code, account.tp
                union
                select -1*sum(amount) as balance,
                    account.code as account,
                    account.note as note,
                    account.passive as passive,
                    asset.code as asset,
                    account.tp as tp
                    from transact
                    left join account on account.id=transact.account_credit_id
                    join asset on asset.id=account.asset_id
                    where {cond}
                            group by account.code, account.note,
                            account.passive, asset.code, account.tp
                ) as templist
                    group by account, note, passive, templist.asset, templist.tp
            {oby}
            """.format(cond=cond,
                       cond_d=cond.replace('_created', ''),
                       oby=('order by ' + oby) if oby else '')))
    while True:
        d = r.fetchone()
        if not d: break
        # if zero is not a "real zero" - consider x < 0.000001 is zero
        if hide_empty is False or (abs(d.balance) > 0.000001):
            row = OrderedDict()
            for i in ('account', 'type', 'passive', 'note', 'asset', 'balance'):
                if i == 'type':
                    row['type'] = ACCOUNT_TYPE_NAMES[d.tp]
                elif i == 'passive':
                    row['passive'] = False if not d.passive else True
                else:
                    row[i] = getattr(d, i)
            if row['passive'] and row['balance']:
                row['balance'] *= -1
            row['balance'] = _demultiply(row['balance'])
            if base:
                row['balance'] *= asset_rate(row['asset'], base, date=date)
            yield row


@core_method
def account_list_summary(asset=None,
                         tp=None,
                         passive=None,
                         code=None,
                         date=None,
                         order_by=['tp', 'asset', 'account', 'balance'],
                         group_by=None,
                         hide_empty=False,
                         base=None):
    """
    List accounts and their balances plus return a total sum

    Args:
        asset: filter by asset
        tp: filter by account type (or types)
        passive: list passive, active or all (if None) accounts
        code: filter by acocunt code (may contain '%' as a wildcards)
        date: get balances for the specified date
        order_by: list ordering
        group_by: 'asset' or 'type'
        hide_empty: hide accounts with zero balance, default is False
        base: base asset (if not specified, config.base_asset is used)

    Returns:
        accounts: list of accounts or
        assets: list of assets or
        account_types: list of accoun types

        total: total sum in base asset
    """
    if base is None:
        base = config.base_asset
    accounts = list(
        account_list(asset=asset,
                     tp=tp,
                     passive=passive,
                     code=code,
                     date=date,
                     order_by=order_by,
                     hide_empty=hide_empty))
    for a in accounts:
        a['balance_bc'] = a['balance'] * asset_rate(a['asset'], base, date=date)
    if group_by:
        res = []
        if group_by not in ('asset', 'tp', 'type'):
            raise ValueError('Invalid group_by value')
        else:
            filt = 'asset' if group_by == 'asset' else 'type'
            dk = ('asset', 'balance_bc',
                  'balance') if group_by == 'asset' else ('type', 'balance_bc')
            f = lambda x: x[filt]
            accounts.sort(key=f)
            for k, v in groupby(accounts, f):
                val = list(v).copy()
                r = dict(
                    zip(dk, (k, sum([z['balance_bc'] for z in val
                                    ]), sum([w['balance'] for w in val]))))
                res.append(r)
        if group_by == 'asset':
            return {
                'assets':
                    res,
                'total':
                    sum(
                        format_amount(d['balance'], d['asset'], d['passive'])
                        if d['asset'] == base else format_amount(
                            d['balance'] *
                            asset_rate(d['asset'], base, date=date), d['asset'],
                            d['passive']) for d in accounts)
            }
        else:
            return {
                'account_types':
                    res,
                'total':
                    sum(
                        format_amount(d['balance_bc'], base, d['passive'])
                        for d in accounts)
            }
    return {
        'accounts':
            accounts,
        'total':
            sum(
                format_amount(d['balance'], d['asset'], d['passive']
                             ) if d['asset'] == base else format_amount(
                                 d['balance'] *
                                 asset_rate(d['asset'], base, date=date),
                                 d['asset'], d['passive']) for d in accounts)
    }


@core_method
def account_credit(account=None,
                   asset=None,
                   date=None,
                   tp=None,
                   order_by=['tp', 'account', 'asset'],
                   hide_empty=False):
    """
    Get credit operations for the account

    Args:
        account: filter by account code
        asset: filter by asset code
        date: get balance for specified date/time
        tp: FIlter by account type
        sort: field or list of sorting fields
        hide_empty: don't return zero balances

    Returns:
        generator object
    """
    return _account_summary('credit',
                            account=account,
                            asset=asset,
                            date=date,
                            tp=tp,
                            order_by=order_by,
                            hide_empty=hide_empty)


@core_method
def account_debit(account=None,
                  asset=None,
                  date=None,
                  tp=None,
                  order_by=['tp', 'account', 'asset'],
                  hide_empty=False):
    """
    Get debit operations for the account

    Args:
        account: filter by account code
        asset: filter by asset code
        date: get balance for specified date/time
        tp: FIlter by account type
        sort: field or list of sorting fields
        hide_empty: don't return zero balances

    Returns:
        generator object
    """
    return _account_summary('debit',
                            account=account,
                            asset=asset,
                            date=date,
                            tp=tp,
                            order_by=order_by,
                            hide_empty=hide_empty)


@core_method
def _account_summary(balance_type,
                     account=None,
                     asset=None,
                     date=None,
                     tp=None,
                     order_by=['tp', 'account', 'asset'],
                     hide_empty=False):
    cond = 'where {} transact.deleted is null'.format(
        'transact.d is not null and ' if _safe_format(balance_type) ==
        'debit' else '')
    if account:
        cond += (' and ' if cond else '') + 'account.code = \'{}\''.format(
            _safe_format(account))
    if asset:
        cond += (' and ' if cond else '') + 'asset.code = \'{}\''.format(
            _safe_format(asset))
    if date:
        dts = parse_date(date, return_timestamp=False)
        cond += (' and ' if cond else '') + 'transact.d <= \'{}\''.format(dts)
    if tp:
        if isinstance(tp, int):
            tp_id = tp
        else:
            tp_id = ACCOUNT_TYPE_IDS[_safe_format(tp)]
        cond += (' and ' if cond else '') + 'account.tp = {}'.format(tp_id)
    oby = ''
    if order_by:
        order_by = _safe_format(order_by)
        if isinstance(order_by, (list, tuple)):
            oby = ','.join(order_by)
        else:
            oby = order_by
    r = get_db().execute(
        sql("""select sum(amount) as {btype}_balance, account.id as id,
    account.tp as tp,
    account.code as account, asset.code as asset
    from transact 
    join account on transact.account_{btype}_id = account.id
    join asset on account.asset_id = asset.id {cond}
    group by account.code, asset.code {oby}""".format(
            btype=balance_type,
            cond=cond,
            oby=('order by ' + oby) if oby else '')))
    while True:
        d = r.fetchone()
        if not d: break
        if hide_empty is False or d.balance:
            row = OrderedDict()
            for i in ('account', 'type', 'asset', balance_type + '_balance'):
                if i == 'type':
                    row['type'] = ACCOUNT_TYPE_NAMES[d.tp]
                else:
                    row[i] = getattr(d, i)
            row[balance_type + '_balance'] = _demultiply(row[balance_type +
                                                             '_balance'])
            yield row


@core_method
def account_balance(account=None, tp=None, base=None, date=None,
                    _natural=False):
    """
    Get account balance

    Args:
        account: account code
        tp: account type/types
        base: base asset (if not specified, config.base_asset is used)
        date: get balance for specified date/time
    """
    if account and tp:
        raise ValueError('Account and type can not be specified together')
    elif not account and not tp:
        tp = [k for k in ACCOUNT_TYPE_IDS if ACCOUNT_TYPE_IDS[k] <= 1000]
    cond = "transact.deleted is null"
    dts = parse_date(date, return_timestamp=False) if date else parse_date(
        return_timestamp=False)
    cond += (' and '
             if cond else '') + 'transact.d_created <= \'{}\''.format(dts)
    balance = None
    if account:
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
        balance = _demultiply(d.balance)
        if base and acc_info['asset'] != base:
            balance = balance * asset_rate(acc_info['asset'], base, date=date)
        else:
            balance = format_amount(balance, acc_info['asset'])
        if not _natural and acc_info['passive'] and balance:
            balance *= -1
    elif tp:
        if not base:
            base = config.base_asset
        accounts = account_list_summary(tp=tp,
                                        group_by='tp',
                                        date=date,
                                        base=base,
                                        hide_empty=True)
        balance = accounts['total']
    return balance


@core_method
def account_balance_range(start,
                          account=None,
                          tp=None,
                          end=None,
                          step=1,
                          return_timestamp=True,
                          base=None):
    """
    Get list of account balances for the specified range

    Args:
        account: account code
        tp: account type/types
        start: start date/time, required
        end: end date/time, if not specified, current time is used
        step: list step in days
        return_timestamp: return dates as timestamps if True, otherwise as
            datetime objects. Default is True

    Returns:
        tuple with time series list and corresponding balance list
    """
    if account and tp:
        raise ValueError('Account and type can not be specified together')
    elif not account and not tp:
        tp = [k for k in ACCOUNT_TYPE_IDS if ACCOUNT_TYPE_IDS[k] <= 1000]
    times = []
    data = []
    acc_info = {'account': account} if account else {'tp': tp}
    dt = parse_date(start, return_timestamp=False)
    end_date = parse_date(
        end, return_timestamp=False
    ) if end else datetime.datetime.now() + datetime.timedelta(days=1)
    delta = datetime.timedelta(days=step)
    last_record = False
    while dt < end_date or not last_record:
        if dt == end_date:
            break
        elif dt > end_date:
            last_record = True
        times.append(dt.timestamp() if return_timestamp else dt)
        b = account_balance(**acc_info, base=base, date=dt)
        data.append(b)
        dt += delta
    return times, data


def _safe_format(val):
    n_allow = '\'";'
    for al in n_allow:
        if isinstance(val, (list, tuple)):
            val = [
                v.replace(al, '')
                if not isinstance(v, (int, float)) and al in v else v
                for v in val
            ]
        elif isinstance(val, str):
            val = val.replace(al, '') if al in val else val
        # forbid bytes
        elif isinstance(val, bytes) or isinstance(val, bytearray):
            val = ''
    return val
