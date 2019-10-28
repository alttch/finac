import logging
from sqlalchemy import (Table, Column, Integer, String, MetaData, TEXT, Float,
                        ForeignKey, text)
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger('finac')

meta = MetaData()

asset = Table('asset', meta,
                 Column('id', Integer, primary_key=True, autoincrement=True),
                 Column('code', String(20), nullable=False, unique=True),
                 Column('precs', Integer, nullable=False, default=2))

asset_rate = Table(
    'asset_rate', meta,
    Column('asset_from_id',
           Integer,
           ForeignKey('asset.id', ondelete='CASCADE'),
           nullable=True,
           primary_key=True),
    Column('asset_to_id',
           Integer,
           ForeignKey('asset.id', ondelete='CASCADE'),
           nullable=False,
           primary_key=True),
    Column('d', Integer, nullable=False, primary_key=True),
    Column('value', Float, nullable=False))

account = Table(
    'account', meta, Column('id', Integer, primary_key=True,
                            autoincrement=True),
    Column('code', String(60), nullable=False, unique=True),
    Column('note', String(2048)), Column('tp', Integer, nullable=False),
    Column('asset_id',
           Integer,
           ForeignKey('asset.id', ondelete='CASCADE'),
           nullable=False), Column('max_overdraft', Float),
    Column('max_balance', Float))

transact = Table(
    'transact', meta, Column('id',
                             Integer,
                             primary_key=True,
                             autoincrement=True),
    Column('account_credit_id', Integer,
           ForeignKey('account.id', ondelete='SET NULL')),
    Column('account_debit_id', Integer,
           ForeignKey('account.id', ondelete='SET NULL')),
    Column('amount', Float, nullable=False), Column('tag', String(20)),
    Column('note', String(20)), Column('note', String(20), default=''),
    Column('d_created', Integer, nullable=False), Column('d', Integer),
    Column('chain_transact_id', Integer),
    Column('deleted', Integer, default=None))


def init_db(engine):
    meta.create_all(engine)
    for cur in ('EUR', 'USD'):
        try:
            engine.execute(text("""
    insert into asset(code, precs) values(:code, 2)"""),
                           code=cur)
        except IntegrityError as e:
            pass
