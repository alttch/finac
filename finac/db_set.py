import logging
from sqlalchemy import (Table, Column, Integer, String, MetaData, TEXT, Float,
                        ForeignKey, text, Index, Boolean, DateTime)
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger('finac')

meta = MetaData()

asset = Table('asset',
              meta,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('code', String(20), nullable=False, unique=True),
              Column('precs', Integer, nullable=False, default=2),
              mysql_engine='InnoDB',
              mysql_charset='utf8mb4')

asset_rate = Table('asset_rate',
                   meta,
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
                   Index('asset_rate_asset_from_id', 'asset_from_id'),
                   Index('asset_rate_asset_to_id', 'asset_to_id'),
                   Column('d', DateTime, nullable=False, primary_key=True),
                   Column('value', Float(precision=32), nullable=False),
                   mysql_engine='InnoDB',
                   mysql_charset='utf8mb4')

account = Table('account',
                meta,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('code', String(60), nullable=False, unique=True),
                Column('note', String(2048)),
                Column('tp', Integer, nullable=False),
                Column('passive', Boolean, default=False),
                Column('asset_id',
                       Integer,
                       ForeignKey('asset.id', ondelete='CASCADE'),
                       nullable=False),
                Index('account_asset_id', 'asset_id'),
                Column('max_overdraft', Float(precision=32)),
                Column('max_balance', Float(precision=32)),
                mysql_engine='InnoDB',
                mysql_charset='utf8mb4')

transact = Table('transact',
                 meta,
                 Column('id', Integer, primary_key=True, autoincrement=True),
                 Column('account_credit_id', Integer,
                        ForeignKey('account.id', ondelete='SET NULL')),
                 Column('account_debit_id', Integer,
                        ForeignKey('account.id', ondelete='SET NULL')),
                 Column('amount', Float(precision=32), nullable=False),
                 Column('tag', String(20)),
                 Column('note', String(20)),
                 Column('note', String(1024), default=''),
                 Column('d_created', DateTime, nullable=False),
                 Column('d', DateTime),
                 Index('transact_account_credit_id', 'account_credit_id'),
                 Index('transact_account_debit_id', 'account_debit_id'),
                 Column('chain_transact_id', Integer),
                 Index('transact_chain_tranasct_id', 'chain_transact_id'),
                 Column('deleted', DateTime, default=None),
                 mysql_engine='InnoDB',
                 mysql_charset='utf8mb4')


def init_db(engine):
    meta.create_all(engine)
    for cur in ('EUR', 'USD'):
        try:
            engine.execute(text("""
    insert into asset(code, precs) values(:code, 2)"""),
                           code=cur)
        except IntegrityError as e:
            pass
