def init_db(engine):
    import logging
    from sqlalchemy import (Table, Column, Integer, String, MetaData, TEXT,
                            Float, ForeignKey, text as sql, Index, Boolean,
                            DateTime)
    from sqlalchemy.exc import IntegrityError, ProgrammingError

    if 'mysql' in engine.name:
        from sqlalchemy.dialects.mysql import DATETIME
        from functools import partial
        DateTime = partial(DATETIME, fsp=6)

    meta = MetaData()

    asset = Table('asset',
                  meta,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('code', String(20), nullable=False, unique=True),
                  Column('precs', Integer, nullable=False, server_default='2'),
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
                       Column('d',
                              DateTime(timezone=True),
                              nullable=False,
                              primary_key=True),
                       Column('value', Float(precision=32), nullable=False),
                       mysql_engine='InnoDB',
                       mysql_charset='utf8mb4')

    account = Table('account',
                    meta,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('code', String(60), nullable=False, unique=True),
                    Column('note', String(2048)),
                    Column('tp', Integer, nullable=False),
                    Column('passive',
                           Boolean,
                           nullable=True,
                           server_default='0'),
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
                     Column('id', Integer, primary_key=True,
                            autoincrement=True),
                     Column('account_credit_id', Integer,
                            ForeignKey('account.id', ondelete='SET NULL')),
                     Column('account_debit_id', Integer,
                            ForeignKey('account.id', ondelete='SET NULL')),
                     Column('amount', Float(precision=32), nullable=False),
                     Column('tag', String(20)),
                     Index('transact_tag', 'tag'),
                     Column('note', String(20)),
                     Column('note', String(1024), server_default=''),
                     Column('d_created',
                            DateTime(timezone=True),
                            nullable=False),
                     Column('d', DateTime(timezone=True)),
                     Index('transact_account_credit_id', 'account_credit_id'),
                     Index('transact_account_debit_id', 'account_debit_id'),
                     Column(
                         'chain_transact_id',
                         Integer,
                         ForeignKey('transact.id', ondelete='SET NULL'),
                     ),
                     Index('transact_chain_transact_id', 'chain_transact_id'),
                     Column('deleted', DateTime(timezone=True), nullable=True),
                     Column('service', Boolean, nullable=True),
                     mysql_engine='InnoDB',
                     mysql_charset='utf8mb4')

    meta.create_all(engine)
    conn = engine.connect()
    for cur in ('EUR', 'USD'):
        try:
            conn.execute(sql("""
                INSERT INTO asset(code, precs) VALUES(:code, 2)"""),
                         code=cur)
        except IntegrityError:
            pass
