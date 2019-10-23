drop table currency;
drop table currency_rate;
drop table account;
drop table transact;
drop table asset_group;
drop table asset;
drop table asset_price;

create table currency(
  id integer primary key autoincrement,
  code text not null unique
);

create table currency_rate(
  currency_from_id integer not null,
  currency_to_id integer not null,
  d integer not null,
  value real not null,
  primary key(currency_from_id, currency_to_id, d),
  foreign key(currency_from_id) references currency(id) on delete cascade,
  foreign key(currency_to_id) references currency(id) on delete cascade
);

create table account(
  id integer primary key autoincrement,
  code text not null unique,
  note text default '',
  tp integer not null,
  currency_id integer not null,
  max_overdraft real,
  max_balance real,
  foreign key(currency_id) references currency(id) on delete cascade);

create table transact(
  id integer primary key autoincrement,
  account_credit_id integer,
  account_debit_id integer,
  amount real not null,
  tag text,
  note text default '',
  d_created integer not null,
  d integer,
  chain_transact_id integer,
  deleted integer default null,
  foreign key(account_credit_id) references account(id) on delete set null,
  foreign key(account_debit_id) references account(id) on delete set null
);

create table asset_group(
  id integer primary key autoincrement,
  code text not null unique);

create table asset(
  id integer primary key autoincrement,
  code text unique,
  currency_id integer not null,
  asset_group_id integer not null,
  amount real not null default 0,
  foreign key(currency_id) references currency(id) on delete cascade,
  foreign key(asset_group_id) references asset_group(id) on delete cascade
);

create table asset_price(
  id integer primary key autoincrement,
  asset_id integer not null,
  d integer not null,
  value real not null,
  foreign key(asset_id) references asset(id)  on delete cascade
);

insert into currency(code) values('USD');
insert into currency(code) values('EUR');
