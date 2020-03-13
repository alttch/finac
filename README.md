# Finac - financial accounting for humans

Finac is a library and function set for Jupyter/ipython, which provides a
double-entry bookkeeping database.

Finac is simple, open and free. It can work with SQLite or any database
supported by SQLAlchemy (tested: SQLite, MySQL, PostgreSQL).

<img src="https://img.shields.io/pypi/v/finac.svg" /> <img src="https://img.shields.io/badge/license-MIT-green" /> <img src="https://img.shields.io/badge/python-3.5%20%7C%203.6%20%7C%203.7%20%7C%203.8-blue.svg" />

You can use Finac either in interactive mode with
[Jupyter](https://jupyter.org/), [Spyder-IDE](https://www.spyder-ide.org/),
ipython or other similar environment or embed Finac library into own projects.
The library may be used in accounting applications as well it's useful for the
fin-tech services.

Finac supports multiple currencies, simple transactions, double-entry
bookkeeping transactions, watches overdrafts, balance limits and has many
useful features, which make accounting simple and fun.

## Install

```bash
pip3 install finac
```

Sources: https://github.com/alttch/finac

Documentation: https://finac.readthedocs.io/

## Updating

# from 0.4.10

```sql
ALTER TABLE transact ADD service bool;
UPDATE transact SET service=True WHERE d_created<'1970-01-03';
ALTER TABLE transact ADD FOREIGN KEY(chain_transact_id)
  REFERENCES transact(id) ON DELETE SET null;
```

# from 0.3.x

Starting from 0.4, Finac uses DateTime columns for:

* asset_rate.d
* transact.d
* transact.d_created
* transact.deleted

Depending to your database type, it's REQUIRED to convert these columns to
either DATETIME (SQLite, for MySQL DATETIME(6) recommended) or TIMESTAMP
(PostgreSQL, with timezone).

## How to use in interactive mode

Finac database contain 3 entity types:

* **asset** currency, ISIN, stock code etc., currencies "USD" and "EUR" are
  created automatically. Finac doesn't divide assets into currencies, property
  and other. This allows creating applications for the various areas using the
  single library.

* **account** bank account, counterparty account, tax account, special account
  etc. Everything is accounts :)

* **transaction** movements from (credit) / to (debit) and between accounts

Assets have **rates** - value of one asset, relative to other.

Transactions can be simple (no counterparty) or classic double-entry
bookkeeping (between debit and credit account)

```python
import finac as f
# init finac, 
f.init('/tmp/test.db')
# create a couple of accounts
f.account_create('acc1', 'USD')
f.account_create('acc2', 'USD')
f.account_create('depo', 'USD', 'saving')
# import initial balance with a simple transaction
f.tr('acc1', 10000, tag='import')
# move some assets to other accounts
f.mv(dt='acc2', ct='acc1', amount=2000)
f.mv(dt='depo', ct='acc1', amount=3000)
```

```python
# display statement for acc1
f.ls('acc1')
```

```
id     amount  cparty  tag     note  created              completed
-----------------------------------------------------------------------------
7   10 000.00          import        2019-10-26 03:04:02  2019-10-26 03:04:02
8   -2 000.00  ACC2                  2019-10-26 03:04:02  2019-10-26 03:04:02
9   -3 000.00  DEPO                  2019-10-26 03:04:02  2019-10-26 03:04:02
-----------------------------------------------------------------------------
Debit turnover: 10 000.00, credit turnover: 5 000.00

Net profit/loss: 5 000.00 USD
```

```python
# display summary for all accounts
f.ls()
```

```
account  type      asset     balance  balance USD
-------------------------------------------------
ACC1     current    USD     5 000.00     5 000.00
ACC2     current    USD     2 000.00     2 000.00
DEPO     saving     USD     3 000.00     3 000.00
-------------------------------------------------
Total: 10 000.00 USD
```

```python
# display summary only for current accounts
f.ls(tp='current')
```

```
account  type     asset     balance   balance USD
-------------------------------------------------
ACC1     current    USD     5 000.00     5 000.00
ACC2     current    USD     2 000.00     2 000.00
-------------------------------------------------
Total: 7 000.00 USD
```

```python
# display assets pie chart, (wrapper for matplotlib.pyplot, requires Jupyter,
# Spyder-IDE or similar interactive environment)
f.pie()
```
<img src="https://github.com/alttch/finac/blob/master/doc/images/pie.png?raw=true" width="400" />

Note: when addressing currencies and accounts both in interactive and API mode,
you should use account and asset codes as object identifiers. **All codes
are case-insensitive**.

Inside database, Finac uses numeric IDs to connect objects, so all their codes
can be changed without any problems.

## Special features

### Lazy exchange

Finac can automatically move assets between accounts with different currencies,
if exchange rate is set or specified in transaction details:

```python
# create EUR account
f.account_create('acc5', 'eur')
# set exchange rate (in real life you would probably use cron job)
f.asset_set_rate('eur/usd', value=1.1)
f.mv(dt='acc5', ct='acc1', amount=100)
```

hoorah, account acc5 got 100 EUR! And exchange rate was 1.1. Let's check:

    >>> f.ls('acc1')

```
id     amount  cparty  tag     note  created              completed
-----------------------------------------------------------------------------
..............
..............
14    -110.00                        2019-10-26 03:15:41  2019-10-26 03:15:41
-----------------------------------------------------------------------------
```

    >>> f.ls('acc5')

```
id  amount  cparty  tag  note  created              completed
-----------------------------------------------------------------------
15  100.00                     2019-10-26 03:15:41  2019-10-26 03:15:41
-----------------------------------------------------------------------
Debit turnover: 100.00, credit turnover: 0.00

Net profit/loss: 100.00 EUR
```

As you see, there's no counterparty account in lazy exchange. This feature is
useful for personal accounting and special applications, but for the
professional accounting, you should create counterparty exchange account and
perform buy-sell transactions with it.

### Targets

Targets is a feature I wrote Finac for. You have account balances in bank and
in accounting. They differ by some amount and you are going to record this with
a single transaction.

But the problem is there's a lot of transactions you should sum up. Or
calculate the difference between bank balance and accounting. Pretty common,
eh? Don't do this, we have targets.

Specifying targets instead of amount tells Finac to calculate transaction
amount by itself.

After the previous operation, we have *4,890.00* USD on "acc1" and want to move
all except $1000 to "acc2". Let's do it:

    >>> f.mv(dt='acc2', ct='acc1', target_ct=1000)

```
id     amount  cparty  tag     note  created              completed
-----------------------------------------------------------------------------
......
......
16  -3 890.00  ACC2                  2019-10-26 03:25:56  2019-10-26 03:25:56
-----------------------------------------------------------------------------
Debit turnover: 10 000.00, credit turnover: 9 000.00

Net profit/loss: 1 000.00 USD

```

The transaction amount is automatically calculated. Lazy people are happy :)

If you want to specify a debit account balance target instead, use *target_dt*
function argument. Note: calculated transaction amount should be always greater
than zero (if you try specifying credit account target higher than its current
balance, you get *ValueError* exception)

For the simple transactions (*f.tr(...))*), use *target=*.

### Transaction templates

Example: you have a recurrent payment orders in your bank, which pay office
utility bills every 5th day of month, plus automatically moves $100 to saving
account. To fill this into accounting, just create YAML transaction template:

```yaml
transactions:
  - account: acc1
    amount: 200
    tag: electricity
    note: energy company deposit
  - account: acc1
    amount: 800
    tag: rent
    note: office rent
  - dt: depo
    ct: acc1
    amount: 200
    tag: savings
    note: rainy day savings
```

then create a cron job which calls *f.transaction_apply("/path/to/file.yml")*
and that's it.

Actually, transaction templates are useful for any recurrent operations. You
may specify all same arguments, as for the core functions.

### Number formatting

Finac doesn't use system locale. If you input amounts and targets as strings,
you may input them in any format and Finac will try converting it to the float
numeric automatically. The following values for amounts and targets are valid
and will be automatically parsed:

* 1 000,00 = 1000.0
* 1,000.00 = 1000.0
* 1.000,00 = 1000.0
* 1,000.00 = 1000.0
* 10,0 = 10.0
* 10.0 = 10.0

### Passive accounts

If account is passive, its assets are decremented from totals. To create
passive account, use *passive* argument:

```python
f.account_create('passive1', 'usd', passive=True)
```

Accounts of types "tax", "supplier" and "finagent" are passive by default.

### Data multiplier

Depending on data, it may be useful to store numeric values in database as
integers instead of floats. Finac library has built-in data multiplier feature,
to enable it, set *multiplier=N* in *finac.init()* method, e.g.
*multiplier=1000* if you want to store integers in tables and have data with
max precision 3 digits after comma.

Note: you must manually convert table fields to numeric/integer types, and
multiply them if performing data multiplier implementation on living database.

Full list of tables and fields is available in dict
*finac.core.multiply_fields*.

Note: multiplier can be used only with integer and numeric(X) field types, as
core conversion functions always return rounded value.

## How to embed Finac library into own project

See [Finac documentation](https://finac.readthedocs.io/) for core function API
details.

## Client-server mode and HTTP API

See [Finac documentation](https://finac.readthedocs.io/) for server mode and
HTTP API details.
