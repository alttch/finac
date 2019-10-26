Finac - financial accounting for humans
=======================================

Finac is a library and function set for Jupyter/ipython, which provides
a simple double-entry bookkeeping database.

Finac is simple, open and free. It can work with SQLite or any database
supported by SQLAlchemy.

Install
-------

.. code:: bash

   pip3 install finac

Sources: https://github.com/alttch/finac

Documentation: https://finac.readthedocs.io/

How to use
----------

Finac database contain 3 entity types:

-  **currency** asset currency, currencies “USD” and “EUR” are created
   automatically

-  **account** bank account, counterparty account, tax account, special
   account etc. Everything is accounts :)

-  **transaction** movements from (credit) / to (debit) and between
   accounts

Transactions can be simple (no counterparty) or classic double-entry
bookkeeping (between debit and credit account)

.. code:: python

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
   f.mv(dt='acc2', ct='acc1', 2000)
   f.mv(dt='depo', ct='acc1', 3000)
   # display statement for acc1
   f.ls('acc1')
   # display summary for all accounts
   f.ls()
   # 

TODO
----

-  Cross-currency rates
-  Portfolio management functions
-  finac-cli
