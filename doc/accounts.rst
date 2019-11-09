Account types
*************

Finac account types are hard-coded and can not be changed. Type codes are
reserved for the future.

Financial assets
----------------

Finac considers accounts with types "credit", "cash", "current" and "saving"
are primary financial assets and includes them in various listings by default.

* **credit** credit accounts
* **cash** cash (cash desk) accounts
* **current** current bank accounts
* **saving** saving accounts and deposits

Special accounts
----------------

* **transit** transit accounts
* **escrow** escrow accounts
* **holding** holding accounts
* **virtual** virtual accounts
* **temp** temporary accounts
* **exchange** virtual exchange accounts (not used by lazy exchange operations)

Customers, counterparties and company assets
--------------------------------------------

Finac considers accounts with the below types are primary financial assets and
includes them in various listings by default.

* **gs** goods and services
* **supplier** supplier accounts, *passive by default*
* **customer** customer accounts
* **finagent** financial agent accounts, *passive by default*

Investment accounts
-------------------

* **stock** stocks
* **bond** bonds
* **fund** mutual and other funds
* **metal** precious metals
* **reality** real estate objects

Taxes
-----

All tax accounts have the same type: **tax**. Taxes are included in listings by
default, all tax accounts are passive (unless changed).
