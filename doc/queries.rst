Queries
-------

Syntax
======

Finac has a simple query language to access core functions.

Currently only function call statements are supported:

.. code:: sql

    SELECT <function>([args, kwargs])

    /* e.g. */

    SELECT account_balance("myaccount")

Supported core functions:

* get_version
* asset_list
* asset_list_rates
* asset_rate^
* asset_rate_range^
* account_info
* account_statement
* account_list
* account_balance^
* account_balance_range^

Functions marked with "^" support data column assignment with "AS":

.. code:: sql

    SELECT account_balance("myaccount") AS myacc

Executing queries
=================

Interactive
~~~~~~~~~~~

In the interactive mode, query can be executed as:

.. code:: python

    f.query('select account_list()')

The function outputs query result to stdout.

Embedded
~~~~~~~~

If the application want to execute Finac query, it should call method

.. code:: python

    f.exec_query('select account_list()')

The function always returns list of dicts, where list items are result rows and
dict keys are result columns.

API
~~~

Calling queries via :doc:`API <api>` is possible either via JSON RPC, or via
special URI **/query**.

The URI can be requested either via GET (with param q=<query>) or via POST
(with list of queries in JSON payload).

The response format is:

.. code:: javascript

    {
        'ok': true,
        'result': query_result, // (list of dicts)
        'rows': number_of_rows, // integer
        'time': time_spent_in_seconds // float
    }

For GET, errors are returned as HTTP status:

* **400** bad request (e.g. invalid query format / params)
* **404** resource not found
* **409** resource already exists, over limit / overdraft error
* **500** all other errors

For POST, list of responses is returned. If certain query failed with an error,
its response contains *error* field only.
