Finac Server and HTTP API
*************************

You may run Finac as the server with HTTP API. Running in server mode is highly
recommended if you work with remote database.

Finac server
============

To run Finac server, create a WSGI app, in example put the following code
inside file, named **server.py**:

.. code:: python

   import finac as f
   import finac.api as api

   f.init('mysql+pymysql://someuser:somepassword@dbhost/dbname')
   # optional API key
   api.key = 'secret'
   application = api.app

   if __name__ == '__main__':
       application.run(host='0.0.0.0', debug=True)

The code above can be launched directly for the debugging purposes, for
production it's recommended to use WSGI server, e.g. **gunicorn**:

.. code:: bash

   gunicorn -b 0.0.0.0:80 server

Finac client
============

To use Finac library as client for Finac server, provide API params for
**init()** function:

.. code:: python

   import finac as f

   f.init(api_uri='http://finac-host:80/jrpc',
      # optional API key
      api_key='secret',
      # API timeout in seconds, default: 5
      api_timeout=10,
      # cache rates for 600 seconds
      rate_ttl=600)
   # preload static data to avoid unnecessary future requests
   f.preload()

Calling API functions directly
==============================

Finac uses `JSON RPC 2.0 <https://www.jsonrpc.org/specification>`_ protocol.
Default API URI is

   http(s)://host:port/jrpc

You may call any :doc:`core function <core>`, single or batch.

Error codes:

* **-32603** Internal error
* **-32602** Invalid method param
* **-32601** Method not found
* **-32000** Access denied (if API key is set)
* **-32001** ResourceNotFound exception
* **-32002** RateNotFound exception
* **-32003** OverdraftError exception
* **-32004** OverlimitError exception
* **-32005** ResourceAlreadyExists exception

