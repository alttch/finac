Interactive functions
*********************

Function aliases
================

The following functions are aliases for :doc:`core`:

.. code:: python

   import finac as f

   f.tr(...) # alias for finac.core.transaction_create
   f.mv(...) # alias for finac.core.transaction_move
   f.rm(...) # alias for finac.core.transaction_delete
   f.complete(...) # alias for finac.core.transaction complete
   stmt(...) # alias for finac.core.account_statement_summary
   lsaccs(...) # alias for finac.core.account_list_summary
   balance(...) # alias for finac.core.account_balance
   balance_range(...) # alias for finac.core.account_balance_range, in opposite
                      # to the original function, returns dates as datetime
                      # object by default

Special interactive functions
=============================

.. automodule:: finac
   :members:
   :no-undoc-members:
   :exclude-members: balance_range
