Authentication with flowfabricpy
================================

Authentication
--------------

`flowfabricpy` uses JWT Bearer authentication. By default, it manages
your token in the background using
:py:func:`flowfabric_get_token()`. You can also set or override the
token explicitly for any function call.

Background (global) token
-------------------------

Authenticate once per session:

.. code-block:: python

    if is_interactive:
        # This will obtain and cache a token for all API calls
        flowfabric_get_token()  # triggers login if needed
    else:
        print('Skipping authentication: not running interactively.\n')

        ## Skipping authentication: not running interactively.

Explicit token usage
--------------------

You can pass a token directly to some functions:

.. code-block:: python

    if is_interactive:
        token = flowfabric_get_token()
        datasets = flowfabric_list_datasets(token = token)
        print(datasets)
    else:
        print('Skipping dataset listing: not running interactively.\n')

        ## Skipping dataset listing: not running interactively.


Getting a Token
---------------

You can use the built-in function to obtain a token interactively:

.. code-block:: python

    if is_interactive:
        token = flowfabric_get_token()
        print(token)
    else:
        print('Skipping token retrieval: not running interactively.\n')

        ## Skipping token retrieval: not running interactively.


Or pass a token directly to any API function:

.. code-block:: python

    datasets = flowfabric_list_datasets(token = "YOUR_TOKEN")


Token Refresh
-------------

Tokens are automatically refreshed if expired, using the underlying logic.

