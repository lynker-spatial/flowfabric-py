Advanced Usage of flowfabricpy
===============================

Advanced Usage
--------------

This vignette covers advanced features of `flowfabricpy`, including
custom HTTP requests, error handling, and working with Arrow IPC data.

Custom HTTP Requests
--------------------

You can use the low-level HTTP helpers for custom endpoints:

.. code-block:: python

    if is_interactive:
        resp = flowfabric_get("/healthz")
        print(resp.json())
    else:
        print('Skipping HTTP request: not running interactively.\n')

        ## Skipping HTTP request: not running interactively.

Error Handling
--------------

All API errors return a structured JSON error. Use `try...except` to handle
errors gracefully:

.. code-block:: python

    if is_interactive:
        try:
            flowfabric_streamflow_query("bad-dataset")
        except TypeError as e:
            print(f"Error querying streamflow: {e}")
    else:
        print('Skipping error handling example: not running interactively.\n')

        ## Skipping error handling example: not running interactively.

Arrow IPC Data
--------------

All data queries return Arrow tables if possible. Convert to other formats as needed:

.. code-block:: python

    if is_interactive:
        tbl = flowfabric_streamflow_query("nws_owp_nwm_analysis", params={"issue_time": "latest", "scope": "features", "feature_ids": ["101"]})
        print(tbl)
    else:
        cat('Skipping Arrow IPC example: not running interactively.\n')

        ## Skipping Arrow IPC example: not running interactively.
