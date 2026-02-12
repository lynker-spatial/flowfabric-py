Getting Started with flowfabricpy
=================================

Overview
--------

`flowfabricpy` provides a high-performance Python interface to the FlowFabric
REST API, including JWT authentication. This vignette demonstrates basic
usage for expert Python users.

Authentication
--------------

Authentication uses a Bearer token. By default, `flowfabricpy` manages
your token in the background using
:py:func:`flowfabric_get_token()`. You can also
set or override the token explicitly for any function call.

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

See the :doc:`authentication` vignette for more details.

List Datasets
-------------

.. code-block:: python

    datasets = flowfabric_list_datasets()
    print(datasets)

Estimate Query Cost
-------------------

.. code-block:: python

    est = flowfabric_streamflow_estimate(
        dataset_id = "nws_owp_nwm_analysis",
        params = {"issue_time": "latest", "scope": "features", "feature_ids": ["101"]}
    )
    print(est)

Query Streamflow Data
---------------------

.. code-block:: python

    tbl = flowfabric_streamflow_query(
        dataset_id = "nws_owp_nwm_analysis",
        params = {"issue_time": "latest", "scope": "features", "feature_ids": ["101"]}
    )
    print(tbl)

