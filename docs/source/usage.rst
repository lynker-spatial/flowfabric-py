Function Details
================

Installation
------------
To use flowfabricpy, first install it using pip:

.. code-block:: console

   pip install git+https://github.com/lynker-spatial/flowfabric-py#egg=flowfabricpy


Core Functions
--------------
Core functions for interacting with the FlowFabric API:

.. autofunction:: flowfabricpy.flowfabric_list_datasets()


.. autofunction:: flowfabricpy.flowfabric_get_dataset(dataset_id)


.. autofunction:: flowfabricpy.flowfabric_get_latest_run(dataset_id, verbose=False)


.. autofunction:: flowfabricpy.flowfabric_streamflow_query(dataset_id, feature_ids=None, start_time=None, end_time=None, issue_time=None, params=None, verbose=False)


.. autofunction:: flowfabricpy.flowfabric_streamflow_estimate(dataset_id, feature_ids=None, start_time=None, end_time=None, issue_time=None, params=None, verbose=False)


.. autofunction:: flowfabricpy.flowfabric_ratings_query(feature_ids, type="rem", format="arrow", verbose=False)


.. autofunction:: flowfabricpy.flowfabric_ratings_estimate(feature_ids, type="rem", format="arrow", verbose=False)


.. autofunction:: flowfabricpy.flowfabric_stage_query(dataset_id, params=None, verbose=False)


.. autofunction:: flowfabricpy.flowfabric_inundation_ids(params=dict, verbose=False)


.. autofunction:: flowfabricpy.flowfabric_healthz(token=None, verbose=False)


.. autofunction:: flowfabricpy.get_bearer_token(force_refresh=False)


Catalog Utilities
-----------------

.. autofunction:: flowfabricpy.auto_streamflow_params(dataset_id)


HTTP Utilities
--------------

.. autofunction:: flowfabricpy.flowfabric_get(endpoint, token=None, verbose=False)


.. autofunction:: flowfabricpy.flowfabric_post(endpoint, body, token=None, verbose=False)


Auth Utilities
--------------

.. autofunction:: flowfabricpy.flowfabric_get_token(force_refresh=False)


.. autofunction:: flowfabricpy.flowfabric_refresh_token()

