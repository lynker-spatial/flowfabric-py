# client.py
from .flowfabric_http import flowfabric_get, flowfabric_post
from .auth import flowfabric_get_token

# list all datasets
def flowfabric_list_datasets():
    resp = flowfabric_get("/v1/datasets", token=None)
    resp = [x for x in resp if x is not None and isinstance(x, dict) and len(x) > 0]
    if len(resp) == 0:
        return list()
    # add filtering
    return resp

# return a specific dataset
def flowfabric_get_dataset(dataset_id, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id])
    resp = flowfabric_get(endpoint, token=token, verbose=verbose)
    return resp

# get latest run for a specific dataset
def flowfabric_get_latest_run(dataset_id, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id, "/runs/latest"])
    resp = flowfabric_get(endpoint, token=token, verbose=verbose)
    return resp

# get a specific run for a specified dataset
def flowfabric_get_run(dataset_id, issue_time, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id, "/runs/", issue_time])
    resp = flowfabric_get(endpoint, token=token, verbose=verbose)
    return resp

# query streamflow
def flowfabric_streamflow_query():
    return

# estimate size of streamflow data
def flowfabric_streamflow_estimate():
    return

# query ratings
def flowfabric_ratings_query(feature_ids, type="rem", format="arrow", token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    params = dict(feature_ids=feature_ids, type=type, format=format)
    if kwargs:
        params.update(kwargs)
    resp = flowfabric_post("/v1/ratings", body=params, token=token, verbose=verbose)
    return resp

# estimate size of ratings query data
def flowfabric_ratings_estimate(feature_ids, type="rem", format="json", token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    params = dict(feature_ids=feature_ids, type=type, format=format)
    if kwargs:
        params.update(kwargs)
    resp = flowfabric_post("/v1/ratings?estimate=TRUE", body=params, token=token, verbose=verbose)
    return resp

# query stage
# dataset_id is not being used?
def flowfabric_stage_query(dataset_id, params=None, token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    if params is None:
        params = dict(kwargs)
    resp = flowfabric_post("/v1/stage", body=params, token=token, verbose=verbose)
    return resp

# query inundation polygon grid IDs
def flowfabric_inundation_ids():
    return

# health check
def flowfabric_healthz(token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    resp = flowfabric_get("/healthz", token=token, verbose=verbose)
    return resp

# bearer token utility function
def get_bearer_token(force_refresh=False):
    token = flowfabric_get_token(force_refresh=force_refresh)['id_token']
    if token is str:
        return token
    else:
        return "No valid token found. If running in a non-interactive environment, please cache a token or set it explicitly."
