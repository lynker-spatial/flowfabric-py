# client.py
import re

from .catalog_utils import auto_streamflow_params
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
def flowfabric_streamflow_estimate(dataset_id, feature_ids=None, start_time=None, end_time=None, issue_time=None, params=None, token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    if params is not None:
        query_params = dict(params)
    else:
        query_params = dict()
        query_params.update(kwargs)
        query_params['feature_ids'] = feature_ids if feature_ids is not None else None
        query_params['start_time'] = normalize_time(start_time) if start_time is not None else None
        query_params['end_time'] = normalize_time(end_time) if end_time is not None else None
        query_params['issue_time'] = normalize_time(issue_time) if issue_time is not None else None
        auto_params = auto_streamflow_params(dataset_id)
        for name in auto_params:
            query_params[name] = auto_params[name] if name in auto_params else query_params[name] if name in query_params else None
        if len(query_params) == 0:
            query_params = auto_params
    endpoint = "".join(["/v1/datasets/", dataset_id, "/streamflow?estimate=TRUE"])
    resp = flowfabric_post(endpoint, body=query_params, token=token, verbose=verbose)
    return resp

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
def flowfabric_stage_query(dataset_id, params=None, token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    if params is None:
        params = dict(kwargs)
    if params is None:
        params = auto_streamflow_params(dataset_id)
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

# helper function to normalize time
def normalize_time(obj, is_start=True):
    if obj is None:
        return None
    if 'T' in obj and 'Z$' in obj:
        return obj

    date = re.findall(r'(\d{4}-\d{2}-\d{2}$)', obj)
    for day in date:
        if is_start:
            date[date.index(day)] = day + "T00:00:00Z"
        else:
            date[date.index(day)] = day + "T23:59:59Z"
        return date[0] # acceptable because only one date is being passed into this function at a time

    return obj
