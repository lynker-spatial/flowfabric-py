# client.py
import base64
from datetime import datetime, timezone
import re
import tempfile
import json

import polars
import pyarrow.parquet as pq
import requests

from .catalog_utils import auto_streamflow_params
from .flowfabric_http import flowfabric_get, flowfabric_post
from .auth import flowfabric_get_token

# list all datasets
def flowfabric_list_datasets():
    resp = flowfabric_get("/v1/datasets", token=None)
    json_resp = resp.json()
    json_resp = [x for x in json_resp if x is not None and isinstance(x, dict) and len(x) > 0]
    if len(json_resp) == 0:
        return list()
    # add filtering
    return json_resp

# return a specific dataset
def flowfabric_get_dataset(dataset_id, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id])
    resp = flowfabric_get(endpoint, token=token, verbose=verbose)
    return resp.json()

# get latest run for a specific dataset
def flowfabric_get_latest_run(dataset_id, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id, "/runs/latest"])
    resp = flowfabric_get(endpoint, token=token, verbose=verbose)
    return resp.json()

# get a specific run for a specified dataset
def flowfabric_get_run(dataset_id, issue_time, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id, "/runs/", issue_time])
    resp = flowfabric_get(endpoint, token=token, verbose=verbose)
    return resp.json()

# query streamflow
def flowfabric_streamflow_query(dataset_id, feature_ids=None, start_time=None, end_time=None, issue_time=None, params=None, token=None, verbose=False, **kwargs):
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

    # pre-fetch metadata to detect Zarr stores
    is_zarr = False
    try:
        meta_end = "".join(["/v1/datasets/", dataset_id])
        meta_json = flowfabric_get(meta_end, token=token, verbose=verbose)
        if 'storage_type' in meta_json and meta_json['storage_type'] == "zarr":
            is_zarr = True
        elif 'config' in meta_json and 'format' in meta_json['config'] and meta_json['config']['format'] == "zarr":
            is_zarr = True
        elif 'storage' in meta_json and 'type' in meta_json['storage'] and meta_json['storage']['type'] == "zarr":
            is_zarr = True
    except requests.exceptions.Timeout:
        print({"Error": "Request timed out"})
    except requests.exceptions.RequestException as e:
        print({"Error": str(e)})

    # if dataset is zarr, avoid the presign/estimate step
    est_resp = None
    if not is_zarr:
        est_endpoint = "".join(["/v1/datasets/", dataset_id, "/streamflow?estimate=TRUE"])
        est_resp = flowfabric_post(est_endpoint, body=query_params, token=token, verbose=verbose)
    export_url = None
    if est_resp is not None and 'export_url' in est_resp:
        export_url = est_resp['export_url']

    # if export_url was provided, read directly with Arrow
    if export_url is not None and export_url != "":
        # check if data is zarr (same process as pre-check)
        is_zarr = False
        try:
            meta_end = "".join(["/v1/datasets/", dataset_id])
            meta_json = flowfabric_get(meta_end, token=token, verbose=verbose)
            if 'storage_type' in meta_json and meta_json['storage_type'] == "zarr":
                is_zarr = True
            elif 'config' in meta_json and 'format' in meta_json['config'] and meta_json['config']['format'] == "zarr":
                is_zarr = True
            elif 'storage' in meta_json and 'type' in meta_json['storage'] and meta_json['storage']['type'] == "zarr":
                is_zarr = True
        except requests.exceptions.Timeout:
            print({"Error": "Request timed out"})
        except requests.exceptions.RequestException as e:
            print({"Error": str(e)})

        # honor export_url if it looks like a materialized export
        allow_direct = True
        if is_zarr:
            allow_direct = re.compile(r"/exports/").search(export_url)
            print("".join(["[flowfabric_streamflow_query] Dataset appears to be Zarr (is_zarr = ", is_zarr, "); allow_direct = ", allow_direct])) if verbose else None

        if allow_direct:
            print("".join(["[flowfabric_streamflow_query] Server recommended export; reading export_url: ", export_url]))
            # try direct Arrow read first
            try:
                tbl = pq.read_table(export_url)
                return tbl
            except Exception as e:
                print(RuntimeError(f"Error reading Parquet file: {e} - falling back to download"))
                temp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=True)
                try:
                    response = requests.get(export_url, stream=True)
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            temp.write(chunk)
                    tbl = pq.read_table(temp)
                    return tbl
                except requests.exceptions.Timeout:
                    print("Request timed out.")
                except requests.exceptions.HTTPError as e:
                    print(f"HTTP error: {e}")
                except Exception as e:
                    print(f"Unexpected error: {e}")
        else:
            print("[flowfabric_streamflow_query] Ignoring export_url for Zarr dataset; proceeding with streaming query") if verbose else None


    # if not zarr, proceed as normal
    endpoint = "".join(["/v1/datasets/", dataset_id, "/streamflow"])
    resp = flowfabric_post(endpoint, body=query_params, token=token, verbose=verbose)
    to_json = resp.json()
    # parse based on content type of output
    content_resp = requests.get("".join(["https://flowfabric-api.lynker-spatial.com", endpoint]))
    content_type = content_resp.headers.get('Content-Type')
    arrow_stream = re.compile(r"application/vnd\\.apache\\.arrow\\.stream").search(content_type)
    json_resp = re.compile(r"application/json").search(content_type)
    if arrow_stream is not None:
        print("[flowfabric_streamflow_query] Parsing response as Arrow IPC stream.") if verbose else None
        resp_raw = resp.raw.read()
        preview_len = min(64, len(resp_raw))
        try:
            text_preview = resp_raw.decode("utf-8")[:preview_len]
            print(f"[flowfabric_streamflow_query] Raw body (text preview): {text_preview}") if verbose else None
            # if body looks like JSON, handle as base64-encoded Arrow
            if text_preview[0] == "{":
                json_obj = json.loads(resp_raw.json()) # might need to be changed to handle properly
                if 'data' in json_obj:
                    print("[flowfabric_streamflow_query] Detected base64-encoded Arrow in JSON 'data' field.") if verbose else None
                    arrow_bin = base64.b64decode(json_obj)
                    tbl = polars.read_ipc_stream(arrow_bin)
                    if 'time' in tbl.columns:
                        tbl['time'] = datetime.strptime(tbl['time'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    return tbl
                else:
                    quit("JSON response does not contain 'data' field for Arrow stream.")
            else:
                # try to parse as Arrow binary
                try:
                    tbl = polars.read_ipc_stream(resp_raw)
                    if 'time' in tbl.columns:
                        tbl['time'] = datetime.strptime(tbl['time'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    return tbl
                except Exception as e:
                    print(f"flowfabric_streamflow_query: Error reading Arrow IPC stream - {e}")
        except UnicodeDecodeError as e:
            print(f"Error decoding response: {e}")

    elif json_resp is not None:
        print("[flowfabric_streamflow_query] Parsing response as JSON.") if verbose else None
        try:
            return to_json
        except Exception as e:
            print(f"Error decoding JSON response: {e}")

    else:
        quit(f"Unsupported content type: {content_type}")

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
    return resp.json()

# query ratings
def flowfabric_ratings_query(feature_ids, type="rem", format="arrow", token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    params = dict(feature_ids=feature_ids, type=type, format=format)
    if kwargs:
        params.update(kwargs)
    resp = flowfabric_post("/v1/ratings", body=params, token=token, verbose=verbose)
    if format == "arrow":
        print("[flowfabric_ratings_query] Parsing response as Arrow IPC stream.") if verbose else None
        resp_raw = resp.raw.read()
        return polars.read_ipc_stream(resp_raw)
    else:
        print("[flowfabric_ratings_query] Parsing response as JSON.") if verbose else None
        return resp.json()

# estimate size of ratings query data
def flowfabric_ratings_estimate(feature_ids, type="rem", format="json", token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    params = dict(feature_ids=feature_ids, type=type, format=format)
    if kwargs:
        params.update(kwargs)
    resp = flowfabric_post("/v1/ratings?estimate=TRUE", body=params, token=token, verbose=verbose)
    return resp.json()

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
    resp_raw = resp.raw.read()
    return polars.read_ipc_stream(resp_raw)

# query inundation polygon grid IDs
def flowfabric_inundation_ids(params=list, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    resp = flowfabric_post("/v1/inundation-ids", body=params, token=token, verbose=verbose)
    return resp.json()

# health check
def flowfabric_healthz(token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    resp = flowfabric_get("/healthz", token=token, verbose=verbose)
    return resp.json()

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
