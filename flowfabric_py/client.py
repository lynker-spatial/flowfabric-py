# client.py
import base64
import re
import tempfile
import io
import polars
import pyarrow.parquet as pq
import requests
from pyarrow import ArrowInvalid

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
    resp = flowfabric_get(endpoint, verbose=verbose)
    return resp.json() # might need to be resp.content.json()

# get latest run for a specific dataset
def flowfabric_get_latest_run(dataset_id, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id, "/runs/latest"])
    resp = flowfabric_get(endpoint, verbose=verbose)
    return resp.json() # might need to be resp.content.json()

# get a specific run for a specified dataset
def flowfabric_get_run(dataset_id, issue_time, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    endpoint = "".join(["/v1/datasets/", dataset_id, "/runs/", issue_time])
    resp = flowfabric_get(endpoint, verbose=verbose)
    return resp.json() # might need to be resp.content.json()

# query streamflow
def flowfabric_streamflow_query(dataset_id, feature_ids=None, start_time=None, end_time=None, issue_time=None, params=None, token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    if params is not None:
        query_params = {k: v for k, v in params.items() if v is not None}
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
    meta_json = None
    try:
        meta_end = "".join(["/v1/datasets/", dataset_id])
        meta_json = flowfabric_get(meta_end, token=token, verbose=verbose)
        is_zarr = is_zarr_dataset(meta_json)
    except requests.exceptions.Timeout:
        print({"Error": "Request timed out"})
    except requests.exceptions.RequestException as e:
        print({"Error": str(e)})

    # if dataset is zarr, avoid the presign/estimate step
    est_resp = None
    if not is_zarr:
        est_endpoint = "".join(["/v1/datasets/", dataset_id, "/streamflow?estimate=TRUE"])
        est_resp = flowfabric_post(est_endpoint, body=query_params, verbose=verbose)
    export_url = None
    if est_resp is not None and 'export_url' in est_resp:
        export_url = est_resp['export_url']

    # if export_url was provided, read directly with Arrow
    if export_url is not None and export_url != "":
        # check if data is zarr (same process as pre-check)
        is_zarr = False
        try:
            is_zarr = is_zarr_dataset(export_url)
        except requests.exceptions.Timeout:
            raise RuntimeError({"Error": "Request timed out"})
        except requests.exceptions.RequestException as e:
            raise RuntimeError({"Error": str(e)})

        # honor export_url if it looks like a materialized export
        allow_direct = True
        if is_zarr:
            allow_direct = bool(re.search(r"/exports/", export_url))
            print("".join(["[flowfabric_streamflow_query] Dataset appears to be Zarr (is_zarr = ", is_zarr, "); allow_direct = ", allow_direct])) if verbose else None

        if allow_direct:
            print("".join(["[flowfabric_streamflow_query] Server recommended export; reading export_url: ", export_url]))
            # try direct Arrow read first
            try:
                tbl = pq.read_table(export_url)
                return tbl
            except (OSError, ArrowInvalid) as err:
                raise RuntimeError(f"Error reading Parquet file: {err}")
            except Exception as e:
                print(RuntimeError(f"Error reading Parquet file: {e} - falling back to download"))
                temp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=True)
                try:
                    response = requests.get(export_url, stream=True)
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            temp.write(chunk)
                    temp.flush()
                    temp.seek(0)
                    tbl = pq.read_table(temp)
                    return tbl
                except requests.exceptions.Timeout:
                    raise RuntimeError("Request timed out.")
                except requests.exceptions.HTTPError as e:
                    raise RuntimeError(f"HTTP error: {e}")
                except Exception as e:
                    raise RuntimeError(f"Unexpected error: {e}")
        else:
            print("[flowfabric_streamflow_query] Ignoring export_url for Zarr dataset; proceeding with streaming query") if verbose else None


    # if not zarr, proceed as normal
    endpoint = "".join(["/v1/datasets/", dataset_id, "/streamflow"])
    query_params = {k: v for k, v in query_params.items() if v is not None}
    resp = flowfabric_post(endpoint, body=query_params, verbose=verbose)

    # parse based on content type of output
    content_type = resp.headers.get("Content-Type", "")
    arrow_stream = "application/vnd.apache.arrow.stream" in content_type
    json_resp = "application/json" in content_type
    magic = resp.content[:6]

    # handle Parquet response
    if magic.startswith(b"PAR1"):
         return polars.from_arrow(pq.read_table(io.BytesIO(resp.content)))

    # handle Arrow IPC stream
    if arrow_stream:
        print("[flowfabric_streamflow_query] Parsing response as Arrow IPC stream.") if verbose else None
        ipc_resp = polars.read_ipc_stream(resp.content)
        try:
            text_preview = ipc_resp.head()
            print(f"[flowfabric_streamflow_query] Raw body (text preview): {text_preview}") if verbose else None
            # if body looks like JSON, handle as base64-encoded Arrow
            if ipc_resp.height > 0 and ipc_resp.width > 0:
                first_val = ipc_resp.row(0)[0]
                if isinstance(first_val, str) and first_val.startswith("{"):
                    to_json = resp.content.json()
                    if 'data' in to_json:
                        print("[flowfabric_streamflow_query] Detected base64-encoded Arrow in JSON 'data' field.") if verbose else None
                        arrow_bin = base64.b64decode(to_json["data"])
                        tbl = polars.read_ipc_stream(arrow_bin)
                        if 'time' in tbl.columns:
                            tbl = tbl.with_columns(
                                polars.col("time")
                                .str.strptime(polars.Datetime, "%Y-%m-%d %H:%M:%S")
                                .dt.replace_time_zone("UTC")
                            )
                        return tbl
                    else:
                        raise RuntimeError("JSON response does not contain 'data' field for Arrow stream.")
                else:
                    # try to parse as Arrow binary
                    try:
                        if 'time' in ipc_resp.columns:
                            ipc_resp = ipc_resp.with_columns(
                                polars.col("time")
                                .str.strptime(polars.Datetime, "%Y-%m-%d %H:%M:%S")
                                .dt.replace_time_zone("UTC")
                            )
                        return ipc_resp
                    except Exception as e:
                        raise RuntimeError(f"flowfabric_streamflow_query: Error reading Arrow IPC stream - {e}")
        except UnicodeDecodeError as e:
            raise RuntimeError(f"Error decoding response: {e}")

    elif json_resp:
        print("[flowfabric_streamflow_query] Parsing response as JSON.") if verbose else None
        try:
            to_json = resp.json()
            return to_json
        except Exception as e:
            raise RuntimeError(f"Error decoding JSON response: {e}")

    else:
        raise RuntimeError(f"Unsupported content type: {content_type}")

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
    resp = flowfabric_post(endpoint, body=query_params, verbose=verbose)
    return resp.json()

# query ratings
def flowfabric_ratings_query(feature_ids, type="rem", format="arrow", token=None, verbose=False, **kwargs):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    params = dict(feature_ids=feature_ids, type=type, format=format)
    if kwargs:
        params.update(kwargs)
    resp = flowfabric_post("/v1/ratings", body=params, verbose=verbose)
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
    resp = flowfabric_post("/v1/ratings?estimate=TRUE", body=params, verbose=verbose)
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
    params['dataset_id'] = dataset_id
    resp = flowfabric_post("/v1/stage", body=params, verbose=verbose)
    print(resp)
    print(resp.content)
    return polars.read_ipc_stream(resp.content)

# query inundation polygon grid IDs
def flowfabric_inundation_ids(params=list, token=None, verbose=False):
    if token is None:
        print("No token provided, using token from get_bearer_token().") if verbose else None
        token = get_bearer_token()
    resp = flowfabric_post("/v1/inundation-ids", body=params, verbose=verbose)
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

# helper function to detect zarr
def is_zarr_dataset(meta):
    return (
        meta.get("storage_type") == "zarr"
        or meta.get("config", {}).get("format") == "zarr"
        or meta.get("storage", {}).get("type") == "zarr"
    )
