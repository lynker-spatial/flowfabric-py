# random test code
# can delete when building library
import time
import pyarrow.parquet as pq
import polars
import io

from flowfabric_py import auto_streamflow_params
from flowfabric_py.flowfabric_http import flowfabric_post, flowfabric_get
from flowfabric_py.auth import flowfabric_get_token
from flowfabric_py.client import (
    flowfabric_list_datasets,
    flowfabric_get_dataset,
    flowfabric_get_latest_run,
    flowfabric_get_run,
    flowfabric_streamflow_query,
    flowfabric_streamflow_estimate,
    flowfabric_ratings_query,
    flowfabric_ratings_estimate,
    flowfabric_stage_query,
    flowfabric_healthz,
    flowfabric_inundation_ids, normalize_time,
)

time.perf_counter()

#print(flowfabric_get("/v1/datasets/"))

params = {
    "query_mode": "run",
    "feature_ids": ["101", "1001"],
    "issue_time": "latest",
    "scope": "features",
    "lead_start": 0,
    "lead_end": 0,
    "format": "arrow",
}

#print(flowfabric_post("/v1/ratings", body=params))

#print(flowfabric_ratings_query(feature_ids=["101", "1001"], format="json", scope="features", issue_time="latest", lead_start=0, lead_end=0, query_mode="run"))

#print(flowfabric_ratings_estimate(feature_ids=["101", "1001"], format="json", scope="features", issue_time="latest", lead_start=0, lead_end=0, query_mode="run"))

#print(flowfabric_post("/v1/ratings", body=params))

#print(flowfabric_list_datasets())

#print(flowfabric_get_token(force_refresh=True))

print(flowfabric_get_dataset("nws_owp_nwm_reanalysis_3_0"))

#print(flowfabric_get_latest_run("lynker_spatial_ecwmf_glofas_global"))

#print(flowfabric_get_run("lynker_spatial_ecwmf_glofas_global", issue_time="2026010514"))

#print(flowfabric_healthz())

#print(flowfabric_stage_query("nws_owp_nwm_analysis", params=params))

#params['dataset_id'] = "awi_nrds_short_range"
#params['issue_time'] = "2026012316"
#print(flowfabric_inundation_ids(params))

#print(flowfabric_post("/v1/stage", body=params))

#print(auto_streamflow_params("nws_owp_nwm_analysis"))

#print(flowfabric_streamflow_estimate("lynker_spatial_ecwmf_glofas_global", params=params))

#print(normalize_time("2018-01-01"))
#print(type(normalize_time("2018-01-01")))
#print(normalize_time("2018-01-01T00:01:02Z"))
#print(normalize_time(None))

#print(flowfabric_streamflow_query("lynker_spatial_ecwmf_glofas_global", params = params))

#resp = flowfabric_post("/v1/datasets/lynker_spatial_ecwmf_glofas_global/streamflow", body=params)
#print(polars.read_ipc_stream(resp.content))

#(auto_streamflow_params("lynker_spatial_ecwmf_glofas_global"))

#print(time.localtime(flowfabric_get_token()['expires_at']))

time.perf_counter()
