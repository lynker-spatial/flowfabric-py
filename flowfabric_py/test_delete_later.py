# random test code
# can delete when building library
import time

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
    "format": "json",
}

#print(flowfabric_post("/v1/ratings", body=params))

#print(flowfabric_ratings_query(feature_ids=["101", "1001"], format="json", scope="features", issue_time="latest", lead_start=0, lead_end=0, query_mode="run"))

#print(flowfabric_ratings_estimate(feature_ids=["101", "1001"], format="json", scope="features", issue_time="latest", lead_start=0, lead_end=0, query_mode="run"))

#print(flowfabric_post("/v1/ratings", body=params))

#print(flowfabric_list_datasets())

#print(flowfabric_get_token(force_refresh=True))

#print(flowfabric_get_dataset("nws_owp_nwm_analysis", verbose=True))

#print(flowfabric_get_latest_run("nws_owp_nwm_analysis"))

#print(flowfabric_get_run("nws_owp_nwm_analysis", issue_time="2026010514"))

#print(type(flowfabric_healthz()))

#print(flowfabric_stage_query("nws_owp_nwm_analysis", params=params))

#print(flowfabric_post("/v1/stage", body=params))

#print(auto_streamflow_params("nws_owp_nwm_analysis"))

#print(flowfabric_streamflow_estimate("nws_owp_nwm_reanalysis_3_0"))

#print(normalize_time("2018-01-01"))
#print(type(normalize_time("2018-01-01")))
#print(normalize_time("2018-01-01T00:01:02Z"))
#print(normalize_time(None))

print(flowfabric_streamflow_query("lynker_spatial_ecwmf_glofas_global", issue_time = "latest"))

time.perf_counter()
