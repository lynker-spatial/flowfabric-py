# random test code
# can delete when building library
import time
import pyarrow.parquet as pq
import polars
import io

from flowfabric_py import auto_streamflow_params
from flowfabric_py.flowfabric_http import flowfabric_post, flowfabric_get
from flowfabric_py.auth import flowfabric_get_token, flowfabric_refresh_token
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

start_time = time.perf_counter()

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

#datasets = [dataset for dataset in flowfabric_list_datasets()]

#print(flowfabric_post("/v1/ratings", body=params))

#print(flowfabric_ratings_query(feature_ids=["101", "1001"], format="json", scope="features", issue_time="latest", lead_start=0, lead_end=0, query_mode="run"))

#print(flowfabric_ratings_estimate(feature_ids=["101", "1001"], format="arrow", scope="features", issue_time="latest", lead_start=0, lead_end=0, query_mode="run", verbose=True))

#print(flowfabric_post("/v1/ratings", body=params))

#print(flowfabric_list_datasets())

#print(flowfabric_get_token(force_refresh=True))

#print(flowfabric_get_dataset("usgs_nwis_stage"))

#print(flowfabric_get_latest_run("nws_owp_nwm_analysis"))

#print(flowfabric_get_run("nws_owp_nwm_analysis", issue_time="2026010514"))

#print(flowfabric_healthz())

print(flowfabric_refresh_token())

#print(flowfabric_stage_query("usgs_nwis_stage", params=params))

#params['issue_time'] = "2026012316"
#for dataset in flowfabric_list_datasets():
#    params['dataset_id'] = dataset['name']
#    print(flowfabric_inundation_ids(params))

#print(flowfabric_post("/v1/stage", body=params))

#print(auto_streamflow_params("nws_owp_nwm_analysis"))
#print(flowfabric_streamflow_estimate("lynker_spatial_ecwmf_glofas_global", params=auto_streamflow_params("lynker_spatial_ecwmf_glofas_global")))

#print(normalize_time("2018-01-01"))
#print(type(normalize_time("2018-01-01")))
#print(normalize_time("2018-01-01T00:01:02Z"))
#print(normalize_time(None))

#x = flowfabric_streamflow_query("lynker_spatial_ecwmf_glofas_global", params=None)
#print(x)

#resp = flowfabric_post("/v1/datasets/lynker_spatial_ecwmf_glofas_global/streamflow", body=params)
#print(polars.read_ipc_stream(resp.content))

#for dataset in datasets:
#    print((auto_streamflow_params(dataset['name'])))

#print(time.localtime(flowfabric_get_token()['expires_at']))

#print(flowfabric_get_token())

end_time = time.perf_counter()
#print(end_time - start_time)
