# flowfabricpy: Effortless Python Access to FlowFabric API

`flowfabricpy` is a powerful Python client for the FlowFabric API, providing seamless access to hydrologic forecasts, reanalysis, rating curves, and datasets. With robust authentication and automatic token caching, you can focus on data science, not on data munging plumbing.

## Key Features
- **Evolving catalog** of harmonized data from multiple models
- **Single method access** to all models - both retrospective and forecast
- **One-time authentication**: Log in once, and your token is cached for future use.
- **Arrow IPC support**: Fast, memory-efficient data transfer

## Installation
```py
#  Install from GitHub:
pip install git+https://github.com/lynker-spatial/flowfabric-py#egg=flowfabricpy
```

## Quick Start
```py
# 1. List available datasets
datasets = flowfabric_list_datasets()
print(datasets)

# 2. Query streamflow forecast (first call prompts login, then caches token)
# More on atuhentication below ...
tbl = flowfabric_streamflow_query(
	dataset_id = "nws_owp_nwm_analysis",
	feature_ids = ["101", "1001"],
	issue_time = "latest"
)
print(tbl)

# 3. Query streamflow reanalysis data
tbl_re = flowfabric_streamflow_query(
  "nws_owp_nwm_reanalysis_3_0",
  feature_ids = ["101", "1001"],
  start_time = "2018-01-01",
  end_time = "2018-01-31"
)
print(tbl_re)

# 4. Query ratings
ratings = flowfabric_ratings_query(
    feature_ids = ["101", "1001"], 
    type = "rem"
)
print(ratings)
```

## Authentication & Token Caching

Using this platform requires authentication via the Lynker Spatial Portal. Accounts are free to set up and use. If use exceeds costs we can burden we will reach out to power users to better understand how we can help.

To get access, users can create an account here: https://proxy.lynker-spatial.com/

The first API call will prompt you to log in via browser. Your token is then cached and reused for all future calls—no repeated browser prompts!

**Manual token refresh:**
```py
flowfabric_refresh_token() # Forces re-authentication and updates cached token
```

**Advanced:**
You can always pass a token explicitly:
```py
token = flowfabric_get_token()['id_token']
healthz = flowfabric_healthz(token = token)
```
## Troubleshooting
- If you see repeated browser prompts, call `flowfabric_refresh_token()` once, then retry your queries.
- If you switch users, manually refresh the token.
- Use `verbose = TRUE` in any endpoint for detailed debug output.

## Learn More
- See the vignettes for advanced usage, authentication, and custom queries.
- All API responses are Arrow tables for high-performance analytics.
