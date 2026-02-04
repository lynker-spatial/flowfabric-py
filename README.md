\# flowfabricpy: Effortless Python Access to FlowFabric API



`flowfabricpy` is a powerful Python client for the FlowFabric API, providing seamless access to hydrologic forecasts, reanalysis, rating curves, and datasets. With robust authentication and automatic token caching, you can focus on data science, not on data munging plumbing.



\## Key Features

\- \*\*Evolving catalog\*\* of harmonized data from multiple models

\- \*\*Single method access\*\* to all models - both retrospective and forecast

\- \*\*One-time authentication\*\*: Log in once, and your token is cached for future use.

\- \*\*Arrow IPC support\*\*: Fast, memory-efficient data transfer


## Troubleshooting
- If you see repeated browser prompts, call `flowfabric_refresh_token()` once, then retry your queries.
- If you switch users, manually refresh the token.
- Use `verbose = True` in any endpoint for detailed debug output.

## Learn More
- See the vignettes for advanced usage, authentication, and custom queries.
- All API responses are Arrow tables for high-performance analytics.

