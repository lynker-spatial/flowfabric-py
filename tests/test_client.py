import unittest
from flowfabric_py.client import (
    flowfabric_list_datasets,
    flowfabric_get_dataset,
    flowfabric_get_latest_run,
    flowfabric_streamflow_query,
    flowfabric_streamflow_estimate,
    flowfabric_ratings_query,
    flowfabric_ratings_estimate,
    flowfabric_stage_query,
    flowfabric_healthz,
    flowfabric_inundation_ids,
    get_bearer_token,
    normalize_time,
)

class MyTestCase(unittest.TestCase):
    # ensures flowfabric_list_datasets() returns a list
    def test_flowfabric_list_datasets(self):
        datasets = flowfabric_list_datasets()
        self.assertIsInstance(datasets, list)

    # tests if flowfabric_get_dataset() returns a dict
    def test_flowfabric_get_dataset(self):
        dataset = flowfabric_get_dataset("nws_owp_nwm_analysis")
        self.assertIsInstance(dataset, dict)

    # tests if flowfabric_get_latest_run() returns a dict
    def test_flowfabric_get_latest_run(self):
        run = flowfabric_get_latest_run("nws_owp_nwm_analysis")
        self.assertIsInstance(run, dict)

    # test if flowfabric_streamflow_estimate returns a dict
    def test_flowfabric_streamflow_estimate(self):
        estimate = flowfabric_streamflow_estimate("nws_owp_nwm_analysis")
        self.assertIsInstance(estimate, dict)

    # tests if flowfabric_ratings_query() returns a dict
    def test_flowfabric_ratings_query(self):
        ratings = flowfabric_ratings_query(["101", "1001"])
        self.assertIsInstance(ratings, dict)

    # test if flowfabric_ratings_estimate() returns a dict
    def test_flowfabric_ratings_estimate(self):
        ratings = flowfabric_ratings_estimate(["101", "1001"])
        self.assertIsInstance(ratings, dict)

    # test if flowfabric_stage_query() returns a dict
    def test_flowfabric_stage_query(self):
        stages = flowfabric_stage_query("usgs_nwis_stage")
        self.assertIsInstance(stages, dict)

    # test if flowfabric_healthz() returns a dict
    def test_flowfabric_healthz(self):
        health = flowfabric_healthz()
        self.assertIsInstance(health, dict)

    # test if get_bearer_token() returns a str
    def test_get_bearer_token(self):
        token = get_bearer_token()
        self.assertIsInstance(token, str)

    # test normalize_time() helper function for returns
    def test_normalize_time(self):
        time1 = "2018-01-01"
        self.assertEqual(normalize_time(time1), "2018-01-01T00:00:00Z")
        time2 = "2018-01-01T22:00:00Z"
        self.assertEqual(normalize_time(time2), "2018-01-01T22:00:00Z")
        time3 = None
        self.assertIsNone(normalize_time(time3))
        time4 = "2028-01-01"
        self.assertEqual(normalize_time(time4, is_start=False), "2028-01-01T23:59:59Z")

if __name__ == '__main__':
    unittest.main()
