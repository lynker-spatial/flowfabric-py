# test_http.py
import unittest
from src.flowfabricpy.flowfabric_http import flowfabric_get, flowfabric_post

class MyTestCase(unittest.TestCase):

    # tests flowfabric_get() on different return types
    def test_flowfabric_get(self):
        resp_1 = flowfabric_get("/v1/datasets")
        self.assertIsInstance(resp_1, list)
        resp_2 = flowfabric_get("/healthz")
        self.assertIsInstance(resp_2, dict)

    # tests flowfabric_post() on ratings query
    def test_flowfabric_post(self):
        params = {
            "query_mode": "run",
            "feature_ids": ["101", "1001"],
            "issue_time": "latest",
            "scope": "features",
            "lead_start": 0,
            "lead_end": 0,
            "format": "json"
        }
        resp = flowfabric_post("/v1/ratings", body=params)
        self.assertIsInstance(resp, dict)

if __name__ == '__main__':
    unittest.main()
