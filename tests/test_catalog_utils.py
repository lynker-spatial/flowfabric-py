import unittest
from flowfabric_py.catalog_utils import auto_streamflow_params

class MyTestCase(unittest.TestCase):
    # test auto_streamflow_params() comes back with a dictionary
    def test_auto_streamflow_params(self):
        params_run = auto_streamflow_params("nws_owp_nwm_analysis")
        self.assertIsInstance(params_run, dict)
        params_absolute = auto_streamflow_params("nws_owp_nwm_reanalysis_3_0")
        self.assertIsInstance(params_absolute, dict)

if __name__ == '__main__':
    unittest.main()
