import unittest
from flowfabric_py.auth import flowfabric_get_token

class MyTestCase(unittest.TestCase):
    # tests that flowfabric_get_token() returns a dict
    def test_flowfabric_get_token(self):
        token = flowfabric_get_token()
        self.assertIsInstance(token, dict)

if __name__ == '__main__':
    unittest.main()
