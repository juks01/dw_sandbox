import unittest

from extractor.app.config import is_allowed_url


class AllowedHostsTests(unittest.TestCase):
    def test_demo_url_allowed(self):
        self.assertTrue(is_allowed_url("local://demo"))

    def test_known_host_allowed(self):
        self.assertTrue(is_allowed_url("https://dummyjson.com/products"))

    def test_wildcard_subdomain_allowed(self):
        self.assertTrue(is_allowed_url("https://api.dummyjson.com/products"))

    def test_unknown_host_blocked(self):
        self.assertFalse(is_allowed_url("https://example.org"))


if __name__ == "__main__":
    unittest.main()
