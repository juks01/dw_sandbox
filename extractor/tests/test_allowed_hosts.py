import unittest

from extractor.app.config import (
    _read_allowed_hosts_from_file,
    is_allowed_url,
)


class AllowedHostsTests(unittest.TestCase):
    def test_configured_url_allowed(self):
        configured_urls = [
            value for value in _read_allowed_hosts_from_file()
            if "://" in value
        ]
        self.assertTrue(configured_urls)
        self.assertTrue(is_allowed_url(configured_urls[0]))

    def test_local_demo_url_allowed(self):
        self.assertIn("local://demo", _read_allowed_hosts_from_file())
        self.assertTrue(is_allowed_url("local://demo"))

    def test_other_local_url_blocked(self):
        self.assertFalse(is_allowed_url("local://other"))

    def test_unsupported_scheme_blocked(self):
        self.assertFalse(is_allowed_url("ftp://dummyjson.com/products"))

    def test_known_host_allowed(self):
        self.assertTrue(is_allowed_url("https://dummyjson.com/products"))

    def test_wildcard_subdomain_allowed(self):
        self.assertTrue(is_allowed_url("https://api.dummyjson.com/products"))

    def test_unknown_host_blocked(self):
        self.assertFalse(is_allowed_url("https://example.org"))

    def test_unrelated_subdomain_blocked(self):
        self.assertFalse(is_allowed_url("https://dummyjson.com.example.org/products"))


if __name__ == "__main__":
    unittest.main()
