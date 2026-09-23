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

    def test_configured_non_http_url_allowed(self):
        configured_url = next(
            value for value in _read_allowed_hosts_from_file()
            if "://" in value
            and value.split("://", 1)[0].lower() not in {"http", "https", "local"}
        )
        self.assertTrue(is_allowed_url(configured_url))

    def test_known_host_allowed(self):
        self.assertTrue(is_allowed_url("https://dummyjson.com/products"))

    def test_wildcard_subdomain_allowed(self):
        self.assertTrue(is_allowed_url("https://api.dummyjson.com/products"))

    def test_unknown_host_blocked(self):
        self.assertFalse(is_allowed_url("https://example.org"))


if __name__ == "__main__":
    unittest.main()
