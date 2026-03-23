import unittest

from src.api_adapter.rss import discover_rss_feed


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Football Feed</title>
    <item>
      <title>Serie A transfer race heats up</title>
      <link>https://example.com/serie-a-transfer</link>
      <description>Napoli and Milan are active in the transfer window.</description>
      <pubDate>Mon, 10 Mar 2025 08:00:00 GMT</pubDate>
      <category>Transfers</category>
    </item>
  </channel>
</rss>
"""


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self):
        return None


class _FakeClient:
    async def get(self, *_args, **_kwargs):
        return _FakeResponse(SAMPLE_RSS)


class RSSAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_discover_rss_feed_maps_expected_fields(self):
        page = await discover_rss_feed(
            _FakeClient(),
            feed_url="https://feeds.example.com/serie-a.xml",
            league="serie_a",
            feed_name="Football Feed",
        )
        self.assertEqual(len(page.data), 1)
        article = page.data[0]
        self.assertEqual(article.league, "serie_a")
        self.assertEqual(article.source_type, "rss")
        self.assertEqual(article.source_feed, "https://feeds.example.com/serie-a.xml")
        self.assertEqual(article.publisher_name, "Football Feed")
        self.assertIn("Napoli", article.excerpt)


if __name__ == "__main__":
    unittest.main()
