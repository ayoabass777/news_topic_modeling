import unittest

from web_app import filter_articles


class WebFilterTest(unittest.TestCase):
    def setUp(self):
        self.articles = [
            {
                "title": "Premier League title race",
                "league": "premier_league",
                "signal_type": "trends",
                "publisher_name": "Guardian",
                "published_date": "2026-03-01T10:00:00+00:00",
            },
            {
                "title": "Serie A transfer update",
                "league": "serie_a",
                "signal_type": "transfers",
                "publisher_name": "BBC Sport",
                "published_date": "2026-03-03T10:00:00+00:00",
            },
        ]

    def test_filter_articles_by_league_and_signal(self):
        filtered = filter_articles(self.articles, league="serie_a", signal_type="transfers")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["league"], "serie_a")

    def test_filter_articles_by_date_window(self):
        filtered = filter_articles(self.articles, date_from="2026-03-02", date_to="2026-03-04")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["league"], "serie_a")


if __name__ == "__main__":
    unittest.main()
