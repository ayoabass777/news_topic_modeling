import unittest

from src.utils.football import classify_signal, is_football_article


class FootballUtilsTest(unittest.TestCase):
    def test_classify_signal_identifies_transfers(self):
        signal = classify_signal("Juventus complete transfer deal", "medical booked")
        self.assertEqual(signal, "transfers")

    def test_classify_signal_identifies_trends(self):
        signal = classify_signal("Premier League title race trend", "table momentum")
        self.assertEqual(signal, "trends")

    def test_is_football_article_uses_league_metadata(self):
        self.assertTrue(is_football_article({"league": "premier_league"}))

    def test_is_football_article_uses_text_fallback(self):
        self.assertTrue(is_football_article({"title": "Arsenal push in Premier League race"}))


if __name__ == "__main__":
    unittest.main()
