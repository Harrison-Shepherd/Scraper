import unittest
from unittest.mock import patch, MagicMock
from Core.Scraper import Scraper
from Core.FixtureDetails import Fixture
from Core.MatchDetails import Match

class TestScraper(unittest.TestCase):
    @patch('Core.FixtureDetails.Fixture.fetch_data')
    @patch('Core.MatchDetails.Match.fetch_data')
    @patch('Core.MatchDetails.PeriodData.fetch_data')
    @patch('Core.MatchDetails.ScoreFlow.fetch_data')
    def test_scrape_netball_womens_nz(self, mock_scoreflow, mock_perioddata, mock_matchdata, mock_fixturedata):
        # Mock FixtureData and MatchData calls to simulate Netball Women's NZ data processing
        mock_fixturedata.return_value = None  # Mocking fetch_data in Fixture
        mock_matchdata.return_value = None  # Mocking fetch_data in Match
        mock_perioddata.return_value = None  # Mocking fetch_data in PeriodData
        mock_scoreflow.return_value = None  # Mocking fetch_data in ScoreFlow

        scraper = Scraper()

        # Mock the database connection
        scraper.connection = MagicMock()
        scraper.db_helper = MagicMock()

        # Run the scraper function for Netball NZ Women's
        scraper.scrape_entire_database()

        # Ensure all fetch_data methods were called
        mock_fixturedata.assert_called_once()
        mock_matchdata.assert_called_once()
        mock_perioddata.assert_called_once()
        mock_scoreflow.assert_called_once()

        # Ensure data is processed and inserted into the DB dynamically
        scraper.db_helper.insert_data_dynamically.assert_called()

if __name__ == '__main__':
    unittest.main()
