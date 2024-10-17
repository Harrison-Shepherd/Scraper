import unittest
from unittest.mock import patch, MagicMock
from Core.FixtureDetails import Fixture

class TestFixtureDetails(unittest.TestCase): #TODO FIX
    @patch('Core.FixtureDetails.requests.get')
    def test_fetch_data_netball_womens_nz(self, mock_get):
        # Mock the API response for a Netball Women's NZ fixture
        mock_response = {
            'fixture': {
                'match': [
                    {'matchId': 80121405, 'homeSquadId': 71, 'awaySquadId': 72, 'matchStatus': 'completed'}
                ]
            }
        }
        
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = mock_response
        
        fixture = Fixture(8005, 801214, 4)  # Example for Netball NZ Women's league
        fixture.fetch_data()

        # Assert that the data was processed correctly
        self.assertEqual(len(fixture.data), 1)  # 1 match in the fixture
        self.assertEqual(fixture.data.iloc[0]['matchId'], 80121405)
        self.assertEqual(fixture.data.iloc[0]['homeSquadId'], 71)
        self.assertEqual(fixture.data.iloc[0]['awaySquadId'], 72)

if __name__ == '__main__':
    unittest.main()
