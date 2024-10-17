import unittest
from unittest.mock import patch, MagicMock
from Core.MatchDetails import Match

class TestMatchDetails(unittest.TestCase): #TODO FIX
    @patch('Core.MatchDetails.requests.get')
    def test_fetch_data_netball_womens_nz(self, mock_get):
        # Updated mock response to include playerInfo
        mock_response = {
            'matchStats': {
                'playerStats': {
                    'player': [
                        {'playerId': 1, 'squadId': 71, 'playerName': 'Player One'},
                        {'playerId': 2, 'squadId': 72, 'playerName': 'Player Two'}
                    ]
                },
                'teamInfo': {'team': [{'squadId': 71, 'squadName': 'Team One'}, {'squadId': 72, 'squadName': 'Team Two'}]},
                'matchInfo': {'homeSquadId': 71, 'awaySquadId': 72, 'roundNumber': 1}
            },
            'playerInfo': {  # Added playerInfo here
                'player': [
                    {'playerId': 1, 'firstname': 'John', 'surname': 'Doe'},
                    {'playerId': 2, 'firstname': 'Jane', 'surname': 'Doe'}
                ]
            }
        }
        
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = mock_response
        
        match = Match(8005, 80121405, 801214, 8)  # Example League/Match/Fixture/Sport ID for Netball NZ Women's
        match.fetch_data()

        # Assert that the data was correctly processed
        self.assertEqual(len(match.data), 2)  # 2 players
        self.assertIn('uniquePlayerId', match.data.columns)
        self.assertEqual(match.data.iloc[0]['playerId'], 1)
        self.assertEqual(match.data.iloc[0]['squadName'], 'Team One')
        self.assertEqual(match.data.iloc[1]['playerId'], 2)
        self.assertEqual(match.data.iloc[1]['squadName'], 'Team Two')

if __name__ == '__main__':
    unittest.main()
