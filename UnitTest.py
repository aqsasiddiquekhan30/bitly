import unittest
import pandas as pd
from FileReader import FileReader
from Transformer import Transformer
from unittest.mock import patch
from io import StringIO


class TestFileReader(unittest.TestCase):
    @patch('pandas.read_csv')
    def test_read_csv_file(self, mock_read_csv):
        # Mock data
        mock_data = pd.DataFrame({'user_id': ['1', '2'], 'acitivity_type': ['logout', 'login']})
        mock_read_csv.return_value = mock_data
        
        file_reader = FileReader(file_name='dummy.csv')
        result = file_reader.read_csv_file()

        # Assert that the returned result is the same as mock data
        pd.testing.assert_frame_equal(result, mock_data)
        mock_read_csv.assert_called_once_with('dummy.csv')


    def test_calculating_activity_count(self):
        # Sample data
        data = pd.DataFrame({
            'user_id': [1, 1, 2, 2, 3],
            'activity_type': ['login', 'logout', 'login', 'login', 'logout']
        })
        transformer = Transformer(data=data)
        
        result = transformer.calculating_activity_count(activity_column='activity_type', group_by_key='user_id')

        expected_result = pd.DataFrame({
            'user_id': [1, 2, 3],
            'total_activity_count': [2, 2, 1]
        })

        # Assert that the calculated activity count is correct
        pd.testing.assert_frame_equal(result, expected_result)

if __name__ == '__main__':
    unittest.main()
