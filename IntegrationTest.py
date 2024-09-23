import unittest
import pandas as pd
from datetime import datetime
from QualityChecker import QualityChecker
from Transformer import Transformer

class TestQualityCheckerTransformerIntegration(unittest.TestCase):
    def test_remove_nulls_and_calculate_activity_count(self):
        # Sample data with nulls
        data = pd.DataFrame({
            'user_id': [1, 2, 3, 4],
            'activity': ['login', None, 'logout', 'login'],
            'timestamp': [datetime(2023, 9, 1), None, datetime(2023, 9, 2), datetime(2023, 9, 3)]
        })
        
        quality_checker = QualityChecker(data)
        transformer = Transformer(data)

        # Remove rows with nulls in 'activity' and 'timestamp'
        cleaned_data = quality_checker.remove_nulls(data, ['activity', 'timestamp'])
        
        # Calculate activity count
        result = transformer.calculating_activity_count(activity_column='activity', group_by_key='user_id')
        
        expected_result = pd.DataFrame({
            'user_id': [1, 3, 4],
            'total_activity_count': [1, 1, 1]
        })

        pd.testing.assert_frame_equal(result, expected_result)

if __name__ == '__main__':
    unittest.main()
