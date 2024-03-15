import unittest
import pandas as pd
from data_extract import sum_daily_totals

# Unit test class
class TestsumDailyTotals(unittest.TestCase):

    # Test case for successful aggregation
    def test_sum_daily_totals(self):
        # Sample input data
        data = {
            'date': ['2023-03-15', '2023-03-15', '2023-03-16', '2023-03-17'],
            'temperature_2m': [10, 15, 20, 25],
            'rain': [2, 4, 6, 8],
            'showers': [1, 2, 3, 4],
            'visibility': [100, 200, 300, 400]
        }
        df_data = pd.DataFrame(data)

        # Expected output
        expected_totals = pd.DataFrame({
            'date': ['2023-03-15', '2023-03-16'],
            'temperature_2m': [25, 45],  # Sum of temperatures for each date
            'rain': [6, 14],             # Sum of rain for each date
            'showers': [3, 7],           # Sum of showers for each date
            'visibility': [300, 700]     # Sum of visibility for each date
        })

        # Call the function
        result = sum_daily_totals(df_data)

        # Assert that the result matches the expected output
        pd.testing.assert_frame_equal(result, expected_totals)

if __name__ == '__main__':
    unittest.main()
