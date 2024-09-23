import pandas as pd

class QualityChecker:
    
    def __init__(self, data):
        self.data = data
    
    # converting timestamp to datetime 
    
    def convert_to_datetime(self, data_frame, columns):
        for column in columns:
            try:
                data_frame[column]  = pd.to_datetime(data_frame[column])
            except:
                raise Exception(f'Unable to convert {column} to datetime')    
        return data_frame
    
    # removong null values
    
    def remove_nulls(self, data_frame, columns):
        try:
            data_frame.dropna(subset=columns, inplace=True)
        except Exception as e:
            raise Exception(f'Unable to remove nulls from columns {columns}: {e}')
        return data_frame
    
    # storing all the null values in other dataframe
    
    def get_nulls(self, data_frame, columns):
        try:
            data_frame = data_frame[data_frame[columns].isna().any(axis=1)] 
            return data_frame
        except:
            print('no nulls found in data')
    
    # converting float values to integer
    
    def convert_to_int(self, data_frame, columns):
        for column in columns:
            try:
                data_frame[column] = data_frame[column].astype(int)
            except:
                raise Exception(f'Unable to convert {column} to int')
        return data_frame
    
    # removing duplicates

    def remove_duplicate(self, data_frame, id_column, activity_log_column, timestamp_column, tolerance=60):
    # Check if timestamp column exists in the DataFrame
        if timestamp_column in data_frame.columns:
            # Sort the data by user_id and timestamp to ensure chronological order within each user
            data_frame = data_frame.sort_values(by=[id_column, timestamp_column])

            # Initialize an 'is_duplicate' column with False
            data_frame['is_duplicate'] = False

            # Calculate time difference between consecutive rows per user
            data_frame['time_diff'] = data_frame.groupby(id_column)[timestamp_column].diff().dt.total_seconds()

            # Check for duplicates based on user_id, activity_log, and time tolerance
            duplicate_condition = (
                (data_frame[id_column] == data_frame[id_column].shift(1)) &  # Same user
                (data_frame[activity_log_column] == data_frame[activity_log_column].shift(1)) &  # Same activity
                (data_frame['time_diff'] >= 0) &  # Ensure time_diff is positive
                (data_frame['time_diff'] <= tolerance)  # Within tolerance
            )

            # Mark rows that meet the duplicate condition
            data_frame.loc[duplicate_condition, 'is_duplicate'] = True

            # Remove duplicates and drop the temporary columns
            cleaned_df = data_frame[~data_frame['is_duplicate']].drop(columns=['is_duplicate', 'time_diff'])
            return cleaned_df
        else:
            # If no timestamp column is provided, drop duplicates based only on user_id and activity_log
            print("Timestamp column is missing. Checking duplicates based on user_id and activity_log only.")
            return data_frame.drop_duplicates(subset=[id_column, activity_log_column])
