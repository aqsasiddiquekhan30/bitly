from FileReader import FileReader
from QualityChecker import QualityChecker
from Transformer import Transformer
import pandas as pd
from Connector import MySQLConnector
from Loader import DatabaseLoader

def main():
    # Reading data from file
    file_name = 'user_activity_data.csv'
    file_reader = FileReader(file_name)
    data = file_reader.data_reader()

    # Quality checking and cleaning data
    null_columns = ['user_id', 'activity_type', 'timestamp']
    time_columns = ['timestamp']
    id_column = 'user_id'
    activity_column = 'activity_type'
    timestamp_column = 'timestamp'
    int_columns = ['user_id']

    quality_checker = QualityChecker(data)
    null_values = quality_checker.get_nulls(data, null_columns)

    quality_data = quality_checker.remove_nulls(data, null_columns)
    
    quality_data = quality_checker.convert_to_datetime(quality_data, time_columns)
    
    quality_data = quality_checker.remove_duplicate(data_frame=quality_data, id_column=id_column, activity_log_column=activity_column, timestamp_column=timestamp_column)
    quality_data = quality_checker.convert_to_int(quality_data, int_columns)
    
   
    # Transforming data 
    activity_column = 'activity_type'
    group_by_key = 'user_id'
    transformer = Transformer(quality_data)
    transformed_data = transformer.calculating_activity_count(activity_column=activity_column, group_by_key=group_by_key)
   
    
    merged_df = pd.merge(quality_data, transformed_data, on='user_id', how='inner')
   
    merged_df.to_csv('user_log.csv', index=False)  # Save the merged DataFrame to CSV
    
    # Connection to database 
    params = FileReader('params.json')
    params = params.read_json_file()
    db_connector = MySQLConnector(
        host=params.get('host'),  
        port=params.get('port'),        
        user=params.get('user'),       
        password=params.get('password'),
        database=params.get('database')  
    )

    try:
        engine = db_connector.create_connection()  # Get the SQLAlchemy engine
        loader = DatabaseLoader(merged_df)
        table_name = 'user_activity'
        loader.load_dataframe_to_table(engine, table_name)  # Use the engine here
        error_loader= DatabaseLoader(null_values)
        error_table = 'user_activity_errors'
        error_loader.load_dataframe_to_table(engine, error_table)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_connector.close_connection()
if __name__ == '__main__':
    main()
