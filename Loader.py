# class to load dataframe to SQL table

class DatabaseLoader:
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def load_dataframe_to_table(self, engine, table_name):
        if self.data_frame.empty:
            raise Exception(f'No data found in the DataFrame.')

        try:
            self.data_frame.to_sql(table_name, con=engine, if_exists='replace', index=False)
            num_rows = len(self.data_frame)
            print(f'Rows {num_rows} inserted into the {table_name} table.')
        except Exception as e:
            print(f'An error occurred: {e}')
