import pandas as pd
import json 
import os

# class to read the files
class FileReader:
    def __init__(self, file_name):
        self.file_name= file_name 
        _, self.file_extension = os.path.splitext(file_name)
        self.file_extension = self.file_extension.lower()

# function to read csv file

    def read_csv_file(self):
        try:
            data = pd.read_csv(self.file_name, )
            return data
        except Exception as e:
            raise Exception(e)

# function to read excel file

    def read_excel_file(self):
        try:
            data = pd.read_excel(self.file_name)
            return data
        except Exception as e: 
            raise Exception(e)
# functiin to read json file
   
    def read_json_file(self):
        try:
            with open(self.file_name, 'r') as file:
                data = json.load(file)
                return data
        except Exception as e:
            raise Exception(e)
        
# function to process reading the file
  
    def data_reader(self):
        if self.file_extension == '.csv':
            return self.read_csv_file()
        elif self.file_extension == '.xlsx':
            return self.read_excel_file()
        elif self.file_extension == '.json':
            return self.read_json_file()
        else:
            raise ValueError("f Unsupported file type: {self.file_extension}") 
        


