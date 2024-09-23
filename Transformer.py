import pandas as pd
# class to transform the data and apply aggregations

class Transformer:
    def __init__(self, data):
        self.data = data
    
    def calculating_activity_count(self, activity_column, group_by_key):
        # Group by the specified key and count the activities
        activity_count = self.data.groupby(group_by_key)[activity_column].count().reset_index()
        
        # Rename the columns for clarity
        activity_count.columns = [group_by_key, 'total_activity_count']
        
        return activity_count
    
        