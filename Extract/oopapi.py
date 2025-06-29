# ============= oopapi.py =============
from Extract.rule_extract import RuleExtract
import requests
import pandas as pd

class ExtractAPI(RuleExtract):
    
    def __init__(self, api):
        self.api = api
        self.datetime = ['วันเกิด', 'วันที่เข้ารักษา', 'วันที่จำหน่าย']
        
    def extract(self):
        """Returns (rows, header) tuple for consistency"""
        response = requests.get(self.api)
        
        if response.status_code == 200:
            data = response.json()
            
            if not data:
                return [], []
            
            # Extract header from first record
            header = list(data[0].keys())
            
            # Extract rows
            rows = []
            for record in data:
                # Convert datetime fields
                for key in record:
                    if key in self.datetime and record[key]:
                        record[key] = pd.to_datetime(record[key])
                rows.append(list(record.values()))
            
            return rows, header  # Return tuple format
        else:
            raise Exception(f"API request failed with status code: {response.status_code}")
