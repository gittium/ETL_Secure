# ============= oopcsv.py =============
from Extract.rule_extract import RuleExtract
import os 
import pandas as pd
from utils.data_path import path

class ExtractCsv(RuleExtract):
    
    def __init__(self, file):
        self.file = file
        self.datetime_header = ['วันเกิด', 'วันที่เข้ารักษา', 'วันที่จำหน่าย']
        
    def extract(self):
        """Returns (rows, header) tuple for consistency"""
        file_path = path(self.file)
        
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # Convert datetime columns
        for date in self.datetime_header:
            if date in df.columns:
                df[date] = pd.to_datetime(df[date])
        
        header = df.columns.tolist()
        rows = df.values.tolist()
        
        return rows, header  # Return tuple format