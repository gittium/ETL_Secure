# ============= oopexcel.py =============
from Extract.rule_extract import RuleExtract
import pandas as pd
from datetime import datetime   
from typing import List 
from utils.clean_space import CleanSpace 
from utils.check_date import CheckDate
from utils.data_path import path

class ExtractExcel(RuleExtract):
    
    def __init__(self, file):
        self.file = file
        
    def extract(self):
        """Returns (rows, header) tuple for consistency"""
        file_path = path(self.file)
        
        df = pd.read_excel(file_path)
        header = df.columns.tolist()
        rows: List[List[str]] = df.values.tolist()
        
        clean_rows = []
        for row in rows:
            clean_row = []
            for cell in row:
                if isinstance(cell, str) and CheckDate.is_date(cell):
                    cell = CleanSpace.cleaning_space(cell)
                    cell = pd.to_datetime(cell)
                clean_row.append(cell)
            clean_rows.append(clean_row)
            
        return clean_rows, header  # Return tuple format