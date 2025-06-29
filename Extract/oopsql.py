# ============= oopsql.py =============
from Extract.rule_extract import RuleExtract
from sqlalchemy import select, inspect
from sqlalchemy.engine.base import Engine
from utils.db_reflect_helper import ReflectHelper

class ExtractSQL(RuleExtract):
    
    def __init__(self, engine: Engine, tables: list): 
        self.tables = tables
        self.engine = engine
        
    def extract(self):
        """
        For SQL extraction, returns a dictionary of {table_name: (rows, header)}
        This is different but necessary for multi-table extraction
        """
        metadata = ReflectHelper.db_reflect(self.engine)
        
        all_tables = {}
        for table_name in self.tables:
            # Get table metadata
            table = metadata.tables[table_name]
            
            # Get header
            header = self._inspectHeader(table_name)
            
            # Execute query
            stmt = select(table)
            with self.engine.connect() as conn:
                result = conn.execute(stmt).fetchall()
                # Convert rows to list format
                rows = [list(row) for row in result]
                
            all_tables[table_name] = (rows, header)
            
        # If only one table, return tuple format for consistency
        if len(self.tables) == 1:
            return all_tables[self.tables[0]]
        
        return all_tables
               
    def _inspectHeader(self, table: str):
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table)
        header = [col['name'] for col in columns]
        return header