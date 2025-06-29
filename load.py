from sqlalchemy import create_engine , insert , MetaData 
from sqlalchemy.orm import Session
from Extract.rule_extract import RuleLoad

class Loader(RuleLoad) :
    
    
    def __init__(self , destination_engine , destination_table , clean_data):
        self.destination_engine = destination_engine
        self.destination_table =  destination_table
        self.clean_data = clean_data
    
    def load(self):
        with Session(self.destination_engine) as conn:
        # with self.destination_engine.connect() as conn:
            conn.execute(insert(self.destination_table) ,self.clean_data )
            conn.commit()
        return  f"Inserted {len(self.clean_data)} rows into '{self.destination_table.name}'."
        
    







# def load_data(cleaned_data, table_name, conn):
#     print("load connection success")
#     cur = conn.cursor()

#     for row in cleaned_data:
#         columns = list(row.keys())              # ['hospital_name', 'contact', ...]
#         values = list(row.values())             # ['Sirirat', '+6680', ...]

#         placeholders = ", ".join(["%s"] * len(values))     # '%s, %s, %s, ...'
#         column_names = ", ".join(columns)

#         insert_sql = f"""
#             INSERT INTO {table_name} ({column_names})
#             VALUES ({placeholders});
#         """

#         cur.execute(insert_sql, values)

#     conn.commit()
    
#     cur.close()
    
#     conn.close()
    
