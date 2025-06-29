from datetime import datetime

class CheckDate:
    
    
    @staticmethod
    def is_date(value:str):
        formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y']
        
        
        for format in formats:
            try:
                datetime.strptime(value , format)
                return True
            except:
                continue
        return False

            
        
        