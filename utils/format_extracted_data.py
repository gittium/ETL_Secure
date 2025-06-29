# ============= format_extracted_data.py (Fixed) =============
class FormatExtract:
    
    @staticmethod
    def format(head, rows):
        """Convert rows and headers to list of dictionaries"""
        if not rows:
            return []
        dict_data = [dict(zip(head, row)) for row in rows]
        return dict_data