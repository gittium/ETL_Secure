


import pandas as pd
from datetime import date, datetime
from typing import Dict, List, Optional

# Define field types based on common patterns (not specific to Thai healthcare)
datetime_patterns = ['date', 'time', 'created', 'updated', 'modified', 'birth', 'admission', 'discharge']


def identify_field_type(field_name: str) -> str:
    """Identify field type based on name patterns"""
    field_lower = field_name.lower()
    
    # Check for datetime fields
    for pattern in datetime_patterns:
        if pattern in field_lower:
            return 'datetime'
    
    # Default to string
    return 'string'

def validate_field_type(row: Dict) -> bool:

    if not isinstance(row, dict):
        raise ValueError(f"Row must be a dictionary, got {type(row)}")
    
    for key, value in row.items():
        if value is None or pd.isna(value):
            continue  # Skip null values
            
        field_type = identify_field_type(key)
        
        # Check datetime fields
        if field_type == 'datetime':
            if not isinstance(value, (pd.Timestamp, datetime, date, str)):
                raise ValueError(f"Field '{key}' should be datetime-compatible, got {type(value)}: {value}")
        
    
    return True



def validate_num_rows(row: Dict, expected_fields: Optional[List[str]] = None) -> bool:
    
    if not isinstance(row, dict):
        raise ValueError(f"Row must be a dictionary, got {type(row)}")
    
    if expected_fields:
        # Check against specific expected fields
        missing_fields = set(expected_fields) - set(row.keys())
        extra_fields = set(row.keys()) - set(expected_fields)
        
        if missing_fields or extra_fields:
            raise ValueError(
                f"Schema validation failed: expected fields {expected_fields}, "
                f"got fields {list(row.keys())}. "
                f"Missing: {missing_fields}, Extra: {extra_fields}"
            )
    else:
        # Just check that row has at least one field
        if len(row) == 0:
            raise ValueError("Row has no fields")
    
    return True

def validate_row_complete(row: Dict, table_schema: Optional[Dict] = None) -> bool:
    
    try:
        # Basic type validation
        validate_field_type(row)
        
    
        
        # Schema validation only if schema is provided
        if table_schema and 'expected_fields' in table_schema:
            validate_num_rows(row, table_schema['expected_fields'])
        
        return True
    except ValueError as e:
        # Re-raise with more context
        raise ValueError(f"Row validation failed: {str(e)}")




