# OOPtranform.py - Fixed version with flexible field handling
"""
Flexible transformation that adapts to different table schemas
Only processes fields that exist in the data
"""

from hash_data import final_hashed
from typing import Dict, List, Any

class Tranform:
    
    def __init__(self, row: Dict[str, Any], mask_fields: List[str]):
        self.row = row
        self.mask_fields = mask_fields
    
    def standardize_contact(self, field_name: str = 'เบอร์โทรศัพท์'):
        """
        Standardize phone number format if the field exists
        Also handles common phone field names
        """
        # Check for Thai field name first
        phone_field = None
        if field_name in self.row:
            phone_field = field_name
        # Check common English field names
        elif 'phone' in self.row:
            phone_field = 'phone'
        elif 'contact' in self.row:
            phone_field = 'contact'
        elif 'telephone' in self.row:
            phone_field = 'telephone'
        elif 'mobile' in self.row:
            phone_field = 'mobile'
            
        if not phone_field:
            return  # No phone field found
            
        phone_value = str(self.row[phone_field])
        
        # Standardize the phone number
        if phone_value.startswith("66"):
            self.row[phone_field] = "0" + phone_value[2:]
        elif phone_value.startswith("+66"):
            self.row[phone_field] = "0" + phone_value[3:]
        elif not phone_value.startswith("0"):
            self.row[phone_field] = "0" + phone_value
        
        # Remove dashes and underscores
        if "-" in phone_value or "_" in phone_value:
            self.row[phone_field] = phone_value.replace("-", "").replace("_", "")
    
    def tranform_data(self):
        """
        Transform data with flexible field handling
        Only processes fields that actually exist in the row
        """
        # Standardize phone number if exists
        self.standardize_contact()
        
        # Apply masking only to fields that exist
        fields_to_mask = []
        for mask_field in self.mask_fields:
            if mask_field in self.row:
                fields_to_mask.append(mask_field)
        
        if fields_to_mask:
            # Apply hashing to existing fields
            self.row = final_hashed(self.row, fields_to_mask, salt="nuhos")
            
        return self.row

