# config_store.py
from typing import Dict

store: Dict[int, Dict] = {}    # {config_id : {config details}}
next_id: int = 1
