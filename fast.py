# fastapi_server.py - Junior-friendly FastAPI server
"""
Simple FastAPI server for ETL system.
Uses basic concepts that junior developers can easily understand:
- Simple functions instead of complex classes
- Basic error handling with try/except
- Clear variable names and comments
- No advanced patterns or complex abstractions
"""

import os
from typing import Dict, List
from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, inspect

# Simple imports - no complex abstractions
import config_store
from depend import DependencyGraph
from extract import run_sync

# Simple database setup
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/postgres"
DEST_URL = "mysql+pymysql://mysql:pass@127.0.0.1:3307/mysql"

# Create database connections
source_db = create_engine(DATABASE_URL, pool_pre_ping=True)
dest_db = create_engine(DEST_URL, pool_pre_ping=True)
db_inspector = inspect(source_db)
dependency_helper = DependencyGraph(source_db)

# Create FastAPI app
app = FastAPI(title="Simple ETL System", version="1.0.0")

# Allow web browsers to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple data models (no complex Pydantic inheritance)
from pydantic import BaseModel

class TableSelections(BaseModel):
    selections: Dict[str, List[str]]  # {"table_name": ["column1", "column2"]}

class ETLJob(BaseModel):
    config_id: int
    use_pipeline: bool = True
    full_refresh: bool = False

# ═══════════════════════════════════════════════════════════════════════════
# SIMPLE API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/")
def home_page():
    """Show a simple home page"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head><title>Simple ETL System</title></head>
    <body>
        <h1>Welcome to Simple ETL System</h1>
        
        <ul>
            <li><a href="/docs">API Documentation</a></li>
            <li><a href="/health">Health Check</a></li>
            <li><a href="/tables">View Database Tables</a></li>
        </ul>
        
        
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/health")
def check_health():
    """Simple health check - tells you if databases are working"""
    from sqlalchemy import text
    
    try:
        # Test source database
        with source_db.connect() as connection:
            connection.execute(text("SELECT 1"))
        source_status = "OK"
    except Exception as error:
        source_status = f"ERROR: {str(error)}"
    
    try:
        # Test destination database  
        with dest_db.connect() as connection:
            connection.execute(text("SELECT 1"))
        dest_status = "OK"
    except Exception as error:
        dest_status = f"ERROR: {str(error)}"
    
    # Return simple status
    return {
        "system_status": "Healthy" if source_status == "OK" and dest_status == "OK" else "Unhealthy",
        "source_database": source_status,
        "destination_database": dest_status,
        "total_configs": len(config_store.store)
    }

@app.get("/tables")
def get_database_tables():
    """Get list of tables and their columns from the database"""
    try:
        # Get all table names
        table_names = db_inspector.get_table_names(schema='public')
        
        # Get columns for each table
        tables_info = {}
        for table_name in table_names:
            columns = db_inspector.get_columns(table_name, schema='public')
            column_names = [col['name'] for col in columns]
            tables_info[table_name] = column_names
        
        return {
            "status": "success",
            "total_tables": len(tables_info),
            "tables": tables_info
        }
        
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Could not get tables: {str(error)}")

@app.get("/dependencies")
def get_table_dependencies():
    """Show which tables depend on other tables (foreign keys)"""
    try:
        dependencies = {}
        table_names = db_inspector.get_table_names(schema='public')
        
        for table_name in table_names:
            # Get foreign keys for this table
            foreign_keys = db_inspector.get_foreign_keys(table_name, schema='public')
            dependent_tables = [fk['referred_table'] for fk in foreign_keys]
            dependencies[table_name] = dependent_tables
        
        return {
            "status": "success",
            "dependencies": dependencies
        }
        
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Could not get dependencies: {str(error)}")

@app.post("/config")
def create_configuration(table_selections: TableSelections):
    """Create a new ETL configuration"""
    try:
        # Get the tables user selected
        selected_tables = list(table_selections.selections.keys())
        
        # Figure out the safe order to load tables (respecting foreign keys)
        safe_load_order = dependency_helper.sorted_tables(selected_tables)
        
        # Create new configuration ID
        new_config_id = config_store.next_id
        config_store.next_id += 1
        
        # Save the configuration
        config_store.store[new_config_id] = {
            "selections": table_selections.selections,
            "load_order": safe_load_order,
            "created_at": "now",  # Simple timestamp
            "type": "user_config"
        }
        
        return {
            "status": "success", 
            "message": "Configuration created successfully",
            "config_id": new_config_id,
            "selected_tables": selected_tables,
            "load_order": safe_load_order
        }
        
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Could not create config: {str(error)}")

@app.get("/configs")
def list_configurations():
    """List all saved configurations"""
    if not config_store.store:
        return {
            "status": "success",
            "message": "No configurations found",
            "configs": []
        }
    
    # Convert configs to simple list
    config_list = []
    for config_id, config_data in config_store.store.items():
        config_list.append({
            "id": config_id,
            "tables": list(config_data.get("selections", {}).keys()),
            "table_count": len(config_data.get("selections", {})),
            "created_at": config_data.get("created_at", "unknown")
        })
    
    return {
        "status": "success",
        "total_configs": len(config_list),
        "configs": config_list
    }

@app.get("/configs/{config_id}")
def get_configuration(config_id: int):
    """Get details of a specific configuration"""
    if config_id not in config_store.store:
        raise HTTPException(status_code=404, detail=f"Configuration {config_id} not found")
    
    config_data = config_store.store[config_id]
    return {
        "status": "success",
        "config_id": config_id,
        "config": config_data
    }


@app.post("/run-etl")
def run_etl_job(job: ETLJob):
    """Run ETL process for a configuration"""
    # Check if configuration exists
    if job.config_id not in config_store.store:
        raise HTTPException(status_code=404, detail=f"Configuration {job.config_id} not found")
    
    try:
        # Run the ETL process (this happens in the background)
        # Note: In real production, you'd want this to run in background
        # but for simplicity, we'll just start it here
        
        print(f"Starting ETL job for config {job.config_id}")
        print(f"Pipeline enabled: {job.use_pipeline}")
        print(f"Full refresh: {job.full_refresh}")
        
        # This is where the actual ETL work happens
        run_sync(job.config_id, job.full_refresh, job.use_pipeline)
        
        return {
            "status": "success",
            "message": f"ETL job started for configuration {job.config_id}",
            "config_id": job.config_id,
            "pipeline_enabled": job.use_pipeline,
            "full_refresh": job.full_refresh
        }
        
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"ETL job failed: {str(error)}")

@app.get("/load-order")
def get_load_order(tables: str):
    """Get the safe loading order for tables (comma-separated list)"""
    try:
        # Split the comma-separated table names
        table_list = [table.strip() for table in tables.split(',') if table.strip()]
        
        if not table_list:
            raise HTTPException(status_code=400, detail="Please provide table names")
        
        # Get safe loading order
        safe_order = dependency_helper.sorted_tables(table_list)
        
        return {
            "status": "success",
            "requested_tables": table_list,
            "safe_load_order": safe_order,
            "explanation": "Tables are ordered so that parent tables come before child tables"
        }
        
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Could not calculate load order: {str(error)}")


# Run the server if this file is executed directly
if __name__ == "__main__":
    import uvicorn
    print("Starting Simple ETL System...")
    uvicorn.run(app, host="0.0.0.0", port=8000)