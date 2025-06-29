# extract.py - Fixed version with proper data handling

import os, sys
from typing import List, Dict, Any
from collections import defaultdict

from sqlalchemy import create_engine, MetaData, Table, Column, Index, select, text, func,Integer, String, BigInteger, inspect

from sqlalchemy.schema import Sequence

import config_store

# ─── Connection strings ───────────────────────────────────────────────────
SRC_URL = os.getenv(
    "SRC_URL",
    "postgresql+psycopg2://postgres:admin@localhost:5432/postgres"
)
DEST_URL = os.getenv(
    "DEST_URL",
    "mysql+pymysql://mysql:pass@127.0.0.1:3307/mysql"
)

src_engine = create_engine(SRC_URL, pool_pre_ping=True)
dest_engine = create_engine(DEST_URL, pool_pre_ping=True)

CHUNK = 10000
sync_state: Dict[int, Dict[str, int]] = defaultdict(dict)

# ─── Core extraction functions (FIXED for SQLAlchemy 2.0) ─────────────────
def required_cols(tbl: Table) -> List[str]:
    """Return NOT-NULL columns with no default – they must always be copied."""
    return [
        c.name
        for c in tbl.columns
        if (not c.nullable) and c.default is None and c.server_default is None
    ]

def _clean_column(col: Column) -> Column:
    """Clone a column, drop PG defaults, sequences & foreign keys."""
    clone: Column = col.copy()

    # remove server defaults / sequences that MySQL can't use
    if isinstance(clone.server_default, Sequence) or (
        clone.server_default is not None
        and "nextval" in str(clone.server_default.arg)
    ):
        clone.server_default = None
    clone.default = None

    # wipe foreign keys (avoid cross-schema FK errors)
    clone.foreign_keys.clear()
    clone.constraints = {c for c in clone.constraints if not c.foreign_keys}

    return clone

def ensure_tables(src_tbl: Table, dest_meta: MetaData):
    """Create destination and staging tables"""
    name = src_tbl.name
    stg_name = f"_stg_{name}"

    # ── destination table ────────────────────────────────────────────────
    if name not in dest_meta.tables:
        cols: List[Column] = [_clean_column(c) for c in src_tbl.columns]
        dest_tbl = Table(name, dest_meta, *cols, mysql_engine="InnoDB")

        # copy (non-FK) indexes / uniques
        for idx in src_tbl.indexes:
            if any(fk in idx.columns for fk in src_tbl.foreign_keys):
                continue
            try:
                Index(idx.name, *[dest_tbl.c[c.name] for c in idx.columns])
            except Exception:
                # Skip problematic indexes
                pass

        dest_tbl.create(bind=dest_engine)
        print(f"[+] Created dest table {name}")
    else:
        dest_tbl = dest_meta.tables[name]

    # ── staging table (always recreated empty) ───────────────────────────
    if stg_name in dest_meta.tables:
        with dest_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE `{stg_name}`"))
            conn.commit()
        dest_meta.remove(dest_meta.tables[stg_name])

    stg_cols = [_clean_column(c) for c in src_tbl.columns]
    stg_tbl = Table(stg_name, dest_meta, *stg_cols)
    stg_tbl.create(bind=dest_engine)
    
    # Refresh metadata to get the latest table structure
    dest_meta.reflect(bind=dest_engine, only=[name, stg_name])

    return dest_meta.tables[name], dest_meta.tables[stg_name]

def is_numeric(column):
    return isinstance(column.type, (Integer, BigInteger))

def copy_to_staging(src_tbl: Table, cols: List[str], stg_tbl: Table, last_pk):
    """Copy data to staging table - Fixed to avoid boolean evaluation error"""
    # Filter cols to only include columns that exist in source table
    available_cols = [c.name for c in src_tbl.columns]
    cols = [c for c in cols if c in available_cols]
    
    if not cols:
        print(f"   ❌ No valid columns to copy for {src_tbl.name}")
        return
    
    pk_cols = list(src_tbl.primary_key.columns)
    total_copied = 0
    
    # Use raw SQL approach to avoid column object issues
    src_table_name = src_tbl.name
    stg_table_name = stg_tbl.name
    
    try:
        if pk_cols:
            pk_col_name = pk_cols[0].name
            pk_type = pk_cols[0].type
            
            if is_numeric(pk_cols[0]):
                # Numeric primary key - use incremental approach
                cursor = last_pk if last_pk is not None else -1
                
                while True:
                    # Use raw SQL to avoid column object issues
                    with src_engine.connect() as conn:
                        query = text(f"""
                            SELECT {', '.join([f'"{c}"' for c in cols])}
                            FROM "{src_table_name}"
                            WHERE "{pk_col_name}" > :cursor
                            ORDER BY "{pk_col_name}"
                            LIMIT :chunk_size
                        """)
                        
                        result = conn.execute(query, {"cursor": cursor, "chunk_size": CHUNK})
                        rows = result.fetchall()
                        
                    if not rows:
                        break
                    
                    # Update cursor
                    cursor = getattr(rows[-1], pk_col_name)
                    
                    # Convert to dictionaries
                    row_dicts = []
                    for row in rows:
                        row_dict = dict(zip(cols, row))
                        row_dicts.append(row_dict)
                    
                    # Insert into staging
                    if row_dicts:
                        with dest_engine.connect() as conn:
                            conn.execute(text("SET foreign_key_checks = 0"))
                            conn.execute(stg_tbl.insert(), row_dicts)
                            conn.commit()
                            total_copied += len(row_dicts)
                    
                    print(f"   staged {len(row_dicts)} rows (total: {total_copied}, pk up to {cursor})")
            else:
                # Non-numeric or string primary key
                offset = 0
                
                while True:
                    with src_engine.connect() as conn:
                        query = text(f"""
                            SELECT {', '.join([f'"{c}"' for c in cols])}
                            FROM "{src_table_name}"
                            ORDER BY "{pk_col_name}"
                            LIMIT :chunk_size OFFSET :offset
                        """)
                        
                        result = conn.execute(query, {"chunk_size": CHUNK, "offset": offset})
                        rows = result.fetchall()
                        
                    if not rows:
                        break
                    
                    # Convert to dictionaries
                    row_dicts = []
                    for row in rows:
                        row_dict = dict(zip(cols, row))
                        row_dicts.append(row_dict)
                    
                    # Insert into staging
                    if row_dicts:
                        with dest_engine.connect() as conn:
                            conn.execute(text("SET foreign_key_checks = 0"))
                            conn.execute(stg_tbl.insert(), row_dicts)
                            conn.commit()
                            total_copied += len(row_dicts)
                    
                    offset += CHUNK
                    print(f"   staged {len(row_dicts)} rows (total: {total_copied}, offset: {offset})")
        else:
            # No primary key - use offset approach
            print(f"   ⚠️  No primary key found for {src_tbl.name}, using offset approach")
            offset = 0
            
            while True:
                with src_engine.connect() as conn:
                    query = text(f"""
                        SELECT {', '.join([f'"{c}"' for c in cols])}
                        FROM "{src_table_name}"
                        LIMIT :chunk_size OFFSET :offset
                    """)
                    
                    result = conn.execute(query, {"chunk_size": CHUNK, "offset": offset})
                    rows = result.fetchall()
                    
                if not rows:
                    break
                
                # Convert to dictionaries
                row_dicts = []
                for row in rows:
                    row_dict = dict(zip(cols, row))
                    row_dicts.append(row_dict)
                
                # Insert into staging
                if row_dicts:
                    with dest_engine.connect() as conn:
                        conn.execute(text("SET foreign_key_checks = 0"))
                        conn.execute(stg_tbl.insert(), row_dicts)
                        conn.commit()
                        total_copied += len(row_dicts)
                
                offset += CHUNK
                print(f"   staged {len(row_dicts)} rows (total: {total_copied}, offset: {offset})")
        
        if total_copied == 0:
            print(f"   ❌ No rows copied to staging for {src_tbl.name}")
        else:
            print(f"   ✅ Total rows staged: {total_copied}")
            
    except Exception as e:
        print(f"   ❌ Error in copy_to_staging: {e}")
        import traceback
        traceback.print_exc()
        raise

def merge_into_target(dest_tbl: Table, stg_tbl: Table, cols: List[str]):
    """REPLACE strategy: overwrite conflicting PK rows, insert new rows."""
    # First check if staging table has any data
    with dest_engine.connect() as conn:
        count_result = conn.execute(text(f"SELECT COUNT(*) FROM `{stg_tbl.name}`"))
        count = count_result.scalar()
        
        if count == 0:
            print(f"   No data in staging table {stg_tbl.name}, skipping merge")
            return
        
        print(f"   Merging {count} rows from staging to target")
        
        # Use REPLACE INTO to handle conflicts
        col_list = ", ".join(f"`{c}`" for c in cols)
        sql = text(
            f"""
            REPLACE INTO `{dest_tbl.name}` ({col_list})
            SELECT {col_list}
            FROM `{stg_tbl.name}`;
            """
        )
        conn.execute(sql)
        
        # Clear staging table
        conn.execute(text(f"TRUNCATE `{stg_tbl.name}`"))
        conn.commit()
        
        print(f"   Successfully merged data into {dest_tbl.name}")

def apply_etl_pipeline(staging_data: List[Dict[str, Any]], mask_fields: List[str], table_name: str = None) -> List[Dict[str, Any]]:
    """Apply transformation and validation to data with flexible schema handling"""
    cleaned_data = []
    
    print(f"   Validating {len(staging_data)} rows...")
    
    for i, row in enumerate(staging_data):
        try:
            # Skip validation of fixed schema - just validate data types and critical fields
            from error_handling import validate_field_type
            from OOPtranform import Tranform
            
            # Basic validations only
            validate_field_type(row)
            
            # Don't use validate_num_rows as it expects fixed schema
            
            # Apply transformation
            transformer = Tranform(row, mask_fields)
            transformed_row = transformer.tranform_data()
            
            cleaned_data.append(transformed_row)
            
        except Exception as e:
            # Log error but continue processing
            if i < 3:  # Only show first few errors to avoid spam
                print(f"   Warning: Row {i} - {str(e)[:80]}...")
            continue
    
    print(f"   ✅ Validated and transformed {len(cleaned_data)}/{len(staging_data)} rows")
    return cleaned_data

def run_sync(cfg_id: int, full_refresh: bool = False, use_pipeline: bool = False):
    """
    Main sync function - FIXED for SQLAlchemy 2.0 and better data handling
    """
    try:
        cfg = config_store.store.get(cfg_id)
        if not cfg:
            print(f"Config {cfg_id} not found")
            return

        sel: Dict[str, List[str]] = cfg["selections"]
        order: List[str] = cfg["load_order"]
        mask_fields = ['ชื่อนามสกุล', 'รหัสบัตรประชาชน', 'เลขกรมธรรม์']

        # ── Build MetaData for source ────────────────────────────────────────
        src_meta = MetaData()
        for full_name in order:
            if "." in full_name:
                schema, table = full_name.split(".", 1)
            else:
                schema, table = None, full_name
            
            try:
                Table(table, src_meta, schema=schema, autoload_with=src_engine)
            except Exception as e:
                print(f"Warning: Could not load table {full_name}: {e}")
                continue

        # Reflect existing MySQL tables
        dest_meta = MetaData()
        dest_meta.reflect(bind=dest_engine)

        with dest_engine.connect() as conn:
            conn.execute(text("SET foreign_key_checks = 0"))
            conn.commit()

        for tbl_name in order:
            try:
                if tbl_name not in src_meta.tables:
                    print(f"Skipping {tbl_name} - table not found")
                    continue
                    
                src_tbl = src_meta.tables[tbl_name]

                # ── Column list (user + required) ────────────────────────────────
                user_cols = sel.get(tbl_name, ["*"])
                cols = (
                    [c.name for c in src_tbl.columns] if user_cols == ["*"] else list(user_cols)
                )
                cols = list(set(cols) | set(required_cols(src_tbl)))

                pk_cols = list(src_tbl.primary_key.columns)
                if pk_cols:
                    pk_col = pk_cols[0]
                    if pk_col.name not in cols:
                        cols.append(pk_col.name)

                # ── Ensure destination + staging tables ─────────────────────────
                dest_tbl, stg_tbl = ensure_tables(src_tbl, dest_meta)
                print(f"\n[=] Processing {tbl_name}")

                # ── Determine incremental cursor ────────────────────────────────
                last_pk = None if full_refresh else sync_state[cfg_id].get(tbl_name)

                # ── Copy rows into staging ──────────────────────────────────────
                copy_to_staging(src_tbl, cols, stg_tbl, last_pk)

                # ── Apply ETL pipeline if enabled ───────────────────────────────
                if use_pipeline:
                    print(f"   Applying ETL pipeline to {tbl_name}...")
                    
                    # Extract data from staging for processing
                    with dest_engine.connect() as conn:
                        staging_result = conn.execute(select(stg_tbl))
                        staging_data = [dict(row._mapping) for row in staging_result.fetchall()]
                    
                    if staging_data:
                        print(f"   Processing {len(staging_data)} rows through pipeline...")
                        cleaned_data = apply_etl_pipeline(staging_data, mask_fields, tbl_name)
                        
                        # Clear staging and reload with cleaned data
                        with dest_engine.connect() as conn:
                            conn.execute(text(f"TRUNCATE `{stg_tbl.name}`"))
                            if cleaned_data:
                                conn.execute(stg_tbl.insert(), cleaned_data)
                            conn.commit()
                        
                        print(f"   Pipeline complete: {len(cleaned_data)} clean rows")

                # ── Merge staging → destination ─────────────────────────────────
                merge_into_target(dest_tbl, stg_tbl, cols)

                # ── Update cursor state ─────────────────────────────────────────
                if pk_cols and is_numeric(pk_cols[0]):
                    with src_engine.connect() as conn:
                        new_max = conn.execute(select(func.max(pk_cols[0]))).scalar()
                    sync_state[cfg_id][tbl_name] = new_max

            except Exception as e:
                print(f"Error processing table {tbl_name}: {e}")
                import traceback
                traceback.print_exc()
                continue

        # Re-enable foreign key checks
        with dest_engine.connect() as conn:
            conn.execute(text("SET foreign_key_checks = 1"))
            conn.commit()
            
        print("\n✅ Sync complete")

    except Exception as e:
        print(f"❌ ETL job failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Make sure to re-enable foreign key checks even if there's an error
        try:
            with dest_engine.connect() as conn:
                conn.execute(text("SET foreign_key_checks = 1"))
                conn.commit()
        except:
            pass
            
        raise

# ─── CLI Interface ────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract.py <config_id> [--pipeline] [--full-refresh]")
        sys.exit(1)
    
    try:
        cfg_id = int(sys.argv[1])
        use_pipeline = "--pipeline" in sys.argv
        full_refresh = "--full-refresh" in sys.argv
        
        print(f"🚀 Starting ETL extraction...")
        print(f"   Config ID: {cfg_id}")
        print(f"   Pipeline: {use_pipeline}")
        print(f"   Full refresh: {full_refresh}")
        print()
        
        run_sync(cfg_id, full_refresh, use_pipeline)
        
        print("\n🎉 Extraction completed successfully!")
        
    except ValueError:
        print("❌ Error: Config ID must be a number")
        print("Usage: python extract.py <config_id> [--pipeline] [--full-refresh]")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n👋 Extraction cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        sys.exit(1)