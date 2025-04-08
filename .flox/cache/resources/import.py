import os
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
import time
from dotenv import load_dotenv
from typing import Dict, List, Set, Tuple
import logging
import re
import pickle

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('PGHOSTADDR', '127.0.0.1'),
    'port': os.getenv('PGPORT', '15432'),
    'user': os.getenv('PGUSER', 'pguser'),
    'password': os.getenv('PGPASS', 'pgpass'),
    'database': os.getenv('PGDATABASE', 'iowa_liquor_sales')
}

# Configuration
CHUNK_SIZE = 500_000  # Increased from 100k for better throughput
NUM_WORKERS = max(4, os.cpu_count() // 2)  # Use half the CPU cores to avoid overwhelming the system
MIN_POOL_CONN = 4
MAX_POOL_CONN = NUM_WORKERS * 2

# Global tracker for invoice/year combinations 
# This prevents duplicate key violations by tracking across all chunks
GLOBAL_INVOICE_TRACKER = {}

class DimensionCache:
    """Cache for dimension table key mappings"""
    def __init__(self):
        self.date_cache: Dict[str, int] = {}
        self.store_cache: Dict[int, int] = {}
        self.product_cache: Dict[int, int] = {}
        self.vendor_cache: Dict[int, int] = {}

def parse_point(point_str: str) -> str:
    """Convert string point representation to PostGIS format"""
    if pd.isna(point_str):
        return None
    # Extract coordinates from format "POINT (-93.596754 41.554101)"
    match = re.search(r'POINT \(([-\d.]+) ([-\d.]+)\)', point_str)
    if match:
        lon, lat = match.groups()
        # Return WKT format with SRID
        return f'SRID=4326;POINT({lon} {lat})'
    return None

def calculate_is_weekend(day_of_week: int) -> bool:
    """Determine if day is weekend (6=Saturday, 7=Sunday)"""
    return day_of_week in (6, 7)

def safe_convert_to_int(val):
    """Safely convert a value to integer, returning None if conversion fails"""
    if pd.isna(val):
        return None
    try:
        # Remove any leading/trailing whitespace and handle 'x' prefix
        cleaned = str(val).strip().lower()
        if cleaned.startswith('x'):
            cleaned = cleaned[1:]
        return int(cleaned)
    except (ValueError, TypeError):
        return None

def bulk_copy_from_stringio(conn, table_name: str, df: pd.DataFrame, columns: List[str]):
    """Efficiently copy data from DataFrame using COPY command"""
    if df.empty:
        return 0
        
    output = StringIO()
    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)
    
    with conn.cursor() as cur:
        try:
            cur.copy_from(output, table_name, columns=columns, sep='\t')
            return len(df)
        except Exception as e:
            logger.error(f"Error copying to {table_name}: {str(e)}")
            if len(df) > 0:
                logger.error(f"First row that failed: {df.iloc[0].to_dict()}")
            raise

def collect_all_dates(csv_path: str, chunk_size: int) -> Set[pd.Timestamp]:
    """Collect all unique dates from the CSV file"""
    logger.info("Collecting all unique dates from the dataset...")
    all_dates = set()
    
    chunk_iterator = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        usecols=['Date'],  # Only read the Date column
        parse_dates=['Date']
    )
    
    for chunk in chunk_iterator:
        chunk_dates = set(pd.to_datetime(chunk['Date']).dt.normalize())
        all_dates.update(chunk_dates)
    
    logger.info(f"Found {len(all_dates)} unique dates")
    return all_dates

def process_all_dates(dates: Set[pd.Timestamp], conn) -> None:
    """Process and load all unique dates"""
    dates_df = pd.DataFrame({'date': list(dates)})
    
    # Generate date attributes
    dates_df['year'] = dates_df['date'].dt.year
    dates_df['quarter'] = dates_df['date'].dt.quarter
    dates_df['month'] = dates_df['date'].dt.month
    dates_df['day'] = dates_df['date'].dt.day
    dates_df['day_of_week'] = dates_df['date'].dt.dayofweek + 1
    dates_df['is_weekend'] = dates_df['day_of_week'].apply(calculate_is_weekend)
    
    # Convert to records and handle datetime to date conversion
    date_records = [
        (
            row['date'].date(),
            row['year'],
            row['quarter'],
            row['month'],
            row['day'],
            row['day_of_week'],
            row['is_weekend']
        )
        for _, row in dates_df.iterrows()
    ]
    
    if date_records:
        with conn.cursor() as cur:
            try:
                execute_values(
                    cur,
                    """
                    INSERT INTO dim_date (
                        date, year, quarter, month, day, 
                        day_of_week, is_weekend
                    )
                    VALUES %s
                    ON CONFLICT (date) DO NOTHING
                    RETURNING date_key, date
                    """,
                    date_records
                )
                conn.commit()
            except Exception as e:
                logger.error(f"Error inserting date records: {str(e)}")
                conn.rollback()
                raise

def validate_date_keys(conn, cache: DimensionCache) -> None:
    """Validate that all dates are in the cache"""
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(date), MAX(date) FROM dim_date")
        min_date, max_date = cur.fetchone()
        
        if min_date and max_date:
            logger.info(f"Date range in dim_date: {min_date} to {max_date}")
            
            # Get a count of all dates
            cur.execute("SELECT COUNT(*) FROM dim_date")
            date_count = cur.fetchone()[0]
            logger.info(f"Total dates in dim_date: {date_count}")
            
            # Verify cache matches database
            cur.execute("SELECT date_key, date FROM dim_date")
            db_dates = dict(cur.fetchall())
            
            if len(db_dates) != len(cache.date_cache):
                logger.error(f"Cache size ({len(cache.date_cache)}) doesn't match database size ({len(db_dates)})")
                raise ValueError("Date cache is incomplete")

def process_stores(df: pd.DataFrame, conn) -> None:
    """Process and load unique stores with PostGIS geometry and proper type conversion"""
    # First, drop duplicates to get unique stores
    stores_df = df[[
        'Store Number', 'Store Name', 'Address', 'City', 
        'Zip Code', 'County', 'County Number', 'Store Location'
    ]].drop_duplicates('Store Number')
    
    store_records = []
    for _, row in stores_df.iterrows():
        location_geom = parse_point(row['Store Location'])
        
        # Clean up zip code
        zip_code = str(row['Zip Code']).strip() if pd.notna(row['Zip Code']) else None
        
        # Handle county number
        county_number = None
        if pd.notna(row['County Number']):
            try:
                county_number = int(float(row['County Number']))
            except (ValueError, TypeError):
                logger.warning(f"Invalid county number for store {row['Store Number']}: {row['County Number']}")
        
        # Clean up store number and ensure it's an integer
        try:
            store_number = int(float(str(row['Store Number']).strip()))
        except (ValueError, TypeError):
            logger.warning(f"Invalid store number: {row['Store Number']}")
            continue
        
        if store_number > 0:  # Only process valid store numbers
            store_records.append((
                store_number,
                str(row['Store Name'] or '').strip(),
                str(row['Address'] or '').strip(),
                str(row['City'] or '').strip(),
                zip_code,
                str(row['County'] or '').strip(),
                county_number,
                location_geom
            ))
    
    if store_records:
        with conn.cursor() as cur:
            try:
                # Use execute_values for better performance
                execute_values(
                    cur,
                    """
                    INSERT INTO dim_store (
                        store_number, store_name, address, city,
                        zip_code, county, county_number, location_geom
                    )
                    VALUES %s
                    ON CONFLICT (store_number) DO UPDATE 
                    SET 
                        store_name = EXCLUDED.store_name,
                        address = EXCLUDED.address,
                        city = EXCLUDED.city,
                        zip_code = EXCLUDED.zip_code,
                        county = EXCLUDED.county,
                        county_number = EXCLUDED.county_number,
                        location_geom = EXCLUDED.location_geom
                    """,
                    store_records
                )
                conn.commit()
                logger.info(f"Successfully processed {len(store_records)} stores")
            except Exception as e:
                logger.error(f"Error inserting store records: {str(e)}")
                conn.rollback()
                raise

def collect_all_products(csv_path: str, chunk_size: int) -> pd.DataFrame:
    """Collect all unique products from the CSV file"""
    logger.info("Collecting all unique products from the dataset...")
    
    # Use a dict to keep track of the latest version of each product
    products_dict = {}
    
    chunk_iterator = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        usecols=[
            'Item Number', 'Item Description', 'Category',
            'Category Name', 'Pack', 'Bottle Volume (ml)'
        ]
    )
    
    for chunk in chunk_iterator:
        for _, row in chunk.iterrows():
            item_number = row['Item Number']
            if pd.notna(item_number):
                # Handle 'x' prefix in item numbers
                if isinstance(item_number, str) and item_number.startswith('x'):
                    item_number = item_number[1:]
                try:
                    item_number = int(float(item_number))
                    products_dict[item_number] = row.to_dict()
                except (ValueError, TypeError):
                    logger.warning(f"Invalid item number: {item_number}")
                    continue
    
    logger.info(f"Found {len(products_dict)} unique products")
    return pd.DataFrame(products_dict.values())

def process_all_products(df: pd.DataFrame, conn) -> None:
    """Process and load all unique products"""
    if df.empty:
        return
        
    # Fill NaN values with defaults and fix zero volumes
    df['Category'] = df['Category'].fillna(0)
    df['Category Name'] = df['Category Name'].fillna('Uncategorized')
    df['Pack'] = df['Pack'].fillna(1)
    
    # Replace zero or negative bottle volumes with 750 (standard bottle size)
    df['Bottle Volume (ml)'] = df['Bottle Volume (ml)'].fillna(750)
    df.loc[df['Bottle Volume (ml)'] <= 0, 'Bottle Volume (ml)'] = 750
    
    product_records = []
    
    for _, row in df.iterrows():
        try:
            item_number = row['Item Number']
            # Handle 'x' prefix
            if isinstance(item_number, str) and item_number.startswith('x'):
                item_number = int(item_number[1:])
            else:
                item_number = int(float(item_number))
                
            category = int(float(row['Category'])) if pd.notna(row['Category']) else 0
            pack = int(float(row['Pack'])) if pd.notna(row['Pack']) else 1
            bottle_volume = int(float(row['Bottle Volume (ml)']))  # Will now never be 0
            
            product_records.append((
                item_number,
                str(row['Item Description']),
                category,
                str(row['Category Name']),
                pack,
                bottle_volume
            ))
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error processing product {row['Item Number']}: {str(e)}")
            continue
    
    if product_records:
        try:
            with conn.cursor() as cur:
                # Process in batches
                batch_size = 1000
                for i in range(0, len(product_records), batch_size):
                    batch = product_records[i:i + batch_size]
                    execute_values(
                        cur,
                        """
                        INSERT INTO dim_product (
                            item_number, item_description, category_number,
                            category_name, pack, bottle_volume_ml
                        )
                        VALUES %s
                        ON CONFLICT (item_number) DO UPDATE
                        SET 
                            item_description = EXCLUDED.item_description,
                            category_number = EXCLUDED.category_number,
                            category_name = EXCLUDED.category_name,
                            pack = EXCLUDED.pack,
                            bottle_volume_ml = EXCLUDED.bottle_volume_ml
                        """,
                        batch
                    )
                    logger.info(f"Processed products batch {i//batch_size + 1} of {(len(product_records) + batch_size - 1)//batch_size}")
            
            conn.commit()
        except Exception as e:
            logger.error(f"Error inserting product records: {str(e)}")
            conn.rollback()
            raise

def validate_product_cache(conn, cache: DimensionCache) -> None:
    """Validate that all products are in the cache"""
    with conn.cursor() as cur:
        # Get total count of products
        cur.execute("SELECT COUNT(*) FROM dim_product")
        db_count = cur.fetchone()[0]
        
        if len(cache.product_cache) != db_count:
            logger.error(f"Product cache size ({len(cache.product_cache)}) doesn't match database size ({db_count})")
            
            # Find missing products
            cur.execute("""
                SELECT item_number, product_key 
                FROM dim_product 
                WHERE product_key NOT IN %s
            """, (tuple(cache.product_cache.values()) or (0,),))
            
            missing = cur.fetchall()
            if missing:
                logger.error(f"Missing {len(missing)} products in cache. First few: {missing[:5]}")
            
            raise ValueError("Product cache is incomplete")

def process_vendors(df: pd.DataFrame, conn) -> None:
    """Process and load unique vendors with proper handling of duplicates"""
    # Ensure vendor numbers are properly converted to integers
    df['Vendor Number'] = df['Vendor Number'].apply(safe_convert_to_int)
    
    # Get unique vendors but keep track of all names for each vendor number
    vendors_df = df[['Vendor Number', 'Vendor Name']].dropna(subset=['Vendor Number'])
    
    # Group by vendor number and aggregate names
    vendor_groups = vendors_df.groupby('Vendor Number').agg({
        'Vendor Name': lambda x: x.value_counts().index[0]  # Take most common name
    }).reset_index()
    
    vendor_records = []
    skipped_vendors = []
    
    for _, row in vendor_groups.iterrows():
        try:
            vendor_number = int(row['Vendor Number'])
            vendor_name = str(row['Vendor Name']).strip()
            
            if vendor_number > 0 and vendor_name:
                vendor_records.append((vendor_number, vendor_name))
            else:
                skipped_vendors.append(str(vendor_number))
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error processing vendor: {row['Vendor Number']} - {str(e)}")
            skipped_vendors.append(str(row['Vendor Number']))
    
    if vendor_records:
        try:
            # Process in smaller batches to reduce memory usage
            batch_size = 1000
            for i in range(0, len(vendor_records), batch_size):
                batch = vendor_records[i:i + batch_size]
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO dim_vendor (vendor_number, vendor_name)
                        VALUES %s
                        ON CONFLICT (vendor_number) DO UPDATE
                        SET vendor_name = EXCLUDED.vendor_name
                        """,
                        batch
                    )
                logger.info(f"Processed vendors batch {i//batch_size + 1} of {(len(vendor_records) + batch_size - 1)//batch_size}")
            
            conn.commit()
                
        except Exception as e:
            logger.error(f"Error inserting vendor records: {str(e)}")
            if vendor_records:
                logger.error(f"First failing record: {vendor_records[0]}")
            conn.rollback()
            raise
            
    if skipped_vendors:
        logger.warning(f"Skipped {len(skipped_vendors)} vendors due to data quality issues")

def check_existing_invoices(conn) -> Dict:
    """Load all existing invoice number/year combinations from the database"""
    global GLOBAL_INVOICE_TRACKER
    
    logger.info("Loading existing invoice records from database...")
    with conn.cursor() as cur:
        # Get all existing invoice numbers and years
        cur.execute("SELECT invoice_item_number, year FROM fact_sales")
        
        count = 0
        for invoice_item_number, year in cur:
            key = (invoice_item_number, year)
            GLOBAL_INVOICE_TRACKER[key] = True
            count += 1
            
            if count % 1000000 == 0:
                logger.info(f"Loaded {count} existing invoices into tracker")
    
    logger.info(f"Loaded {len(GLOBAL_INVOICE_TRACKER)} existing invoice records into tracker")
    return GLOBAL_INVOICE_TRACKER

def process_chunk(chunk: pd.DataFrame, cache: DimensionCache) -> List[dict]:
    """Process a chunk of data and return fact table records"""
    global GLOBAL_INVOICE_TRACKER
    
    fact_records = []
    missing_products = set()
    missing_vendors = set()
    missing_stores = set()
    chunk_seen_invoices = set()  # Local tracking for performance
    duplicate_count = 0
    
    # Sort by date for consistent handling
    chunk = chunk.sort_values('Date')
    
    for _, row in chunk.iterrows():
        try:
            date = pd.to_datetime(row['Date']).date()
            year = pd.to_datetime(row['Date']).year
            invoice_item_number = row['Invoice/Item Number']
            
            # Skip if missing or invalid invoice
            if pd.isna(invoice_item_number) or not invoice_item_number:
                continue
                
            invoice_key = (invoice_item_number, year)
            
            # Skip if we've seen this invoice in this chunk (for performance)
            if invoice_key in chunk_seen_invoices:
                continue
                
            # Skip if we've seen this invoice in previous chunks or DB
            if invoice_key in GLOBAL_INVOICE_TRACKER:
                duplicate_count += 1
                continue
                
            # Mark as seen locally and globally
            chunk_seen_invoices.add(invoice_key)
            GLOBAL_INVOICE_TRACKER[invoice_key] = True
            
            # Skip if required numeric fields are missing or invalid
            required_numeric_fields = {
                'State Bottle Cost': row['State Bottle Cost'],
                'State Bottle Retail': row['State Bottle Retail'],
                'Bottles Sold': row['Bottles Sold'],
                'Sale (Dollars)': row['Sale (Dollars)'],
                'Volume Sold (Liters)': row['Volume Sold (Liters)'],
                'Volume Sold (Gallons)': row['Volume Sold (Gallons)']
            }
            
            if any(pd.isna(val) or float(val) <= 0 for val in required_numeric_fields.values()):
                continue
            
            store_number = safe_convert_to_int(row['Store Number'])
            vendor_number = safe_convert_to_int(row['Vendor Number'])
            item_number = safe_convert_to_int(row['Item Number'])
            
            # Skip if any required dimension values are missing
            if any(x is None for x in [store_number, vendor_number, item_number]):
                continue
            
            # Look up keys from cache
            store_key = cache.store_cache.get(store_number)
            vendor_key = cache.vendor_cache.get(vendor_number)
            product_key = cache.product_cache.get(item_number)
            date_key = cache.date_cache.get(str(date))
            
            # Skip if any required dimensions are missing in cache
            if not store_key:
                missing_stores.add(store_number)
                continue
                
            if not vendor_key:
                missing_vendors.add(vendor_number)
                continue
                
            if not product_key:
                missing_products.add(item_number)
                continue
                
            if not date_key:
                continue
            
            # All keys are valid, add to facts
            fact_records.append({
                'date_key': date_key,
                'year': year,
                'store_key': store_key,
                'product_key': product_key,
                'vendor_key': vendor_key,
                'invoice_item_number': invoice_item_number,
                'state_bottle_cost': float(row['State Bottle Cost']),
                'state_bottle_retail': float(row['State Bottle Retail']),
                'bottles_sold': int(row['Bottles Sold']),
                'sale_dollars': float(row['Sale (Dollars)']),
                'volume_sold_liters': float(row['Volume Sold (Liters)']),
                'volume_sold_gallons': float(row['Volume Sold (Gallons)'])
            })
            
        except (ValueError, TypeError) as e:
            logger.debug(f"Error converting values for invoice {row['Invoice/Item Number']}: {str(e)}")
            continue
        except KeyError as e:
            logger.debug(f"Missing key: {str(e)}")
            continue
    
    # Log missing dimension keys if any
    if missing_stores:
        logger.warning(f"Found {len(missing_stores)} missing store keys. First few: {list(missing_stores)[:5]}")
    if missing_vendors:
        logger.warning(f"Found {len(missing_vendors)} missing vendor keys. First few: {list(missing_vendors)[:5]}")
    if missing_products:
        logger.warning(f"Found {len(missing_products)} missing product keys. First few: {list(missing_products)[:5]}")
    
    if duplicate_count > 0:
        logger.debug(f"Skipped {duplicate_count} duplicate invoices in this chunk")
    
    return fact_records

def load_fact_records_batch(records: List[dict], pool) -> None:
    """Load a batch of fact records with proper transaction handling"""
    if not records:
        return
        
    conn = None
    try:
        conn = pool.getconn()
        # Convert records to DataFrame for efficient loading
        df = pd.DataFrame(records)
        
        # Ensure numeric columns are properly formatted
        numeric_cols = [
            'state_bottle_cost', 'state_bottle_retail', 'bottles_sold',
            'sale_dollars', 'volume_sold_liters', 'volume_sold_gallons'
        ]
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna(subset=numeric_cols)
        
        # Double-check for duplicates one more time
        df = df.drop_duplicates(subset=['invoice_item_number', 'year'])
        
        if df.empty:
            return
        
        with conn:  # This ensures transaction handling
            with conn.cursor() as cur:
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                output.seek(0)
                
                try:
                    cur.copy_from(
                        output,
                        'fact_sales',
                        columns=[
                            'date_key', 'year', 'store_key', 'product_key', 
                            'vendor_key', 'invoice_item_number', 'state_bottle_cost',
                            'state_bottle_retail', 'bottles_sold', 'sale_dollars',
                            'volume_sold_liters', 'volume_sold_gallons'
                        ]
                    )
                except Exception as e:
                    logger.error(f"Error in COPY operation: {str(e)}")
                    if len(df) > 0:
                        logger.error(f"First row that failed: {df.iloc[0].to_dict()}")
                    raise
                
    except Exception as e:
        logger.error(f"Error loading fact records batch: {str(e)}")
        raise
    finally:
        if conn is not None:
            pool.putconn(conn)

def check_postgis(conn) -> bool:
    """Verify PostGIS is installed"""
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT PostGIS_Version();")
            version = cur.fetchone()[0]
            logger.info(f"PostGIS version: {version}")
            return True
        except Exception as e:
            logger.error(f"PostGIS is not installed! Error: {str(e)}")
            return False

def save_tracker_checkpoint(checkpoint_dir='./checkpoints'):
    """Save the global invoice tracker to disk for resumability"""
    global GLOBAL_INVOICE_TRACKER
    
    os.makedirs(checkpoint_dir, exist_ok=True)
    checkpoint_file = os.path.join(checkpoint_dir, f'invoice_tracker_{int(time.time())}.pkl')
    
    logger.info(f"Saving invoice tracker checkpoint with {len(GLOBAL_INVOICE_TRACKER)} entries...")
    try:
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(GLOBAL_INVOICE_TRACKER, f)
        logger.info(f"Checkpoint saved to {checkpoint_file}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {str(e)}")

def load_tracker_checkpoint(checkpoint_dir='./checkpoints'):
    """Load the most recent invoice tracker checkpoint if available"""
    global GLOBAL_INVOICE_TRACKER
    
    if not os.path.exists(checkpoint_dir):
        logger.info("No checkpoint directory found")
        return False
    
    checkpoint_files = [f for f in os.listdir(checkpoint_dir) if f.startswith('invoice_tracker_') and f.endswith('.pkl')]
    if not checkpoint_files:
        logger.info("No checkpoint files found")
        return False
    
    # Get the most recent checkpoint
    latest_checkpoint = max(checkpoint_files, key=lambda f: int(f.split('_')[2].split('.')[0]))
    checkpoint_path = os.path.join(checkpoint_dir, latest_checkpoint)
    
    logger.info(f"Loading invoice tracker from checkpoint: {checkpoint_path}")
    try:
        with open(checkpoint_path, 'rb') as f:
            GLOBAL_INVOICE_TRACKER = pickle.load(f)
        logger.info(f"Loaded {len(GLOBAL_INVOICE_TRACKER)} invoice records from checkpoint")
        return True
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {str(e)}")
        return False

def main():
    """Main function to process and load Iowa liquor sales data"""
    global GLOBAL_INVOICE_TRACKER
    
    csv_path = 'iowa_liquor_sales.csv'
    start_time = time.time()
    checkpoint_interval = 5  # Save checkpoint every N chunks
    
    # Create connection pool
    pool = ThreadedConnectionPool(MIN_POOL_CONN, MAX_POOL_CONN, **DB_PARAMS)
    main_conn = pool.getconn()
    
    try:
        # Verify PostGIS
        if not check_postgis(main_conn):
            raise Exception("PostGIS is required but not installed")
            
        # First try to load from checkpoint
        checkpoint_loaded = load_tracker_checkpoint()
        
        # If no checkpoint, load existing invoices from database
        if not checkpoint_loaded:
            check_existing_invoices(main_conn)
            
        logger.info(f"Invoice tracker initialized with {len(GLOBAL_INVOICE_TRACKER)} entries")
        
        # First, collect and process ALL dates
        logger.info("Starting date processing...")
        all_dates = collect_all_dates(csv_path, CHUNK_SIZE)
        process_all_dates(all_dates, main_conn)
        logger.info("Completed date processing")
        
        # Process all products next
        logger.info("Starting product processing...")
        all_products = collect_all_products(csv_path, CHUNK_SIZE)
        process_all_products(all_products, main_conn)
        logger.info("Completed product processing")
        
        # Process remaining dimensions
        with main_conn:
            logger.info("Processing remaining dimension tables...")
            chunk_iterator = pd.read_csv(
                csv_path, 
                chunksize=CHUNK_SIZE,
                dtype={
                    'Zip Code': str,
                    'Store Number': str,
                    'County Number': str,
                    'Vendor Number': str,
                },
                parse_dates=['Date'],
                low_memory=False
            )
            
            for chunk_number, chunk in enumerate(chunk_iterator, 1):
                logger.info(f"Processing dimensions for chunk {chunk_number}")
                process_stores(chunk, main_conn)
                process_vendors(chunk, main_conn)
            
            # Load dimension caches with validation
            logger.info("Loading dimension caches...")
            cache = DimensionCache()
            
            with main_conn.cursor() as cur:
                # Load date cache
                cur.execute("SELECT date_key, date FROM dim_date")
                for date_key, date in cur.fetchall():
                    cache.date_cache[str(date)] = date_key
                
                # Load product cache
                cur.execute("SELECT product_key, item_number FROM dim_product")
                for product_key, item_number in cur.fetchall():
                    cache.product_cache[item_number] = product_key
                
                # Load store cache
                cur.execute("SELECT store_key, store_number FROM dim_store")
                for store_key, store_number in cur.fetchall():
                    cache.store_cache[store_number] = store_key
                
                # Load vendor cache
                cur.execute("SELECT vendor_key, vendor_number FROM dim_vendor")
                for vendor_key, vendor_number in cur.fetchall():
                    cache.vendor_cache[vendor_number] = vendor_key
            
            # Validate caches
            validate_date_keys(main_conn, cache)
            validate_product_cache(main_conn, cache)
            
            # Log cache sizes
            logger.info(f"Cache sizes - Dates: {len(cache.date_cache)}, "
                       f"Products: {len(cache.product_cache)}, "
                       f"Stores: {len(cache.store_cache)}, "
                       f"Vendors: {len(cache.vendor_cache)}")
            
            # Process fact table with parallel processing
            logger.info("Processing fact table...")
            chunk_iterator = pd.read_csv(
                csv_path, 
                chunksize=CHUNK_SIZE,
                dtype={
                    'Store Number': str,
                    'Vendor Number': str,
                    'Item Number': str
                },
                parse_dates=['Date'],
                low_memory=False
            )
            
            # Process chunks in parallel with better error handling
            with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                futures = []
                errors = []
                completed = 0
                chunk_count = 0
                
                for chunk_number, chunk in enumerate(chunk_iterator, 1):
                    chunk_count = chunk_number
                    logger.info(f"Processing fact records for chunk {chunk_number}")
                    fact_records = process_chunk(chunk, cache)
                    
                    if fact_records:
                        # Split fact records into smaller batches for loading
                        batch_size = 50_000
                        for i in range(0, len(fact_records), batch_size):
                            batch = fact_records[i:i + batch_size]
                            futures.append(
                                executor.submit(load_fact_records_batch, batch, pool)
                            )
                    
                    # Save checkpoint periodically
                    if chunk_number % checkpoint_interval == 0:
                        save_tracker_checkpoint()
                    
                    # Log progress periodically
                    if chunk_number % 10 == 0:
                        logger.info(f"Processed {chunk_number} chunks so far...")
                        logger.info(f"Global invoice tracker now has {len(GLOBAL_INVOICE_TRACKER)} entries")
                
                # Wait for all futures to complete and handle any errors
                logger.info("Waiting for all fact processing to complete...")
                
                for future in futures:
                    try:
                        future.result()
                        completed += 1
                        if completed % 100 == 0:
                            logger.info(f"Completed {completed} fact batches out of {len(futures)}")
                    except Exception as e:
                        # Don't raise errors, just collect them
                        errors.append(str(e))
                        logger.error(f"Error in fact processing thread: {str(e)}")
                
                # Save final checkpoint
                save_tracker_checkpoint()
                
                if errors:
                    logger.warning(f"Completed with {len(errors)} errors out of {len(futures)} batch jobs")
                else:
                    logger.info(f"Successfully processed all {chunk_count} chunks and {completed} batches with no errors")
        
        # Analyze tables at the end
        logger.info("Analyzing tables for better query performance...")
        with main_conn.cursor() as cur:
            try:
                cur.execute("ANALYZE dim_date")
                cur.execute("ANALYZE dim_store")
                cur.execute("ANALYZE dim_product")
                cur.execute("ANALYZE dim_vendor")
                cur.execute("ANALYZE fact_sales")
                main_conn.commit()
            except Exception as e:
                logger.warning(f"Error analyzing tables: {str(e)}")
        
        duration = time.time() - start_time
        logger.info(f"Total processing time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        try:
            main_conn.rollback()
        except Exception:
            pass  # Ignore rollback errors
        raise
    finally:
        try:
            pool.putconn(main_conn)
        except Exception:
            pass  # Ignore connection return errors
        try:
            pool.closeall()
        except Exception:
            pass  # Ignore pool closure errors

if __name__ == "__main__":
    main()
