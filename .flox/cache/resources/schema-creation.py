import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import logging
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_sql_from_file(filename):
    with open(filename, 'r') as file:
        return file.read()

def check_table_exists(cursor, table_name):
    """Check if a table exists in the current database."""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]

def get_existing_tables(cursor):
    """Get list of existing tables in the current database."""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'spatial_ref_sys'
        AND table_name NOT LIKE 'geography_columns'
        AND table_name NOT LIKE 'geometry_columns';
    """)
    return [row[0] for row in cursor.fetchall()]

def drop_tables_if_exist(cursor):
    """Drop all existing tables in the correct order."""
    logger.info("Dropping existing tables...")
    
    # First get a list of fact table partitions
    cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE tablename LIKE 'fact_sales_%'
        AND schemaname = 'public';
    """)
    partition_tables = [row[0] for row in cursor.fetchall()]
    
    # Drop each partition
    for table in partition_tables:
        logger.info(f"Dropping partition table: {table}")
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    
    # Drop main tables in the correct order
    tables_to_drop = [
        'fact_sales',  # Parent table after partitions
        'dim_date',
        'dim_store',
        'dim_product',
        'dim_vendor'
    ]
    
    for table in tables_to_drop:
        logger.info(f"Dropping table: {table}")
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    
    logger.info("All existing tables dropped successfully.")

def check_postgis(cursor) -> bool:
    """Check if PostGIS extension is available"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM pg_available_extensions
            WHERE name = 'postgis'
        );
    """)
    return cursor.fetchone()[0]

def split_sql_statements(sql_script):
    """Split SQL script into individual statements while preserving DO blocks and complex statements."""
    statements = []
    current_statement = []
    in_dollar_quotes = False
    in_single_quotes = False
    in_double_quotes = False
    
    lines = sql_script.splitlines(True)
    
    for line in lines:
        stripped_line = line.strip()
        
        # Skip empty lines and comments
        if not stripped_line or stripped_line.startswith('--'):
            current_statement.append(line)
            continue
            
        # Process the line character by character
        i = 0
        while i < len(line):
            char = line[i]
            next_char = line[i + 1] if i + 1 < len(line) else ''
            
            # Handle dollar quotes ($$)
            if char == '$' and next_char == '$' and not in_single_quotes and not in_double_quotes:
                in_dollar_quotes = not in_dollar_quotes
                i += 2
                continue
                
            # Handle single quotes
            elif char == "'" and not in_dollar_quotes and not in_double_quotes:
                if i > 0 and line[i-1] == '\\':  # Check for escaped quote
                    i += 1
                    continue
                in_single_quotes = not in_single_quotes
                
            # Handle double quotes
            elif char == '"' and not in_dollar_quotes and not in_single_quotes:
                if i > 0 and line[i-1] == '\\':  # Check for escaped quote
                    i += 1
                    continue
                in_double_quotes = not in_double_quotes
                
            # Handle semicolons
            elif char == ';' and not in_dollar_quotes and not in_single_quotes and not in_double_quotes:
                current_statement.append(line[:i+1])
                statement = ''.join(current_statement).strip()
                if statement:
                    statements.append(statement)
                current_statement = []
                line = line[i+1:]
                i = 0
                continue
                
            i += 1
            
        # Add the remaining part of the line to the current statement
        if line:
            current_statement.append(line)
    
    # Add the last statement if it exists
    if current_statement:
        statement = ''.join(current_statement).strip()
        if statement:
            statements.append(statement)
    
    return statements

def execute_sql_script(cursor, sql_script):
    """Execute SQL statements with proper handling of complex statements."""
    statements = split_sql_statements(sql_script)
    
    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
            except Exception as e:
                logger.error(f"Error executing statement: {statement}")
                logger.error(f"Error details: {str(e)}")
                raise

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create or recreate the Iowa Liquor Sales database schema.')
    parser.add_argument('--force', action='store_true', 
                      help='Drop existing tables and recreate schema')
    parser.add_argument('--check', action='store_true',
                      help='Check if schema exists without making changes')
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Database connection parameters
    db_params = {
        'host': os.getenv('PGHOSTADDR', '127.0.0.1'),
        'port': os.getenv('PGPORT', '15432'),
        'user': os.getenv('PGUSER', 'pguser'),
        'password': os.getenv('PGPASS', 'pgpass')
    }
    
    database_name = os.getenv('PGDATABASE', 'iowa_liquor_sales')
    
    # First, connect to PostgreSQL without specifying a database
    conn = psycopg2.connect(**db_params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    try:
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
        exists = cursor.fetchone()
        
        if not exists:
            logger.info(f"Creating database {database_name}...")
            cursor.execute(f'CREATE DATABASE {database_name}')
            logger.info("Database created successfully!")
        else:
            logger.info(f"Database {database_name} already exists.")
            
    finally:
        cursor.close()
        conn.close()
    
    # Now connect to the specific database
    db_params['database'] = database_name
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    try:
        # Check for existing tables
        existing_tables = get_existing_tables(cursor)
        
        if existing_tables:
            if args.check:
                logger.info("Schema exists with the following tables:")
                for table in existing_tables:
                    logger.info(f"  - {table}")
                return
            elif not args.force:
                logger.error("Schema already exists. Use --force to drop and recreate or --check to view existing tables.")
                return
            else:
                drop_tables_if_exist(cursor)
                conn.commit()
        
        # Check and enable PostGIS
        if check_postgis(cursor):
            logger.info("Enabling PostGIS extension...")
            cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')
            conn.commit()
        else:
            raise Exception("PostGIS extension is not available. Please install PostGIS first.")
        
        # Execute the schema SQL
        logger.info("Creating schema...")
        schema_sql = load_sql_from_file('schema.sql')
        execute_sql_script(cursor, schema_sql)
        conn.commit()
        logger.info("Schema created successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        conn.rollback()
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
