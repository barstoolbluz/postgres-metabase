#!/bin/bash

# Iowa Liquor Sales Database Population Script
# This script automates the process of creating and populating a PostgreSQL database
# with Iowa liquor sales data.

# Database connection parameters
DB_USER="pguser"
DB_PASS="pgpass"
DB_NAME="iowa_liquor_sales"
CSV_FILE="./iowa_liquor_sales.csv"

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file '$CSV_FILE' not found!"
    echo "Please place the Iowa liquor sales CSV file in the same directory as this script."
    exit 1
fi

echo "============================================"
echo "  Iowa Liquor Sales Database Population"
echo "============================================"
echo "User: $DB_USER"
echo "Database: $DB_NAME"
echo "CSV File: $CSV_FILE"
echo "--------------------------------------------"

# Create a temporary SQL file with the entire database setup
TMP_SQL_FILE=$(mktemp)

cat > "$TMP_SQL_FILE" << 'EOF'
-- Drop table if it exists already
DROP TABLE IF EXISTS iowa_liquor_sales;

-- Create the table with appropriate data types
CREATE TABLE iowa_liquor_sales (
    invoice_item_number VARCHAR(50),
    date VARCHAR(50),  
    store_number INTEGER,
    store_name VARCHAR(100),
    address VARCHAR(100),
    city VARCHAR(50),
    zip_code VARCHAR(10),
    store_location VARCHAR(100),
    county_number FLOAT,
    county VARCHAR(50),
    category INTEGER,
    category_name VARCHAR(100),
    vendor_number INTEGER,
    vendor_name VARCHAR(100),
    item_number INTEGER,
    item_description VARCHAR(200),
    pack INTEGER,
    bottle_volume_ml INTEGER,
    state_bottle_cost NUMERIC(10,2),
    state_bottle_retail NUMERIC(10,2),
    bottles_sold INTEGER,
    sale_dollars NUMERIC(10,2),
    volume_sold_liters NUMERIC(10,2),
    volume_sold_gallons NUMERIC(10,2)
);

-- Performance optimization: disable logging temporarily
ALTER TABLE iowa_liquor_sales SET UNLOGGED;
EOF

# Add the \copy command dynamically with the correct path
echo "\copy iowa_liquor_sales FROM '$CSV_FILE' WITH CSV HEADER;" >> "$TMP_SQL_FILE"

# Add the rest of the SQL commands
cat >> "$TMP_SQL_FILE" << 'EOF'

-- Convert date string to actual DATE type
ALTER TABLE iowa_liquor_sales ADD COLUMN date_formatted DATE;
UPDATE iowa_liquor_sales SET date_formatted = TO_DATE(date, 'MM/DD/YYYY');
ALTER TABLE iowa_liquor_sales DROP COLUMN date;
ALTER TABLE iowa_liquor_sales RENAME COLUMN date_formatted TO date;

-- Re-enable logging
ALTER TABLE iowa_liquor_sales SET LOGGED;

-- Create indexes for better query performance
CREATE INDEX idx_iowa_date ON iowa_liquor_sales(date);
CREATE INDEX idx_iowa_store ON iowa_liquor_sales(store_number);
CREATE INDEX idx_iowa_item ON iowa_liquor_sales(item_number);
CREATE INDEX idx_iowa_county ON iowa_liquor_sales(county);

-- Run ANALYZE to update statistics
ANALYZE iowa_liquor_sales;
EOF

echo "Checking if database '$DB_NAME' exists..."
if ! psql -U "$DB_USER" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Database '$DB_NAME' does not exist. Creating it now..."
    export PGPASSWORD="$DB_PASS"
    createdb -U "$DB_USER" "$DB_NAME"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create database."
        rm "$TMP_SQL_FILE"
        exit 1
    fi
    echo "Database created successfully."
else
    echo "Database '$DB_NAME' already exists."
fi

# Execute the SQL file
echo "Populating database with Iowa liquor sales data..."
echo "This may take a while depending on the size of your CSV file..."
export PGPASSWORD="$DB_PASS"
psql -U "$DB_USER" -d "$DB_NAME" -f "$TMP_SQL_FILE" 2>&1

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "============================================"
    echo "  Database population completed successfully!"
    echo "============================================"
    
    # Create a temporary file for unit test queries
    TEST_SQL_FILE=$(mktemp)
    
    # Generate unit test queries
    cat > "$TEST_SQL_FILE" << 'EOF'
-- Unit Tests for Iowa Liquor Sales Database

\echo '\n[TEST 1] Counting total records:'
SELECT COUNT(*) AS total_records FROM iowa_liquor_sales;

\echo '\n[TEST 2] Verifying date conversion:'
SELECT 
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date,
    COUNT(DISTINCT date) AS unique_dates
FROM iowa_liquor_sales;

\echo '\n[TEST 3] Checking top 5 counties by sales volume:'
SELECT 
    county,
    SUM(sale_dollars) AS total_sales,
    SUM(bottles_sold) AS total_bottles,
    COUNT(DISTINCT store_number) AS num_stores
FROM iowa_liquor_sales
GROUP BY county
ORDER BY total_sales DESC
LIMIT 5;

\echo '\n[TEST 4] Verifying vendor data integrity:'
SELECT 
    COUNT(*) AS total_vendors,
    COUNT(DISTINCT vendor_name) AS unique_vendor_names
FROM (
    SELECT DISTINCT vendor_number, vendor_name
    FROM iowa_liquor_sales
) AS vendors;

\echo '\n[TEST 5] Checking for any NULL values in critical columns:'
SELECT
    SUM(CASE WHEN invoice_item_number IS NULL THEN 1 ELSE 0 END) AS null_invoice_items,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_dates,
    SUM(CASE WHEN store_number IS NULL THEN 1 ELSE 0 END) AS null_store_numbers,
    SUM(CASE WHEN item_number IS NULL THEN 1 ELSE 0 END) AS null_item_numbers,
    SUM(CASE WHEN sale_dollars IS NULL THEN 1 ELSE 0 END) AS null_sale_dollars
FROM iowa_liquor_sales;

\echo '\n[TEST 6] Verifying price calculations:'
SELECT
    (AVG(state_bottle_retail) > AVG(state_bottle_cost)) AS retail_greater_than_cost,
    AVG(state_bottle_retail) AS avg_retail,
    AVG(state_bottle_cost) AS avg_cost,
    AVG(state_bottle_retail - state_bottle_cost) AS avg_markup
FROM iowa_liquor_sales;

\echo '\n[TEST 7] Testing index performance (should be fast):'
EXPLAIN ANALYZE SELECT * FROM iowa_liquor_sales WHERE county = 'POLK' AND date BETWEEN '2021-01-01' AND '2021-12-31';

\echo '\n[TEST 8] Sample data (first 5 rows):'
SELECT 
    invoice_item_number,
    date,
    store_name,
    city,
    county,
    item_description,
    bottles_sold,
    sale_dollars
FROM iowa_liquor_sales
LIMIT 5;
EOF

    # Run the tests
    echo ""
    echo "Running database validation tests..."
    echo "============================================"
    export PGPASSWORD="$DB_PASS"
    psql -U "$DB_USER" -d "$DB_NAME" -f "$TEST_SQL_FILE"
    echo "============================================"
    echo "Tests completed. Check the results above to verify data integrity."
    
    # Clean up test file
    rm "$TEST_SQL_FILE"
else
    echo "============================================"
    echo "  Error: Database population failed!"
    echo "============================================"
fi

# Clean up
rm "$TMP_SQL_FILE"
