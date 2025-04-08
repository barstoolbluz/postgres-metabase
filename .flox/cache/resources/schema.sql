-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Enable btree_gin extension for GIN indexes
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Dimension: Date
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    CONSTRAINT date_quarter_check CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT date_month_check CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT date_day_check CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT date_day_of_week_check CHECK (day_of_week BETWEEN 1 AND 7)
) WITH (FILLFACTOR = 100);

-- Dimension: Store
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_number INTEGER UNIQUE NOT NULL,
    store_name VARCHAR(100) NOT NULL,
    address VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    zip_code VARCHAR(10),
    county VARCHAR(50) NOT NULL,
    county_number INTEGER,
    location_geom GEOMETRY(POINT, 4326),
    CONSTRAINT store_county_number_check CHECK (county_number IS NULL OR county_number BETWEEN 1 AND 99)
) WITH (FILLFACTOR = 95);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    item_number INTEGER UNIQUE NOT NULL,
    item_description VARCHAR(200) NOT NULL,
    category_number INTEGER NOT NULL DEFAULT 0,
    category_name VARCHAR(100) NOT NULL DEFAULT 'Uncategorized',
    pack INTEGER NOT NULL DEFAULT 1,
    bottle_volume_ml INTEGER NOT NULL,
    CONSTRAINT product_pack_check CHECK (pack >= 1),
    CONSTRAINT product_category_check CHECK (category_number >= 0),
    CONSTRAINT product_volume_check CHECK (bottle_volume_ml > 0)
) WITH (FILLFACTOR = 95);

-- Dimension: Vendor
CREATE TABLE dim_vendor (
    vendor_key SERIAL PRIMARY KEY,
    vendor_number INTEGER UNIQUE NOT NULL,
    vendor_name VARCHAR(100) NOT NULL,
    CONSTRAINT vendor_number_check CHECK (vendor_number > 0)
) WITH (FILLFACTOR = 95);

-- Fact: Sales (with relaxed constraints)
CREATE TABLE fact_sales (
    sales_key BIGSERIAL,
    date_key INTEGER NOT NULL,
    year INTEGER NOT NULL,
    store_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    vendor_key INTEGER NOT NULL,
    invoice_item_number VARCHAR(20) NOT NULL,
    state_bottle_cost NUMERIC(10,2) NOT NULL,
    state_bottle_retail NUMERIC(10,2) NOT NULL,
    bottles_sold INTEGER NOT NULL,
    sale_dollars NUMERIC(10,2) NOT NULL,
    volume_sold_liters NUMERIC(10,3) NOT NULL,
    volume_sold_gallons NUMERIC(10,3) NOT NULL,
    PRIMARY KEY (sales_key, year),
    CONSTRAINT fact_sales_invoice_year_key UNIQUE (invoice_item_number, year),
    CONSTRAINT fact_sales_date_fk FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fact_sales_store_fk FOREIGN KEY (store_key) REFERENCES dim_store (store_key),
    CONSTRAINT fact_sales_product_fk FOREIGN KEY (product_key) REFERENCES dim_product (product_key),
    CONSTRAINT fact_sales_vendor_fk FOREIGN KEY (vendor_key) REFERENCES dim_vendor (vendor_key),
    CONSTRAINT fact_sales_cost_check CHECK (state_bottle_cost >= 0),
    CONSTRAINT fact_sales_retail_check CHECK (state_bottle_retail >= 0),
    CONSTRAINT fact_sales_bottles_check CHECK (bottles_sold > 0),
    CONSTRAINT fact_sales_volume_check CHECK (volume_sold_liters >= 0 AND volume_sold_gallons >= 0)
) PARTITION BY RANGE (year);

-- Create partitions for each year (2012-2025)
DO $$
BEGIN
    FOR year_val IN 2012..2025 LOOP
        EXECUTE format(
            'CREATE TABLE fact_sales_%s PARTITION OF fact_sales 
             FOR VALUES FROM (%s) TO (%s)',
            year_val, year_val, year_val + 1
        );
    END LOOP;
END $$;

-- Create indexes
CREATE INDEX idx_dim_date_lookup ON dim_date (date);
CREATE INDEX idx_dim_date_year_month ON dim_date (year, month) INCLUDE (quarter, date_key);

CREATE INDEX idx_dim_store_city ON dim_store (city);
CREATE INDEX idx_dim_store_county ON dim_store (county, county_number);
CREATE INDEX idx_dim_store_location ON dim_store USING GIST (location_geom);

CREATE INDEX idx_dim_product_category ON dim_product (category_number) INCLUDE (category_name);
CREATE INDEX idx_dim_product_description ON dim_product USING gin (to_tsvector('english', item_description));

CREATE INDEX idx_fact_sales_lookup ON fact_sales (year, invoice_item_number);
CREATE INDEX idx_fact_sales_analysis ON fact_sales (date_key, store_key) INCLUDE (sale_dollars);
CREATE INDEX idx_fact_sales_reporting ON fact_sales (year, date_key) INCLUDE (sale_dollars, bottles_sold);

-- Cluster tables on their primary keys
ALTER TABLE dim_store CLUSTER ON dim_store_pkey;
ALTER TABLE dim_product CLUSTER ON dim_product_pkey;
ALTER TABLE dim_vendor CLUSTER ON dim_vendor_pkey;

-- Set statistics for better query planning
ALTER TABLE fact_sales ALTER COLUMN year SET STATISTICS 1000;
ALTER TABLE fact_sales ALTER COLUMN date_key SET STATISTICS 1000;
ALTER TABLE fact_sales ALTER COLUMN store_key SET STATISTICS 1000;

-- Add table comments
COMMENT ON TABLE dim_date IS 'Date dimension for Iowa liquor sales';
COMMENT ON TABLE dim_store IS 'Store locations with PostGIS geometry support';
COMMENT ON TABLE dim_product IS 'Product information including categories and packaging';
COMMENT ON TABLE dim_vendor IS 'Vendor/supplier information';
COMMENT ON TABLE fact_sales IS 'Sales transactions, partitioned by year';

COMMENT ON COLUMN dim_store.location_geom IS 'Store location in WGS84 coordinates (SRID 4326)';
COMMENT ON COLUMN fact_sales.year IS 'Denormalized year for partitioning';
COMMENT ON COLUMN dim_store.county_number IS 'Optional county number (1-99)';
