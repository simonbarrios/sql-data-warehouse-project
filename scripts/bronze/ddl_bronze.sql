-- =========================================================================
-- SCRIPT PURPOSE: BRONZE LAYER SCHEMA DEFINITION
-- This script creates (or recreates) the tables in the 'bronze' schema.
-- The bronze schema acts as the raw landing zone for data coming directly 
-- from two presumed source systems: CRM and ERP. 
-- Data types are chosen to match the incoming CSV file structure, often 
-- defaulting to INT/VARCHAR/TIMESTAMP/DATE to ensure raw ingestion works.
-- =========================================================================

-- === CRM SOURCE TABLES (Customer Relationship Management) ===

-- 1. Customer Information from CRM
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id                 INT,         -- Unique numerical identifier for the customer
    cst_key                VARCHAR(50), -- Customer's alphanumeric key (unique identifier)
    cst_firstname          VARCHAR(50), -- Customer's first name
    cst_lastname           VARCHAR(50), -- Customer's last name
    cst_marital_status     VARCHAR(50), -- Customer's marital status
    cst_gndr               VARCHAR(50), -- Customer's gender
    cst_create_date        DATE         -- Date the customer record was created in the CRM
);

-- 2. Product Information from CRM
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id           INT,           -- Unique numerical identifier for the product
    prd_key          VARCHAR(50),   -- Product's alphanumeric key (unique identifier)
    prd_nm           VARCHAR(50),   -- Product name
    prd_cost         INT,           -- Product cost (assuming whole numbers for simplicity)
    prd_line         VARCHAR(50),   -- Product line or category
    prd_start_dt     TIMESTAMP,     -- Date the product became active
    prd_end_dt       TIMESTAMP      -- Date the product became inactive
);

-- 3. Sales Order Details from CRM
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num    VARCHAR(50),   -- Sales Order Number (may contain letters, so VARCHAR)
    sls_prd_key    VARCHAR(50),   -- Key of the product sold (foreign key to crm_prd_info)
    sls_cust_id    INT,           -- Customer ID who placed the order (foreign key to crm_cust_info)
    sls_order_dt   INT,           -- Order Date (Note: Stored as INT, likely requiring conversion to DATE later)
    sls_ship_dt    INT,           -- Ship Date (Note: Stored as INT)
    sls_due_dt     INT,           -- Due Date (Note: Stored as INT)
    sls_sales      INT,           -- Sales amount (assuming whole numbers)
    sls_quantity   INT,           -- Quantity sold
    sls_price      INT            -- Unit price
);

-- === ERP SOURCE TABLES (Enterprise Resource Planning) ===
-- These tables likely contain dimensional or lookup data from the ERP system.

-- 4. Location Lookup Table from ERP
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid      VARCHAR(50), -- Composite ID or identifier (could be a link key)
    cntry    VARCHAR(50)  -- Country name or code
);

-- 5. Customer Attribute Data from ERP
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid      VARCHAR(50), -- Customer identifier (likely used to join with CRM data)
    bdate    DATE,        -- Birth date
    gen      VARCHAR(50)  -- Gender (alternative source/format compared to CRM)
);

-- 6. Product Category Data from ERP
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id             VARCHAR(50), -- Product ID or link key
    cat            VARCHAR(50), -- Main category
    subcat         VARCHAR(50), -- Subcategory
    maintenance    VARCHAR(50)  -- Maintenance status or flag
);
