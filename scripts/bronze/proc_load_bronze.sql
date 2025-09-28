/*
========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================================

Script Purpose:
	This stored procedure loads data into the 'Bronze' schema from external CSV files.
	It performs th following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'COPY' command to load data from csv files to bronze tables.

Parameters:
	None.

This stored procedure does not accept any parameters or return any values.

Usage Example:
	CALL bronze.load_bronze();

========================================================================================
*/


DROP PROCEDURE IF EXISTS bronze.load_bronze;

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS
$$
	-- Variable to hold the start time for duration tracking
	DECLARE 
	    start_time TIMESTAMP; 
		load_duration INTERVAL;

		-- Variables for overall Bronze layer tracking
	    bronze_start_time TIMESTAMP := clock_timestamp(); -- Capture start time immediately
	    total_duration INTERVAL;
	
BEGIN
    -- Start of the TRY block
    
	RAISE NOTICE '======================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '======================';

	RAISE NOTICE '----------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '----------------------';

 	-- 1. crm_cust_info
	-- TRUNCATE and COPY for crm_cust_info
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	-- Start timer
    start_time := clock_timestamp();

		RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
		COPY bronze.crm_cust_info
		FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_crm\cust_info.csv'
		DELIMITER ',' CSV HEADER;

	-- Calculate and log duration
    load_duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> crm_cust_info load time: % seconds', load_duration;

	-- 2. crm_prod_info
	-- TRUNCATE and COPY for crm_prod_info
	RAISE NOTICE '>> Truncating Table: bronze.crm_prod_info';
	TRUNCATE TABLE bronze.crm_prod_info;
	
	-- Start timer
    start_time := clock_timestamp();
	
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prod_info';
		COPY bronze.crm_prod_info
		FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_crm\prd_info.csv'
		DELIMITER ',' CSV HEADER;

	-- Calculate and log duration
    load_duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> crm_cust_info load time: % seconds', load_duration;

	-- 3. crm_sales_details
	-- TRUNCATE and COPY for crm_sales_details
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	-- Start timer
    start_time := clock_timestamp();

		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_crm\sales_details.csv'
		DELIMITER ',' CSV HEADER;

	-- Calculate and log duration
    load_duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> crm_cust_info load time: % seconds', load_duration;

	RAISE NOTICE '----------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '----------------------';

	-- 1.  erp_cust_az12
	-- TRUNCATE and COPY for erp_cust_az12
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	-- Start timer
    start_time := clock_timestamp();

		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_erp\CUST_AZ12.csv'
		DELIMITER ',' CSV HEADER;

	-- Calculate and log duration
    load_duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> crm_cust_info load time: % seconds', load_duration;

	-- 2. erp_loc_a101
	-- TRUNCATE and COPY for erp_loc_a101
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	-- Start timer
    start_time := clock_timestamp();

		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_erp\LOC_A101.csv'
		DELIMITER ',' CSV HEADER;

	-- Calculate and log duration
    load_duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> crm_cust_info load time: % seconds', load_duration;

	-- 3. erp_px_cat_g1v2
	-- TRUNCATE and COPY for erp_px_cat_g1v2
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	-- Start timer
    start_time := clock_timestamp();

		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_erp\PX_CAT_G1V2.csv'
		DELIMITER ',' CSV HEADER;

	-- Calculate and log duration
    load_duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> crm_cust_info load time: % seconds', load_duration;
    
	------------------------------------------
    -- 🟢 Total Duration Calculation and Final Message
    ------------------------------------------
    total_duration := clock_timestamp() - bronze_start_time;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE '✅ BRONZE LAYER LOADING COMPLETED SUCCESSFULLY';
    RAISE NOTICE 'TOTAL BRONZE LOAD TIME: %', total_duration; -- Display total time
    RAISE NOTICE '==========================================';

-- Start of the CATCH block
EXCEPTION
    WHEN OTHERS THEN
        -- Log the error details
        RAISE EXCEPTION '❌ BRONZE LAYER LOADING FAILED: SQLSTATE % - %', SQLSTATE, SQLERRM;
        
        -- Since procedures can manage transactions, you can explicitly ROLLBACK here
        -- to undo any partial TRUNCATEs or COPY operations.
        ROLLBACK;
        
END;

$$;
