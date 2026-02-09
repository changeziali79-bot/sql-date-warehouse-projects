/*
==========================================================================================================================
STORED PROCEDURE: Load Silver Layer (BRONZE -> Silver)
==========================================================================================================================
SCRIPT Purpose;
      This stored procedure performs the ETL( EXTRACT, TRANSFORM, LOAD) process to 
      populate the 'silver' schema tables from the 'bronze' schema.
  Action Performed;
    -Truncates Silver Tables. 
    -Insets transformed and cleansed data from bronze into silver tables.

Parameters:
    NONE.
    This stored procudure does not accept any parameters or return any values. 
Usage Example or Command;
    EXEC Silver.load_silver;
==========================================================================================================================
*/

  
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @batch_start_time DATETIME, 
            @batch_end_time DATETIME;

    SET @batch_start_time = GETDATE();

    BEGIN TRY

        PRINT '===============================================';
        PRINT 'Loading Silver Layer';
        PRINT '===============================================';
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------------------------';

        --Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>>Truncate table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Insert DATA IN TO silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_martital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) as cst_firstname,
            TRIM(cst_lastname) as cst_lastname,
            CASE WHEN UPPER(TRIM(cst_martital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_martital_status)) = 'M' THEN 'Married'
                 ELSE 'N/A'
            END cst_martital_status, ----- Normalize the martital status value to readable format.
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'N/A'
            END cst_gndr, ----- Normalize the Gender value to readable format.
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t 
        WHERE flag_last = 1; ---AND cst_id IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        --Loading silver.erp_Loc_a101
        SET @start_time = GETDATE();
        PRINT '>>Truncate table: silver.erp_Loc_a101';
        TRUNCATE TABLE silver.erp_Loc_a101;

        PRINT '>> Insert DATA IN TO silver.erp_Loc_a101';
        INSERT INTO silver.erp_Loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') cid,
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                 ELSE TRIM(cntry)
            END AS cntry ----- Normalize and handle missing or blank country codes
        FROM bronze.erp_Loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        --Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>>Truncate table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Insert DATA IN TO silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --- Remove 'NAS' prefix if present
                 ELSE cid
            END cid,
            CASE WHEN bdate > GETDATE() THEN NULL
                 ELSE bdate
            END AS bdate, ---set future birthddates to NULL
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a'
            END AS gen --- NORMALIZE gender Values and handle the unkown caces
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        --Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>>Truncate table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Insert DATA IN TO silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, ---Extract category Id
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, ----Extract product key
            prd_nm AS ProductNAME,
            ISNULL(prd_cost, 0) AS prd_cost, ---Handling NUllS. Changing Nulls value to 0
            CASE UPPER(TRIM(prd_line))
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'S' THEN 'Other sales'
                 WHEN 'T' THEN 'Touring'
                 ELSE 'N/A'
            END pre_line, ------------Change Product name to redable format.
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1
                AS DATE
            ) AS prd_end_dt --------Calculate end date as one day before the start of the next date.(DATA ENRICHMENT)
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        --Loading silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>>Truncate table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Insert DATA IN TO silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT *
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        --Loading silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>>Truncate table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Insert DATA IN TO silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sle_price)
                 THEN sls_quantity * ABS(sle_price)
                 ELSE sls_sales
            END AS sls_sales, ---Reculculating sales if the orginal if the orginal value is missing or incorrect
            CASE WHEN sle_price IS NULL OR sle_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sle_price
            END AS sls_price, ---Derive price if orginal value is invalid
            sls_quantity
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        SET @batch_end_time = GETDATE();

        PRINT '===============================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT 'Total Duration :' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
        PRINT '==========================================================';

    END TRY
    BEGIN CATCH

        PRINT '=============================';
        PRINT 'Error Occared During The silver Layer';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Line' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);

    END CATCH

END
