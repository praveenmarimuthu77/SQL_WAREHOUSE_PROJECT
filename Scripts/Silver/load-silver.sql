-- ==========================================================
-- Stored Procedure: silver_load_silver
-- Purpose:  ETL from bronze -> silver in MySQL
-- Usage:    CALL silver_load_silver();
-- ==========================================================

DROP PROCEDURE IF EXISTS silver_load_silver;
DELIMITER $$

CREATE PROCEDURE silver_load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start DATETIME;
    DECLARE batch_end DATETIME;

    SET batch_start = NOW();

    -- ==========================================================
    -- CRM CUSTOMER INFO
    -- ==========================================================
    SET start_time = NOW();
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        CASE 
            WHEN CAST(cst_create_date AS CHAR) = '0000-00-00' THEN NULL
            ELSE cst_create_date
        END
    FROM (
        SELECT 
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS t
    WHERE flag_last = 1;

    SET end_time = NOW();
    SELECT CONCAT('Loaded crm_cust_info in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;

    -- ==========================================================
    -- CRM PRODUCT INFO
    -- ==========================================================
    SET start_time = NOW();
    TRUNCATE TABLE silver.crm_prd_info;

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
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7) AS prd_key,
        prd_nm,
        IFNULL(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)
    FROM bronze.crm_prd_info;

    SET end_time = NOW();
    SELECT CONCAT('Loaded crm_prd_info in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;

    -- ==========================================================
    -- CRM SALES DETAILS
    -- ==========================================================
    SET start_time = NOW();
    TRUNCATE TABLE silver.crm_sales_details;

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
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    SET end_time = NOW();
    SELECT CONCAT('Loaded crm_sales_details in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;

    -- ==========================================================
    -- ERP CUSTOMER AZ12
    -- ==========================================================
    SET start_time = NOW();
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        IF(LEFT(cid,3)='NAS', SUBSTRING(cid,4), cid),
        IF(bdate > NOW(), NULL, bdate),
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    SET end_time = NOW();
    SELECT CONCAT('Loaded erp_cust_az12 in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;

    -- ==========================================================
    -- ERP LOCATION A101
    -- ==========================================================
    SET start_time = NOW();
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid,'-',''),
        CASE
            WHEN TRIM(cntry)='DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    SET end_time = NOW();
    SELECT CONCAT('Loaded erp_loc_a101 in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;

    -- ==========================================================
    -- ERP PRODUCT CATEGORY PX_CAT_G1V2
    -- ==========================================================
    SET start_time = NOW();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    SET end_time = NOW();
    SELECT CONCAT('Loaded erp_px_cat_g1v2 in ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;

    -- ==========================================================
    -- End of Procedure
    -- ==========================================================
    SET batch_end = NOW();
    SELECT CONCAT('Total load duration: ', TIMESTAMPDIFF(SECOND, batch_start, batch_end), ' sec') AS total_time;
END$$

DELIMITER ;
