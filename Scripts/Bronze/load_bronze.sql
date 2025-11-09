
USE bronze;


SELECT DATABASE();

-- 3️⃣ Load CRM Customer Info
LOAD DATA LOCAL INFILE '/Users/praveen/Desktop/DE/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 4️⃣ Load CRM Product Info
LOAD DATA LOCAL INFILE '/Users/praveen/Desktop/DE/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 5️⃣ Load CRM Sales Details
LOAD DATA LOCAL INFILE '/Users/praveen/Desktop/DE/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 6️⃣ Load ERP Location
LOAD DATA LOCAL INFILE '/Users/praveen/Desktop/DE/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 7️⃣ Load ERP Customer (az12)
LOAD DATA LOCAL INFILE '/Users/praveen/Desktop/DE/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 8️⃣ Load ERP Product Category (px_cat_g1v2)
LOAD DATA LOCAL INFILE '/Users/praveen/Desktop/DE/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 9️⃣ Check record counts for all tables
SELECT 'crm_cust_info' AS table_name, COUNT(*) AS rows_loaded FROM crm_cust_info
UNION ALL
SELECT 'crm_prd_info', COUNT(*) FROM crm_prd_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
UNION ALL
SELECT 'erp_loc_a101', COUNT(*) FROM erp_loc_a101
UNION ALL
SELECT 'erp_cust_az12', COUNT(*) FROM erp_cust_az12
UNION ALL
SELECT 'erp_px_cat_g1v2', COUNT(*) FROM erp_px_cat_g1v2;
