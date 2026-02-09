/*
=========================================================================================
Quality Checks
=========================================================================================
Script Purpose:
      This script performs various quaity checks for data consistency, accuracy,
      and standardization across the 'silver' schemas. It includes checks for:
      -Null or duplicate primary keys.
      -Unwanted spaces in string fields.
      - Data standardization and consistency.
      -Invalid date ranges and orders.
      -Data consistency between related fields.

Usage Notes:
    -Run these checks after data loading on silver layer.
    -Investigating and resolves any discreprancies found during the checks.


=========================================================================================
*/



========================================================
--Checking for unwanted spaces
--Expection is no result
========================================================

SELECT

cst_lastname
From bronze.crm_cust_info

WHERE cst_lastname != TRIM(cst_lastname)

========================================================
--checking for nulls Duplicate in prd_id
========================================================
SELECT
prd_id,
count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAving COUNT(*) > 1 OR prd_id IS NULL
========================================================
--CHECK NULLS OR NEGATIVE NUMBERS
---EXPECTION IS NO RESULT
======================================================== 
SELECT 
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

========================================================
---CHECK for INVAILD DATES
========================================================
SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

========================================================
---CHECH FOR INVAILD DATE ORDERS
========================================================
SELECT
*
FROM bronze.crm_sales_details
where sls_order_dt  > sls_ship_dt OR sls_order_dt > sls_due_dt

==============================================================
--CHECH DATA CONSISTENCY : Between Sales,  Quantity, and price
--> sales = Quantoty * Price
---VaLUE must not be null, zero, or negative
==============================================================

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sle_price AS old_sle_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sle_price)
THEN sls_quantity * ABS(sle_price)
ELSE sls_sales
END AS sls_sales,

CASE WHEN sle_price IS NULL OR sle_price <=0
	THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sle_price
END AS sle_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sle_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sle_price iS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sle_price <=0
ORDER BY sls_sales,sls_quantity, sle_price

SELECT DISTINCT
sls_sales,
sls_quantity,
sle_price
FROM sliver.crm_sales_details
WHERE sls_sales != sls_quantity * sle_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sle_price iS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sle_price <=0
ORDER BY sls_sales,sls_quantity, sle_price

==============================================================
--Identify out of Range birthday.
==============================================================
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1920-01-01' OR  bdate > GETDATE()

--DATA Standardization & consistency

SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

==============================================================
--DATA Standardization & consistency
==============================================================
SELECT 
REPLACE (cid, '-' , '') cid,
CASE WHEN TRIM(cntry ) = 'D' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry 


FROM bronze.erp_Loc_a101
