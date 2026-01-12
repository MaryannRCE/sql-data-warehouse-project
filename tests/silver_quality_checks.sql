-- Data Quality Checks

-- ====================
-- cust_info
-- ====================
-- Check for nulls or duplicates in primary keys
-- Expectation: No result

SELECT
cst_id,
COUNT(*)
FROM [silver].[crm_cust_info]
GROUP BY cst_id
having COUNT(*) > 1 or cst_id is null;

-- Check for unwanted spaces
-- Expectation: No result

SELECT
cst_firstname
FROM [silver].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_firstname);

SELECT
cst_lastname
FROM [silver].[crm_cust_info]
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization and consistency

SELECT DISTINCT cst_marital_status
FROM [silver].[crm_cust_info];

-- ====================
-- prd_info
-- ====================

-- Check for nulls or duplicates in primary keys

SELECT
prd_id,
COUNT(*)
FROM [bronze].[crm_prd_info]
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id is null;

-- Check for unwanted spaces

SELECT
prd_nm
FROM [bronze].[crm_prd_info]
where prd_nm != TRIM(prd_nm)

-- Check for nulls or negative numbers in prd_cost

SELECT
*
FROM [bronze].[crm_prd_info]
where prd_cost is null or prd_cost < 0

-- Data Standardization and consistency
select
distinct prd_line
from [bronze].[crm_prd_info]

-- Check for invalid date orders
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY prd_key ORDER BY prd_start_dt)
FROM [bronze].[crm_prd_info]

-- ====================
-- prd_info
-- ====================

SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM [bronze].[crm_sales_details]

SELECT
*
FROM [bronze].[crm_sales_details]
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

SELECT
	sls_sales as old_sls_sales,
	CASE
		WHEN sls_sales IS null OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/nullif(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM [bronze].[crm_sales_details]
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales is null
OR sls_quantity is null
OR sls_price is null
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- ====================
-- erp_cust_az12
-- ====================

--Identify out of range dates
SELECT
DISTINCT bdate
from [bronze].[erp_cust_az12]
where bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization
SELECT
	DISTINCT gen,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'Unknown'
	END AS gen
FROM [bronze].[erp_cust_az12]
