-- =====================================================
-- PAN CARD VALIDATION - ETL PIPELINE
-- Author: Shuchi Shipra
-- Skills: SQL, ETL, Data Quality, Business Rules
-- =====================================================

-- =====================================================
-- STEP 1: EXTRACT - Create Staging Table
-- =====================================================
CREATE TABLE stg_pan_numbers_dataset
(
    pan_number TEXT
);

-- View sample data
SELECT * FROM stg_pan_numbers_dataset;

-- =====================================================
-- STEP 2: TRANSFORM - Data Quality Checks
-- =====================================================

-- 2.1 Check for NULL values
SELECT * FROM stg_pan_numbers_dataset WHERE pan_number IS NULL;

-- 2.2 Check for duplicates
SELECT pan_number, COUNT(1) 
FROM stg_pan_numbers_dataset 
GROUP BY pan_number 
HAVING COUNT(1) > 1;

-- 2.3 Check for leading/trailing spaces
SELECT * FROM stg_pan_numbers_dataset 
WHERE pan_number <> TRIM(pan_number);

-- 2.4 Check for lowercase letters
SELECT * FROM stg_pan_numbers_dataset 
WHERE pan_number <> UPPER(pan_number);

-- 2.5 Create cleaned dataset
SELECT DISTINCT UPPER(TRIM(pan_number)) AS pan_number
FROM stg_pan_numbers_dataset
WHERE pan_number IS NOT NULL 
  AND TRIM(pan_number) <> '';

-- =====================================================
-- STEP 3: TRANSFORM - Create Validation Functions
-- =====================================================

-- Function to check adjacent same characters (e.g., AA, BB, 11, 22)
CREATE OR REPLACE FUNCTION fn_check_adjacent_characters(p_str TEXT)
RETURNS BOOLEAN 
LANGUAGE plpgsql
AS $$
BEGIN
    FOR i IN 1..(LENGTH(p_str)-1)
    LOOP 
        IF SUBSTRING(p_str, i, 1) = SUBSTRING(p_str, i+1, 1) THEN 
            RETURN TRUE; -- Adjacent same characters found
        END IF;
    END LOOP;
    RETURN FALSE; -- No adjacent same characters
END;
$$;

-- Function to check sequential characters (e.g., ABCDE, 1234)
CREATE OR REPLACE FUNCTION fn_check_sequential_characters(p_str TEXT)
RETURNS BOOLEAN 
LANGUAGE plpgsql
AS $$
BEGIN
    FOR i IN 1..(LENGTH(p_str)-1)
    LOOP 
        IF ASCII(SUBSTRING(p_str, i+1, 1)) - ASCII(SUBSTRING(p_str, i, 1)) <> 1 THEN 
            RETURN FALSE; -- Not a sequence
        END IF;
    END LOOP;
    RETURN TRUE; -- Is a sequence (INVALID for PAN)
END;
$$;

-- =====================================================
-- STEP 4: TRANSFORM - Apply Business Rules
-- =====================================================

-- Validate PAN format using regex (AAAAA1234A)
SELECT * FROM stg_pan_numbers_dataset
WHERE pan_number ~ '^[A-Z]{5}[0-9]{4}[A-Z]$';

-- =====================================================
-- STEP 5: TRANSFORM - Categorize Valid/Invalid PAN
-- =====================================================

-- Create view for valid/invalid categorization
CREATE OR REPLACE VIEW vw_valid_invalid_pans AS
WITH cte_cleaned_pan AS
    (
        SELECT DISTINCT UPPER(TRIM(pan_number)) AS pan_number
        FROM stg_pan_numbers_dataset 
        WHERE pan_number IS NOT NULL
          AND TRIM(pan_number) <> ''
    ),
    cte_valid_pan AS
    (
        SELECT *
        FROM cte_cleaned_pan
        WHERE fn_check_adjacent_characters(pan_number) = FALSE
          AND fn_check_sequential_characters(SUBSTRING(pan_number, 1, 5)) = FALSE
          AND fn_check_sequential_characters(SUBSTRING(pan_number, 6, 4)) = FALSE
          AND pan_number ~ '^[A-Z]{5}[0-9]{4}[A-Z]$'
    )
SELECT 
    cln.pan_number,
    CASE 
        WHEN vld.pan_number IS NOT NULL THEN 'Valid PAN'
        ELSE 'Invalid PAN'
    END AS status
FROM cte_cleaned_pan cln 
LEFT JOIN cte_valid_pan vld ON vld.pan_number = cln.pan_number;

-- View results
SELECT * FROM vw_valid_invalid_pans;

-- =====================================================
-- STEP 6: LOAD - Create Summary Report
-- =====================================================

-- Generate executive summary
WITH cte AS 
(
    SELECT 
        (SELECT COUNT(*) FROM stg_pan_numbers_dataset) AS total_processed_records,
        COUNT(*) FILTER (WHERE status = 'Valid PAN') AS total_valid_pans,
        COUNT(*) FILTER (WHERE status = 'Invalid PAN') AS total_invalid_pans
    FROM vw_valid_invalid_pans
)
SELECT 
    total_processed_records,
    total_valid_pans,
    total_invalid_pans,
    (total_processed_records - (total_valid_pans + total_invalid_pans)) AS total_missing_pans,
    ROUND(100.0 * total_valid_pans / total_processed_records, 2) AS valid_percentage,
    ROUND(100.0 * total_invalid_pans / total_processed_records, 2) AS invalid_percentage
FROM cte;

-- =====================================================
-- ETL PIPELINE COMPLETED SUCCESSFULLY
-- =====================================================






















