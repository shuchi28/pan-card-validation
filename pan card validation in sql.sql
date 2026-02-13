-- ============================================================
-- PAN CARD VALIDATION - END TO END ETL PIPELINE
-- Skills: ETL, Data Quality, Business Rules Implementation
-- Author: Shuchi Shipra
-- For: Data Analyst / Data Engineer Role
-- ============================================================

-- ============================================================
-- STEP 1: EXTRACT - Data Ingestion from CSV
-- ============================================================
-- Assuming table is already created from CSV import
-- If not, run this first:
/*
CREATE TABLE raw_pan_data (
    pan_number VARCHAR(20)
);
-- Then import CSV data using your SQL tool's import wizard
*/

-- Verify data extraction
SELECT 'EXTRACT PHASE: Raw Data Sample' as phase;
SELECT * FROM raw_pan_data LIMIT 10;

-- Check for data quality issues in raw data
SELECT 
    COUNT(*) as total_raw_records,
    SUM(CASE WHEN pan_number IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(DISTINCT pan_number) as unique_count,
    COUNT(*) - COUNT(DISTINCT pan_number) as duplicate_count
FROM raw_pan_data;

-- ============================================================
-- STEP 2: TRANSFORM - Data Cleaning & Standardization
-- ============================================================

-- Create staging table for transformations
DROP TABLE IF EXISTS staging_pan_data;
CREATE TABLE staging_pan_data AS
SELECT * FROM raw_pan_data;

SELECT 'TRANSFORM PHASE: Data Cleaning Started' as phase;

-- 2.1 Handle missing values
DELETE FROM staging_pan_data 
WHERE pan_number IS NULL OR TRIM(pan_number) = '';

-- 2.2 Remove leading/trailing spaces and convert to uppercase
UPDATE staging_pan_data 
SET pan_number = UPPER(TRIM(pan_number));

-- 2.3 Remove duplicate PAN numbers (keeping first occurrence)
DELETE FROM staging_pan_data a
WHERE a.ctid NOT IN (
    SELECT MIN(b.ctid)
    FROM staging_pan_data b
    GROUP BY b.pan_number
);

-- 2.4 Verify cleaning results
SELECT 
    'After Cleaning' as stage,
    COUNT(*) as records_remaining,
    COUNT(DISTINCT pan_number) as unique_records
FROM staging_pan_data;

-- ============================================================
-- STEP 3: TRANSFORM - Business Rules Validation
-- ============================================================

-- Create validation table with detailed results
DROP TABLE IF EXISTS pan_validation_details;
CREATE TABLE pan_validation_details AS
SELECT 
    pan_number,
    LENGTH(pan_number) as char_length,
    
    -- Rule 1: Length must be 10
    CASE WHEN LENGTH(pan_number) = 10 THEN TRUE ELSE FALSE END as rule_1_length_valid,
    
    -- Rule 2: First 5 characters must be alphabets
    CASE WHEN pan_number ~ '^[A-Z]{5}' THEN TRUE ELSE FALSE END as rule_2_first5_alpha_valid,
    
    -- Rule 3: Next 4 characters must be digits
    CASE WHEN SUBSTRING(pan_number, 6, 4) ~ '^[0-9]{4}$' THEN TRUE ELSE FALSE END as rule_3_digits_valid,
    
    -- Rule 4: Last character must be alphabet
    CASE WHEN SUBSTRING(pan_number, 10, 1) ~ '^[A-Z]$' THEN TRUE ELSE FALSE END as rule_4_last_alpha_valid,
    
    -- Rule 5: No consecutive same alphabets in first 5
    CASE 
        WHEN NOT (
            SUBSTRING(pan_number, 1, 1) = SUBSTRING(pan_number, 2, 1) OR
            SUBSTRING(pan_number, 2, 1) = SUBSTRING(pan_number, 3, 1) OR
            SUBSTRING(pan_number, 3, 1) = SUBSTRING(pan_number, 4, 1) OR
            SUBSTRING(pan_number, 4, 1) = SUBSTRING(pan_number, 5, 1)
        ) THEN TRUE ELSE FALSE END as rule_5_no_consecutive_alpha_valid,
    
    -- Rule 6: No alphabetic sequence (like ABCDE)
    CASE 
        WHEN NOT (
            ASCII(SUBSTRING(pan_number, 1, 1)) + 1 = ASCII(SUBSTRING(pan_number, 2, 1)) AND
            ASCII(SUBSTRING(pan_number, 2, 1)) + 1 = ASCII(SUBSTRING(pan_number, 3, 1)) AND
            ASCII(SUBSTRING(pan_number, 3, 1)) + 1 = ASCII(SUBSTRING(pan_number, 4, 1)) AND
            ASCII(SUBSTRING(pan_number, 4, 1)) + 1 = ASCII(SUBSTRING(pan_number, 5, 1))
        ) THEN TRUE ELSE FALSE END as rule_6_no_alpha_sequence_valid,
    
    -- Rule 7: No consecutive same digits
    CASE 
        WHEN NOT (
            SUBSTRING(pan_number, 6, 1) = SUBSTRING(pan_number, 7, 1) OR
            SUBSTRING(pan_number, 7, 1) = SUBSTRING(pan_number, 8, 1) OR
            SUBSTRING(pan_number, 8, 1) = SUBSTRING(pan_number, 9, 1)
        ) THEN TRUE ELSE FALSE END as rule_7_no_consecutive_digits_valid,
    
    -- Rule 8: No digit sequence (like 1234)
    CASE 
        WHEN NOT (
            CAST(SUBSTRING(pan_number, 6, 1) AS INTEGER) + 1 = CAST(SUBSTRING(pan_number, 7, 1) AS INTEGER) AND
            CAST(SUBSTRING(pan_number, 7, 1) AS INTEGER) + 1 = CAST(SUBSTRING(pan_number, 8, 1) AS INTEGER) AND
            CAST(SUBSTRING(pan_number, 8, 1) AS INTEGER) + 1 = CAST(SUBSTRING(pan_number, 9, 1) AS INTEGER)
        ) THEN TRUE ELSE FALSE END as rule_8_no_digit_sequence_valid

FROM staging_pan_data;

-- ============================================================
-- STEP 4: TRANSFORM - Categorization & Business Logic
-- ============================================================

-- Create final validation results with status and reason
DROP TABLE IF EXISTS pan_validation_results;
CREATE TABLE pan_validation_results AS
SELECT 
    pan_number,
    CASE 
        WHEN NOT (rule_1_length_valid AND rule_2_first5_alpha_valid AND 
                  rule_3_digits_valid AND rule_4_last_alpha_valid AND
                  rule_5_no_consecutive_alpha_valid AND rule_6_no_alpha_sequence_valid AND
                  rule_7_no_consecutive_digits_valid AND rule_8_no_digit_sequence_valid)
        THEN 'INVALID'
        ELSE 'VALID'
    END as validation_status,
    
    -- Detailed reason for rejection (for audit trail)
    CASE 
        WHEN NOT rule_1_length_valid THEN 'REJECTED: Invalid length (must be 10 chars)'
        WHEN NOT rule_2_first5_alpha_valid THEN 'REJECTED: First 5 chars must be alphabets'
        WHEN NOT rule_3_digits_valid THEN 'REJECTED: Positions 6-9 must be digits'
        WHEN NOT rule_4_last_alpha_valid THEN 'REJECTED: Last character must be alphabet'
        WHEN NOT rule_5_no_consecutive_alpha_valid THEN 'REJECTED: Consecutive same alphabets found'
        WHEN NOT rule_6_no_alpha_sequence_valid THEN 'REJECTED: Alphabet sequence found (like ABCDE)'
        WHEN NOT rule_7_no_consecutive_digits_valid THEN 'REJECTED: Consecutive same digits found'
        WHEN NOT rule_8_no_digit_sequence_valid THEN 'REJECTED: Digit sequence found (like 1234)'
        ELSE 'ACCEPTED: Valid PAN format'
    END as validation_message,
    
    CURRENT_TIMESTAMP as processed_date

FROM pan_validation_details;

-- View sample results
SELECT 'VALIDATION RESULTS:' as phase;
SELECT * FROM pan_validation_results LIMIT 20;

-- ============================================================
-- STEP 5: LOAD - Create Summary Reports & Final Tables
-- ============================================================

-- 5.1 Create summary statistics table (for reporting)
DROP TABLE IF EXISTS pan_validation_summary;
CREATE TABLE pan_validation_summary AS
SELECT 
    COUNT(*) as total_records_processed,
    SUM(CASE WHEN validation_status = 'VALID' THEN 1 ELSE 0 END) as total_valid_pans,
    SUM(CASE WHEN validation_status = 'INVALID' THEN 1 ELSE 0 END) as total_invalid_pans,
    ROUND(100.0 * SUM(CASE WHEN validation_status = 'VALID' THEN 1 ELSE 0 END) / COUNT(*), 2) as valid_percentage,
    ROUND(100.0 * SUM(CASE WHEN validation_status = 'INVALID' THEN 1 ELSE 0 END) / COUNT(*), 2) as invalid_percentage
FROM pan_validation_results;

-- 5.2 Create invalid cases analysis table (for quality improvement)
DROP TABLE IF EXISTS invalid_cases_analysis;
CREATE TABLE invalid_cases_analysis AS
SELECT 
    validation_message,
    COUNT(*) as occurrence_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage_of_invalid
FROM pan_validation_results
WHERE validation_status = 'INVALID'
GROUP BY validation_message
ORDER BY occurrence_count DESC;

-- 5.3 Create final output table (analysis-ready dataset)
DROP TABLE IF EXISTS final_clean_pan_dataset;
CREATE TABLE final_clean_pan_dataset AS
SELECT 
    pvr.pan_number,
    pvr.validation_status,
    pvr.validation_message,
    pvr.processed_date,
    -- Include validation flags for filtering
    vd.rule_1_length_valid,
    vd.rule_2_first5_alpha_valid,
    vd.rule_3_digits_valid,
    vd.rule_4_last_alpha_valid
FROM pan_validation_results pvr
JOIN pan_validation_details vd ON pvr.pan_number = vd.pan_number
ORDER BY 
    CASE WHEN pvr.validation_status = 'VALID' THEN 1 ELSE 2 END,
    pvr.pan_number;

-- ============================================================
-- STEP 6: EXPORT - Generate Final Reports
-- ============================================================

-- 6.1 Executive Summary Report
SELECT 'EXECUTIVE SUMMARY REPORT' as report_title;
SELECT * FROM pan_validation_summary;

-- 6.2 Quality Issues Report
SELECT 'TOP VALIDATION FAILURE REASONS' as report_title;
SELECT * FROM invalid_cases_analysis;

-- 6.3 Valid PAN Cards (Ready for Business Use)
SELECT 'SAMPLE OF VALID PAN CARDS' as report_title;
SELECT pan_number, validation_message 
FROM final_clean_pan_dataset 
WHERE validation_status = 'VALID'
LIMIT 10;

-- 6.4 Invalid PAN Cards (Needs Review)
SELECT 'SAMPLE OF INVALID PAN CARDS' as report_title;
SELECT pan_number, validation_message 
FROM final_clean_pan_dataset 
WHERE validation_status = 'INVALID'
LIMIT 10;

-- ============================================================
-- PROJECT COMPLETED
-- ETL Pipeline Successfully Implemented
-- Tables Created for Further Analysis:
-- 1. raw_pan_data (source)
-- 2. staging_pan_data (cleaned)
-- 3. pan_validation_details (rule-level)
-- 4. pan_validation_results (final status)
-- 5. pan_validation_summary (metrics)
-- 6. invalid_cases_analysis (quality insights)
-- 7. final_clean_pan_dataset (analysis-ready)
-- ============================================================































