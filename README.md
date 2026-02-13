# pan-card-validation
# 🏦 PAN Card Data Validation - Advanced SQL ETL Pipeline

![Project Status](https://img.shields.io/badge/status-completed-brightgreen)
![SQL](https://img.shields.io/badge/language-PostgreSQL-blue)
![ETL](https://img.shields.io/badge/process-ETL-orange)
![Functions](https://img.shields.io/badge/feature-Custom%20Functions-purple)

## 📋 Project Overview

A robust **ETL (Extract, Transform, Load) pipeline** built using **PostgreSQL** with **custom PL/pgSQL functions** to validate Indian PAN (Permanent Account Number) card data. This project demonstrates advanced SQL capabilities including **stored procedures**, **regular expressions**, and **complex business rule implementation**.

### 🎯 Business Objective
Clean and validate PAN card data to ensure compliance with government format standards, enabling accurate KYC processing and regulatory reporting for financial institutions.

---

## 🛠️ Technical Skills Demonstrated

| Category | Skills & Technologies |
|----------|----------------------|
| **ETL Processing** | Staging Tables, Data Cleaning, Transformation Logic |
| **Advanced SQL** | PL/pgSQL Functions, CTEs, Views, Regex Pattern Matching |
| **Data Quality** | NULL Handling, Duplicate Removal, Standardization |
| **Business Rules** | Custom Validation Functions, Adjacent Character Checks |
| **Reporting** | Summary Statistics, KPI Metrics, Executive Views |

---

## 📊 ETL Pipeline Architecture
┌─────────────────────────────────────┐
│ EXTRACT PHASE │
│ ┌─────────────────────────────────┐ │
│ │ stg_pan_numbers_dataset │ │
│ │ (Raw CSV Data Import) │ │
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘
↓
┌─────────────────────────────────────┐
│ TRANSFORM PHASE │
│ ┌─────────────────────────────────┐ │
│ │ Data Cleaning: │ │
│ │ • TRIM spaces │ │
│ │ • UPPER case conversion │ │
│ │ • NULL removal │ │
│ │ • Duplicate elimination │ │
│ └─────────────────────────────────┘ │
│ ┌─────────────────────────────────┐ │
│ │ Custom Functions: │ │
│ │ • fn_check_adjacent_characters │ │
│ │ • fn_check_sequential_characters│ │
│ └─────────────────────────────────┘ │
│ ┌─────────────────────────────────┐ │
│ │ Business Rules: │ │
│ │ • Regex Pattern Validation │ │
│ │ • Adjacent Char Check │ │
│ │ • Sequence Detection │ │
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘
↓
┌─────────────────────────────────────┐
│ LOAD PHASE │
│ ┌─────────────────────────────────┐ │
│ │ vw_valid_invalid_pans (View) │ │
│ │ • Valid PAN categorization │ │
│ │ • Invalid PAN identification │ │
│ └─────────────────────────────────┘ │
│ ┌─────────────────────────────────┐ │
│ │ Summary Report │ │
│ │ • Total Records │ │
│ │ • Valid/Invalid Counts │ │
│ │ • Missing Records │ │
│ │ • Percentage Analysis │ │
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘


---

## 🔍 PAN Card Validation Rules

A valid PAN card must follow this format: **AAAAA1234A**

| Rule | Description | SQL Implementation |
|------|-------------|-------------------|
| **Rule 1** | Exactly 10 characters | `LENGTH(pan_number) = 10` |
| **Rule 2** | First 5 characters = alphabets | Regex: `^[A-Z]{5}` |
| **Rule 3** | Next 4 characters = digits | Regex: `[0-9]{4}` |
| **Rule 4** | Last character = alphabet | Regex: `[A-Z]$` |
| **Rule 5** | No consecutive same alphabets | `fn_check_adjacent_characters()` |
| **Rule 6** | No alphabetic sequence (ABCDE) | `fn_check_sequential_characters()` |
| **Rule 7** | No consecutive same digits | `fn_check_adjacent_characters()` |
| **Rule 8** | No digit sequence (1234) | `fn_check_sequential_characters()` |

---

Sample Output
Executive Summary Report
Metric	Value
Total Records Processed	1,000
Valid PAN Cards	847
Invalid PAN Cards	153
Missing/Null Records	0
Valid Percentage	84.7%
Invalid Percentage	15.3%
Sample Validation Results
PAN Number	Status
AHGVE1276F	Valid PAN
AAAAA1234F	Invalid PAN
ABCDE1234G	Invalid PAN
ZWOVO3987M	Valid PAN



