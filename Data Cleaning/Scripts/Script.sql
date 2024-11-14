SELECT *
FROM layoffs l 

-- 1. Remove duplicate
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns

--DROP TABLE IF EXISTS layoffs_staging;

-- Duplicate the dataset to keep the original data
-- Step 1: Create a new dataset，and define the datatype 
CREATE TABLE layoffs_staging (
    company VARCHAR,
    location VARCHAR,
    industry VARCHAR,
    total_laid_off VARCHAR,
    percentage_laid_off VARCHAR,
    date VARCHAR,
    stage VARCHAR,
    country VARCHAR,
    funds_raised_millions VARCHAR
);

-- Step 2: 将数据从原表复制到新表
INSERT INTO layoffs_staging (company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
FROM layoffs;


SELECT 
    *,
    ROW_NUMBER() OVER(
        PARTITION BY company, industry, total_laid_off, percentage_laid_off, date 
        ORDER BY company) AS row_num
FROM 
    layoffs_staging;


-- Step 1: Check the Duplicate
SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY company, location,industry, total_laid_off, percentage_laid_off, stage,'date', country, funds_raised_millions
            ORDER BY company) AS row_num
    FROM 
        layoffs_staging
) AS Duplicate_cte
WHERE row_num > 1;

SELECT *
FROM  layoffs_staging ls 
WHERE company = 'Casper'

-- Step 2: Delete the Duplicate
DELETE FROM layoffs_staging
WHERE rowid IN (
    SELECT rowid
    FROM (
        SELECT 
            rowid,
            ROW_NUMBER() OVER(
                PARTITION BY company, location,industry, total_laid_off, percentage_laid_off, stage,'date', country, funds_raised_millions
            ORDER BY company) AS row_num
        FROM 
            layoffs_staging
    ) AS Duplicate_cte
    WHERE row_num > 1
);

-- Check the duplicate is really deleted
SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY company, location,industry, total_laid_off, percentage_laid_off, stage,'date', country, funds_raised_millions
            ORDER BY company) AS row_num
    FROM 
        layoffs_staging
) AS Duplicate_cte
WHERE row_num > 1;

-- Standardizing data
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging ls 

UPDATE layoffs_staging 
SET company = TRIM(company)

--Merge the industry that related to crypto
SELECT *
FROM layoffs_staging ls 
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging 
SET industry  = 'Crypto'
WHERE industry LIKE 'Crypto%';

--Examine every column and standardize it 
SELECT DISTINCT country, RTRIM(country, '.') AS country_trimmed
FROM layoffs_staging ls 
WHERE country LIKE 'United States%';

UPDATE layoffs_staging 
SET country = RTRIM(country, '.')
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging ls 
WHERE country LIKE 'United States%'

-- change date form

SELECT
	date, 
	substr(date, -4) || '-' || 
	printf('%02d', CAST(substr(date, 1, INSTR(date, '/') - 1) AS INTEGER)) || '-' ||
	printf('%02d', CAST(substr(date, INSTR(date, '/') + 1, INSTR(substr(date, INSTR(date, '/') + 1), '/') - 1) AS INTEGER)) AS formatted_date
FROM 
    layoffs_staging;


UPDATE layoffs_staging
SET date = substr(date, -4) || '-' || 
           printf('%02d', CAST(substr(date, 1, INSTR(date, '/') - 1) AS INTEGER)) || '-' ||
           printf('%02d', CAST(substr(date, INSTR(date, '/') + 1, INSTR(substr(date, INSTR(date, '/') + 1), '/') - 1) AS INTEGER))
WHERE date IS NOT NULL;

UPDATE layoffs_staging
SET date = strftime('%Y-%m-%d', date);


SELECT *
From layoffs_staging ls 

--Deal with the null value

-- change the text 'null' to real null value
SELECT *
FROM layoffs_staging ls 
WHERE total_laid_off ISNULL 

UPDATE layoffs_staging 
SET 
    company = NULLIF(company, 'NULL'),
    location = NULLIF(location, 'NULL'),
    industry = NULLIF(industry, 'NULL'),
    total_laid_off = NULLIF(total_laid_off, 'NULL'),
    percentage_laid_off = NULLIF(percentage_laid_off, 'NULL'),
    date = NULLIF(date, 'NULL'),
    stage = NULLIF(stage, 'NULL'),
    country = NULLIF(country, 'NULL'),
    funds_raised_millions = NULLIF(funds_raised_millions, 'NULL');


--Fill the industry column of the company that have show industry but missing in come rows
SELECT *
FROM layoffs_staging ls 

--change the null value  
SELECT *
FROM layoffs_staging ls 
WHERE industry ISNULL 
or industry = '';

SELECT *
FROM layoffs_staging ls 
WHERE company = 'Airbnb'

SELECT ls.industry, ls2.industry 
FROM layoffs_staging ls 
JOIN layoffs_staging ls2 
	ON ls.company = ls2.company
	AND ls.location = ls2.location 
WHERE (ls.industry ISNULL or ls.industry = '')
AND ls2.industry  IS NOT NULL 

UPDATE layoffs_staging 
SET industry = (
    SELECT ls2.industry
    FROM layoffs_staging AS ls2
    WHERE ls2.company = layoffs_staging.company 
      AND ls2.industry IS NOT NULL 
      AND industry != ''
    LIMIT 1
)
WHERE industry IS NULL OR industry = '';

-- Null value for total_laid_off and percentage_laid _off is hard to deal with, 
-- there's no other data for reference, here I decided to delete cause we cannot trust the data with both two column missing
SELECT *
FROM layoffs_staging ls 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL 

DELETE FROM layoffs_staging 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL

--The basic data cleaning is done