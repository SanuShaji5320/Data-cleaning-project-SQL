
-- Step 1: Create a working (staging) table from the original dataset
   select *
   into LayoffsData_Staging2
   from LayoffsData

-- Preview the staging table
   select* from LayoffsData_Staging2


-- Step 2: Identify and remove duplicate records

-- 2.1: Add row numbers to help detect duplicate rows based on key columns
   
   select* ,
   ROW_NUMBER() over 
   (partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions order by company)
   as row_num
   from LayoffsData_Staging2

-- 2.2: Use a Common Table Expression (CTE) to isolate duplicate rows

   with rownumber as (select* ,
   ROW_NUMBER() over 
   (partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions order by company)
   as row_num
   from LayoffsData_Staging2)
   select* from rownumber where row_num > 1

-- 2.3: Delete the duplicate rows (row numbers > 1)

     with rownumber as (select* ,
   ROW_NUMBER() over 
   (partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions order by company)
   as row_num
   from LayoffsData_Staging2)
   delete from rownumber where row_num > 1


-- Step 3: Standardizing Data 
-- 3.1: Clearing white space from columns

   Update LayoffsData_Staging set company = trim(company)

-- 3.2: Unifying similar rows

   --identified crypto, cryptocurrency and crypto currency as a similar row and updated the rows to crypto
   select distinct industry from LayoffsData_Staging2 order by 1

   select distinct industry from LayoffsData_Staging2 where industry like 'crypto%'

   update LayoffsData_Staging2 set industry = 'crypto' where industry like 'crypto%'

   -- Check for inconsistent company names (e.g., with trailing spaces)
   select distinct country from LayoffsData_Staging2 order by 1

   select distinct country from LayoffsData_Staging2 where country like 'United States%'

   select distinct country, trim( trailing '.' from country) from LayoffsData_Staging2 where country like 'United States%'

   update LayoffsData_Staging2 set country = trim( trailing '.' from country) where country like 'United States%' -- trimming additional '.' from the column

-- 3.3: Convert string dates into proper DATE format date formatting

   select date from LayoffsData_Staging2 order by date desc

   select date, TRY_CONVERT (date, [DATE], 101) AS Newdate
   from LayoffsData_Staging2

   update LayoffsData_Staging2 set date = TRY_CONVERT (date, [DATE], 101)

   alter table layoffsData_Staging2
   Alter column [date] DATE

-- Step 4: Fix null or missing values

   select* from LayoffsData_Staging2 where total_laid_off = 'NULL' OR total_laid_off = ''

   select* from LayoffsData_Staging2 where industry is NULL OR industry = ''

   select* from LayoffsData_Staging2 where company = 'Airbnb'

-- 4.1: self join query to fill in missing industry values based on matching company names
   
   select * from LayoffsData_Staging2 st1 
   join LayoffsData_Staging2 st2 
   on st1.company = st2.company and st1.location = st2.location
   where st1.industry is null and st2.industry is not null

   update st1 
   set st1.industry = st2.industry
   from LayoffsData_Staging2 st1
   join LayoffsData_Staging2 st2 
       on st1.company = st2.company  
   where st1.industry is null 
     and st2.industry is not null

-- 4.2: total and percentage laid offs as null to be removed

   select * from LayoffsData_Staging2
   where total_laid_off = 'null' and percentage_laid_off = 'null'

   delete from LayoffsData_Staging2
   where total_laid_off = 'null' and percentage_laid_off = 'null'


-------- Data cleaning complete-------------------------------------------------------------------------------------------------------------------

   

 