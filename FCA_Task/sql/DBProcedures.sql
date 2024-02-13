CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_FIRMS_OF_INTEREST()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.BRONZE.FIRMS_OF_INTEREST;
  
  INSERT INTO MOTOR_INSURANCE.BRONZE.FIRMS_OF_INTEREST(MENTION_URL, TWITTER_HANDLE) 
  SELECT MENTION_URL, TRIM(CName,'*@:,;') TWITTER_HANDLE FROM (
	SELECT A.MENTION_URL, C.value::string AS CName
	FROM MOTOR_INSURANCE.BRONZE.TWITTER A,
	LATERAL FLATTEN(input=>split(MENTION_CONTENT, ' ')) C
	WHERE SUBSTR(CName,1,1) = '@'
	);

  RETURN 0;
END;

CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_TWITTER_DATA()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.SILVER.TWITTER;
  
INSERT INTO MOTOR_INSURANCE.SILVER.TWITTER 
(LOG_DATE_TIME, MEDIA_TYPE, MENTION_URL, MENTION_CONTENT, SENTIMENT, COUNTRY, DQ_SCORE)
SELECT TRY_TO_TIMESTAMP(SUBSTR(A.LOG_DATE,1,11) || SUBSTR(A.LOG_TIME,1,8),'YYYY-MM-DD HH24:MI:SS') 
AS LOG_DATE_TIME,
MEDIA_TYPE, MENTION_URL, MENTION_CONTENT, SENTIMENT, COUNTRY,
CASE WHEN SENTIMENT NOT IN ('negative','positive','neutral') THEN 2
    WHEN LOG_DATE_TIME IS NULL OR COUNTRY = 'Not available' THEN 1
    ELSE 0
END DQ_SCORE
FROM MOTOR_INSURANCE.BRONZE.TWITTER A;


  RETURN 0;
END;

CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_FIRM_DETAILS()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.SILVER.FIRM_DETAILS; 
INSERT INTO MOTOR_INSURANCE.SILVER.FIRM_DETAILS 
(REFERENCE_NUMBER,BUSINESS_NAME, TWITTER_HANDLE, BUSINESS_TYPE, STATUS)
SELECT REFERENCE_NUMBER, CASE WHEN LENGTH(SUBSTR(BUSINESS_NAME, 1, CHARINDEX('(',BUSINESS_NAME) - 1)) = 0 THEN BUSINESS_NAME ELSE TRIM(SUBSTR(BUSINESS_NAME, 1, CHARINDEX('(',BUSINESS_NAME) - 1)) END AS BUSINESS_NAME,
TWITTER_HANDLE, BUSINESS_TYPE, STATUS
FROM MOTOR_INSURANCE.BRONZE.FIRM_DETAILS
WHERE BUSINESS_TYPE = 'Firm';


  RETURN 0;
END;


CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_NEW_CASES()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.SILVER.NEW_CASES; 
  
INSERT INTO MOTOR_INSURANCE.SILVER.NEW_CASES (
BUSINESS_NAME, BUSINESS_GROUP, TOTAL_CASES, BANKING_CREDIT,
MORTGAGES_HOME_FINANCE, GENERAL_INSURANCE_PP, PPI, INVESTMENTS,
LIFE_PENSIONS)
SELECT BUSINESS_NAME, BUSINESS_GROUP, TOTAL_CASES, BANKING_CREDIT,
MORTGAGES_HOME_FINANCE, GENERAL_INSURANCE_PP, PPI, INVESTMENTS,
LIFE_PENSIONS
FROM MOTOR_INSURANCE.BRONZE.NEW_CASES
WHERE BUSINESS_GROUP <> 'TOTALS';	

  RETURN 0;
END;

CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_RESOLVED_CASES()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.SILVER.RESOLVED_CASES; 
  
INSERT INTO MOTOR_INSURANCE.SILVER.RESOLVED_CASES (
BUSINESS_NAME,BUSINESS_GROUP,PERCENT_UPHELD,RESOLVED_CASES,
PERCENT_BANKING_CREDIT,PERCENT_MORTGAGES_HOME_FINANCE,
PERCENT_GENERAL_INSURANCE_PP,PERCENT_PPI,
PPI_TOTAL,PERCENT_INVESTMENTS,PERCENT_LIFE_PENSIONS)
SELECT BUSINESS_NAME,BUSINESS_GROUP,PERCENT_UPHELD,RESOLVED_CASES,
PERCENT_BANKING_CREDIT,PERCENT_MORTGAGES_HOME_FINANCE,
PERCENT_GENERAL_INSURANCE_PP,PERCENT_PPI,
PPI_TOTAL,PERCENT_INVESTMENTS,PERCENT_LIFE_PENSIONS
FROM MOTOR_INSURANCE.BRONZE.RESOLVED_CASES
WHERE BUSINESS_GROUP IS NOT NULL;	

  RETURN 0;
END;

CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_TWITTER_STATS()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.GOLD.TWITTER;
  
INSERT INTO MOTOR_INSURANCE.GOLD.TWITTER 
(YEAR, YEAR_PART, DQ_SCORE, SENTIMENT, BUSINESS_NAME, SENTIMENT_COUNT)
SELECT 2022 YEAR, 1 YEAR_PART, A.DQ_SCORE, A.SENTIMENT, C.BUSINESS_NAME, COUNT(1) SENTIMENT_COUNT
FROM MOTOR_INSURANCE.SILVER.TWITTER A,
MOTOR_INSURANCE.BRONZE.FIRMS_OF_INTEREST B,
MOTOR_INSURANCE.SILVER.FIRM_DETAILS C
WHERE A.MENTION_URL = B.MENTION_URL
AND B.TWITTER_HANDLE = C.TWITTER_HANDLE
GROUP BY A.DQ_SCORE, A.SENTIMENT, C.BUSINESS_NAME;


  RETURN 0;
END;

CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_NEW_CASES_STATS()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.GOLD.NEW_CASES; 
  
INSERT INTO MOTOR_INSURANCE.GOLD.NEW_CASES (YEAR, YEAR_PART, 
BUSINESS_NAME, BUSINESS_GROUP, TOTAL_CASES, BANKING_CREDIT,
MORTGAGES_HOME_FINANCE, GENERAL_INSURANCE_PP, PPI, INVESTMENTS,
LIFE_PENSIONS)
SELECT 2022 YEAR, 1 YEAR_PART, BUSINESS_NAME, BUSINESS_GROUP, TOTAL_CASES, BANKING_CREDIT,
MORTGAGES_HOME_FINANCE, GENERAL_INSURANCE_PP, PPI, INVESTMENTS,
LIFE_PENSIONS
FROM MOTOR_INSURANCE.SILVER.NEW_CASES;	

  RETURN 0;
END;

CREATE OR REPLACE PROCEDURE MOTOR_INSURANCE.BRONZE.LOAD_RESOLVED_CASES_STATS()
RETURNS INTEGER NOT NULL
LANGUAGE SQL
AS
BEGIN

  TRUNCATE TABLE MOTOR_INSURANCE.GOLD.RESOLVED_CASES; 
  
INSERT INTO MOTOR_INSURANCE.GOLD.RESOLVED_CASES (YEAR, YEAR_PART, 
BUSINESS_NAME,BUSINESS_GROUP,PERCENT_UPHELD,RESOLVED_CASES,
PERCENT_BANKING_CREDIT,PERCENT_MORTGAGES_HOME_FINANCE,
PERCENT_GENERAL_INSURANCE_PP,PERCENT_PPI,
PPI_TOTAL,PERCENT_INVESTMENTS,PERCENT_LIFE_PENSIONS)
SELECT 2022 YEAR, 1 YEAR_PART, BUSINESS_NAME,BUSINESS_GROUP,PERCENT_UPHELD,RESOLVED_CASES,
PERCENT_BANKING_CREDIT,PERCENT_MORTGAGES_HOME_FINANCE,
PERCENT_GENERAL_INSURANCE_PP,PERCENT_PPI,
PPI_TOTAL,PERCENT_INVESTMENTS,PERCENT_LIFE_PENSIONS
FROM MOTOR_INSURANCE.SILVER.RESOLVED_CASES;	

  RETURN 0;
END;
                     