CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.TWITTER (
LOG_DATE VARCHAR,
LOG_TIME VARCHAR,
MEDIA_TYPE VARCHAR,
MENTION_URL VARCHAR,
MENTION_CONTENT VARCHAR,
SENTIMENT VARCHAR,
COUNTRY VARCHAR
);

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.FORUMS (
LOG_DATE VARCHAR,
LOG_TIME VARCHAR,
MEDIA_TYPE VARCHAR,
SITE_NAME VARCHAR,
MENTION_URL VARCHAR,
TITLE VARCHAR,
MENTION_CONTENT VARCHAR,
SENTIMENT VARCHAR,
COUNTRY VARCHAR
);

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.TRUST_PILOT (
LOG_DATE VARCHAR,
LOG_TIME VARCHAR,
SITE_NAME VARCHAR,
MENTION_URL VARCHAR,
TITLE VARCHAR,
MENTION_CONTENT VARCHAR,
STAR_RATING VARCHAR
);

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.NEW_CASES (
BUSINESS_NAME VARCHAR,
BUSINESS_GROUP VARCHAR,
TOTAL_CASES VARCHAR,
BANKING_CREDIT VARCHAR,
MORTGAGES_HOME_FINANCE VARCHAR,
GENERAL_INSURANCE_PP VARCHAR,
PPI VARCHAR,
INVESTMENTS VARCHAR,
LIFE_PENSIONS VARCHAR
);		

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.RESOLVED_CASES (
BUSINESS_NAME VARCHAR,
BUSINESS_GROUP VARCHAR,
PERCENT_UPHELD VARCHAR,
RESOLVED_CASES VARCHAR,
PERCENT_BANKING_CREDIT VARCHAR,
PERCENT_MORTGAGES_HOME_FINANCE VARCHAR,
PERCENT_GENERAL_INSURANCE_PP VARCHAR,
PERCENT_PPI VARCHAR,
PPI_TOTAL VARCHAR,
PERCENT_INVESTMENTS VARCHAR,
PERCENT_LIFE_PENSIONS VARCHAR
);

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.EARLY_RESOLUTION (
BUSINESS_NAME VARCHAR,
BUSINESS_GROUP VARCHAR,
TOTAL_RESOLVED VARCHAR
);

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.FIRMS_OF_INTEREST (
MENTION_URL VARCHAR,
TWITTER_HANDLE VARCHAR
);

CREATE OR REPLACE TABLE MOTOR_INSURANCE.BRONZE.FIRM_DETAILS (
REFERENCE_NUMBER VARCHAR,
TWITTER_HANDLE VARCHAR,
BUSINESS_NAME VARCHAR,
BUSINESS_TYPE VARCHAR,
STATUS VARCHAR
);
