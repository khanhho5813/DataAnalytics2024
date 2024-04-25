CREATE SCHEMA IF NOT EXISTS "CRDB"; 
CREATE TABLE IF NOT EXISTS "CRDB".ussd17_edited (
    state VARCHAR(2),
    state_FIPS VARCHAR(2),
    DistrictID VARCHAR(5),
    NameSchoolDistrict VARCHAR(81),
    TotalPopulation VARCHAR(7),
    Population5_17 VARCHAR(7),
    Population5_17InPoverty VARCHAR(6)
);
COPY "CRDB".ussd17_edited FROM '/Users/abigailiovino/Documents/GitHub/DataAnalytics2024/data/ussd17/cleaned_ussd17_edited.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
