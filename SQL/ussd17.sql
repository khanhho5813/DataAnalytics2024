CREATE SCHEMA IF NOT EXISTS "CRDB"; 
CREATE TABLE "CRDB".ussd21 (
    StatePostalCode VARCHAR(2),
    StateFIPSCode VARCHAR(2),
    DistrictID VARCHAR(5),
    Name VARCHAR(69),
    EstimatedTotalPopulation VARCHAR(7),
    EstimatedPopulation5_17 VARCHAR(7),
    EstimatedChildrenPoverty VARCHAR(6)
);
COPY "CRDB".ussd21 FROM 'F:/Dropbox/!JBGCourses/!2024/2024 Data Analytics/github/data/cleaned_ussd21.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
