CREATE SCHEMA IF NOT EXISTS "CRDB"; 
CREATE TABLE IF NOT EXISTS "CRDB".ussd17_edited (
    state_postal_code VARCHAR(2),
    state_fips_code VARCHAR(2),
    district_id VARCHAR(5),
    name VARCHAR(81),
    estimated_total_population VARCHAR(7),
    estimated_population_5_17 VARCHAR(7),
    estimated_number_of_relevant_children_5_to_17_years_old_in_poverty_who_are_related_to_the_householder VARCHAR(6)
);
COPY "CRDB".ussd17_edited FROM '/Users/abigailiovino/Documents/GitHub/DataAnalytics2024/data/ussd17/cleaned_ussd17_edited.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
