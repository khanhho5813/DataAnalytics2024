CREATE SCHEMA IF NOT EXISTS "CRDB"; 
CREATE TABLE IF NOT EXISTS "CRDB".grf17_lea_tract (
    LEAID VARCHAR(7),
    NAME_LEA17 VARCHAR(81),
    TRACT VARCHAR(11),
    COUNT VARCHAR(4),
    LANDAREA VARCHAR(22),
    WATERAREA VARCHAR(22)
);
COPY "CRDB".grf17_lea_tract FROM '/Users/abigailiovino/Documents/GitHub/DataAnalytics2024/data/GRF17/cleaned_grf17_lea_tract.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
CREATE TABLE IF NOT EXISTS "CRDB".grf17_lea_place (
    LEAID VARCHAR(7),
    NAME_LEA17 VARCHAR(81),
    PLACE VARCHAR(7),
    NAME_PLACE17 VARCHAR(57),
    COUNT VARCHAR(3),
    LANDAREA VARCHAR(22),
    WATERAREA VARCHAR(22)
);
COPY "CRDB".grf17_lea_place FROM '/Users/abigailiovino/Documents/GitHub/DataAnalytics2024/data/GRF17/cleaned_grf17_lea_place.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
CREATE TABLE IF NOT EXISTS "CRDB".grf17_lea_county (
    leaid VARCHAR(7),
    name_lea17 VARCHAR(81),
    stcounty VARCHAR(5),
    name_county17 VARCHAR(33),
    count VARCHAR(2),
    landarea VARCHAR(21),
    waterarea VARCHAR(22)
);
COPY "CRDB".grf17_lea_county FROM '/Users/abigailiovino/Documents/GitHub/DataAnalytics2024/data/GRF17/cleaned_grf17_lea_county.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
CREATE TABLE IF NOT EXISTS "CRDB".grf17_lea_blkgrp (
    LEAID VARCHAR(7),
    NAME_LEA17 VARCHAR(81),
    BLKGRP VARCHAR(12),
    COUNT VARCHAR(4),
    LANDAREA VARCHAR(22),
    WATERAREA VARCHAR(22)
);
COPY "CRDB".grf17_lea_blkgrp FROM '/Users/abigailiovino/Documents/GitHub/DataAnalytics2024/data/GRF17/cleaned_grf17_lea_blkgrp.csv' DELIMITER ',' CSV HEADER ENCODING 'windows-1251';
