-- Index on 'leaid' 
CREATE INDEX idx_LEAID_grf17_lea_tract ON "CRDB".grf17_lea_tract (LEAID);

-- Index on 'tract' 
CREATE INDEX idx_TRACT_grf17_lea_tract ON "CRDB".grf17_lea_tract (TRACT);

-- Composite index on 'state' and 'county' in 'aclf_data_national'
CREATE INDEX idx_STATE_COUNTY_texas_address ON texas_address (STATE, COUNTY);

-- Index on 'block_geoid' in 'aclf_data_national'
CREATE INDEX idx_BLOCK_GEOID_texas_address ON texas_address (BLOCK_GEOID);

-- Index on 'leaid' for 'lea_block_groups'
CREATE INDEX idx_LEAID_leaid ON "CRDB".grf17_lea_blkgrp (LEAID);



CREATE OR REPLACE FUNCTION get_blocks_in_district(district_id VARCHAR(7))
RETURNS TABLE (
    district VARCHAR(7),
    tracts_in_district_array VARCHAR[],
    num_tracts_in_district INT,
    lea_tract_count INT,
    all_blocks_in_tracts_in_district_array VARCHAR[],
    num_all_blocks_tracts_in_district INT,
    blocks_in_district_array VARCHAR[],
    num_blocks_in_district INT,
    lea_block_group_count INT
) AS
$$
BEGIN
    RETURN QUERY
    WITH DistrictTracts AS (
        SELECT
            leaid AS district,
            ARRAY_AGG(DISTINCT tract) AS tracts_in_district_array,
            COUNT(DISTINCT tract)::INT AS num_tracts_in_district,
            count AS lea_tract_count
        FROM
            school_tract_data
        WHERE
            leaid = district_id
        GROUP BY
            leaid, count
    ),
    TractBlocks AS (
	    SELECT
	        dt.district,
	        t.tract,
	        ARRAY_AGG(DISTINCT ab.block_geoid) AS blocks_in_tract_array
	    FROM
	        UNNEST((SELECT dt.tracts_in_district_array FROM DistrictTracts dt WHERE dt.district = district_id)) AS t(tract)
	    JOIN
	        aclf_data_national ab ON 
	            ab.state = SUBSTRING(t.tract::text, 1, 2) AND
	            ab.county = SUBSTRING(t.tract::text, 3, 3) AND
	            SUBSTRING(ab.block_geoid::text, 1, 11) = t.tract::text
	    CROSS JOIN
	        DistrictTracts dt
	    WHERE
	        dt.district = district_id
	    GROUP BY
	        dt.district, t.tract
	),
    AllBlocksInDistrict AS (
	    SELECT
	        dt.district,
	        ARRAY_AGG(DISTINCT block_geoid::VARCHAR) AS all_blocks_in_tracts_in_district_array,  -- Cast to VARCHAR
	        COUNT(DISTINCT block_geoid)::INT AS num_all_blocks_tracts_in_district
	    FROM
	        TractBlocks
	    JOIN
	        DistrictTracts dt ON dt.district = TractBlocks.district
	    CROSS JOIN
	        UNNEST(blocks_in_tract_array) AS block_geoid
	    GROUP BY
	        dt.district
	),
    DistrictBlocks AS (
        SELECT
            leaid AS district,
            ARRAY_AGG(DISTINCT blkgrp) AS blocks_in_district_array,
            COUNT(DISTINCT blkgrp)::INT AS num_blocks_in_district
        FROM
            lea_block_groups
        WHERE
            leaid = district_id
        GROUP BY
            leaid
    ),
    DistrictCounts AS (
        SELECT
            leaid AS district,
            count AS lea_block_group_count
        FROM
            lea_block_groups
        WHERE
            leaid = district_id
    )
    SELECT
        dt.district,
        dt.tracts_in_district_array,
        dt.num_tracts_in_district,
        dt.lea_tract_count,
        abid.all_blocks_in_tracts_in_district_array,
        abid.num_all_blocks_tracts_in_district, 
        db.blocks_in_district_array,
        db.num_blocks_in_district,
        dc.lea_block_group_count
    FROM
        DistrictTracts dt
    JOIN
        DistrictBlocks db ON dt.district = db.district
    JOIN
        DistrictCounts dc ON dt.district = dc.district
    JOIN
        AllBlocksInDistrict abid ON dt.district = abid.district;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_blocks_in_district('4808130');

