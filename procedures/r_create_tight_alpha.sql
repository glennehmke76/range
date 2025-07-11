DROP PROCEDURE IF EXISTS r_create_regional_alpha_shapes;
CREATE OR REPLACE PROCEDURE r_create_regional_alpha_shapes(
  p_sp_id integer,
  p_hull_type varchar,
  p_alpha numeric,
  p_class integer,
  p_simplify_tolerance numeric DEFAULT 0.01
)
LANGUAGE plpgsql
AS $$

DECLARE
  v_schema_name TEXT;
  v_region_count INTEGER;

BEGIN
    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Create temporary table with simplified region geometries
    EXECUTE format('
        DROP TABLE IF EXISTS tmp_regions;
        CREATE TEMPORARY TABLE tmp_regions AS
        SELECT
            id AS region_id,
            ST_SimplifyPreserveTopology(geom, %s) AS geom
        FROM region_divisions
    ', p_simplify_tolerance, p_sp_id);

    -- Create index on the temporary table for better performance
    EXECUTE 'CREATE INDEX ON tmp_regions USING GIST(geom)';

    -- Get count of regions for logging
    EXECUTE 'SELECT COUNT(*) FROM tmp_regions' INTO v_region_count;
    RAISE NOTICE 'Created temporary table with % simplified regions', v_region_count;

    -- Create output table if it doesn't exist
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS rl_%s.regional_alpha (
        id SERIAL PRIMARY KEY,
        sp_id INTEGER NOT NULL,
        hull_type VARCHAR NOT NULL,
        alpha NUMERIC NOT NULL,
        class INTEGER NOT NULL,
        region_id INTEGER NOT NULL,
        geom geometry NOT NULL
    );', p_sp_id);

    -- Insert alpha shapes by region
    EXECUTE format('
        INSERT INTO rl_%s.regional_alpha (hull_type, alpha, sp_id, class, region_id, geom)
        SELECT
          %L AS hull_type,
          %s AS alpha,
          %s AS sp_id,
          %s AS class,
          r.region_id,
          ST_Multi(
              ST_AlphaShape(
                  ST_Collect(s.geom),
                  %s,
                  true
              )
          ) AS hull
        FROM rl_%s.sightings s
        JOIN tmp_regions r ON ST_Intersects(s.geom, r.geom)
        WHERE s.class_specified IS NULL
        AND EXISTS (
            SELECT 1
            FROM rl_%s.processed_hulls ph
            WHERE ph.class = %s
            AND ST_Intersects(s.geom, ph.geom)
        )
        GROUP BY r.region_id
    ',
    p_sp_id,           -- for table name
    p_hull_type,       -- hull_type
    p_alpha,           -- alpha value
    p_sp_id,           -- sp_id
    p_class,           -- class
    p_alpha,           -- alpha for ST_AlphaShape
    p_sp_id,           -- for sightings table
    p_sp_id,           -- for processed_hulls table
    p_class            -- class filter
    );

    -- Get count of created alpha shapes
    EXECUTE format('
        SELECT COUNT(*)
        FROM rl_%s.regional_alpha
        WHERE hull_type = %L AND class = %s
    ', p_sp_id, p_hull_type, p_class) INTO v_region_count;

    RAISE NOTICE 'Created % regional alpha shapes for sp_id: %, hull_type: %, alpha: %, class: %',
                 v_region_count, p_sp_id, p_hull_type, p_alpha, p_class;

    -- Clean up temporary table
    EXECUTE 'DROP TABLE IF EXISTS tmp_regions';

    COMMIT;
END;
$$;

-- Example call
CALL r_create_regional_alpha_shapes(
    p_sp_id := 385,
    p_hull_type := 'alpha',
    p_alpha := 0.5,
    p_class := 1,
    p_simplify_tolerance := 0.01
);
