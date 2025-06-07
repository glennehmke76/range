DROP PROCEDURE IF EXISTS r_insert_vagrant_hull;
CREATE OR REPLACE PROCEDURE r_insert_vagrant_hull(
    p_sp_id integer,
    p_hull_type varchar,
    p_alpha numeric
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_sighting_count INTEGER;
    v_non_vagrant_count INTEGER;
BEGIN
    -- Input validation
    IF p_sp_id IS NULL THEN
        RAISE EXCEPTION 'Species ID cannot be null';
    END IF;

    IF p_hull_type IS NULL THEN
        RAISE EXCEPTION 'Hull type cannot be null';
    END IF;

    IF p_alpha IS NULL OR p_alpha <= 0 THEN
        RAISE EXCEPTION 'Alpha must be a positive number';
    END IF;

    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Check if schema exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = v_schema_name) THEN
        RAISE EXCEPTION 'Schema % does not exist', v_schema_name;
    END IF;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Check if required tables exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = v_schema_name AND table_name = 'sightings') THEN
        RAISE EXCEPTION 'Sightings table does not exist in schema %', v_schema_name;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = v_schema_name AND table_name = 'base_hulls') THEN
        RAISE EXCEPTION 'Base_hulls table does not exist in schema %', v_schema_name;
    END IF;

    -- Check if we have any sightings
    SELECT COUNT(*) INTO v_sighting_count FROM sightings;
    IF v_sighting_count = 0 THEN
        RAISE EXCEPTION 'No sightings data found for species %', p_sp_id;
    END IF;

    -- Check if we have any non-vagrant hulls
    SELECT COUNT(*) INTO v_non_vagrant_count
    FROM base_hulls
    WHERE class BETWEEN 1 AND 3 AND hull_type = p_hull_type;

    IF v_non_vagrant_count = 0 THEN
        RAISE EXCEPTION 'No non-vagrant hulls found for species % with hull_type %', p_sp_id, p_hull_type;
    END IF;

    -- Main insertion
    INSERT INTO base_hulls (hull_type, alpha, sp_id, class, geom)
    WITH overall_hull AS (
        SELECT
            p_hull_type AS hull_type,
            p_alpha AS alpha,
            0 AS class,
            ST_Multi(
                ST_Union(
                    ST_SetSRID(hulls.hull, 4283)
                )
            ) AS geom
        FROM (
            SELECT
                ST_Multi(
                    ST_AlphaShape(
                        ST_Collect(sightings.geom),
                        p_alpha,  -- Use the parameter value instead of hardcoded 2.5
                        false
                    )
                ) AS hull
            FROM sightings
            WHERE geom IS NOT NULL  -- Ensure we only process valid geometries
        ) hulls
    ),
    non_vagrant_hull AS (
        SELECT ST_Union(geom) AS geom
        FROM base_hulls
        WHERE class BETWEEN 1 AND 3
        AND hull_type = p_hull_type
        AND geom IS NOT NULL
    )
    SELECT
        overall_hull.hull_type,
        overall_hull.alpha,
        p_sp_id AS sp_id,
        9 AS class,
        ST_Multi(ST_Difference(overall_hull.geom, non_vagrant_hull.geom)) AS geom
    FROM non_vagrant_hull
    JOIN overall_hull ON ST_Intersects(non_vagrant_hull.geom, overall_hull.geom);

    GET DIAGNOSTICS v_sighting_count = ROW_COUNT;

    IF v_sighting_count = 0 THEN
        RAISE WARNING 'No vagrant areas were identified for species %', p_sp_id;
    ELSE
        RAISE NOTICE 'Successfully inserted vagrant hull for sp_id: %, hull_type: %, alpha: %, class: 9',
                     p_sp_id, p_hull_type, p_alpha;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in insert_vagrant_hull: % %', SQLERRM, SQLSTATE;
END;
$$;

-- CALL r_insert_vagrant_hull(223, 'alpha', 20);  -- For species 123, alpha hull type, alpha value 2.5
