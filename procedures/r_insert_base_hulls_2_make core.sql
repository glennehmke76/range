DROP PROCEDURE IF EXISTS r_insert_core_hull_only;
CREATE OR REPLACE PROCEDURE r_insert_core_hull_only(
    p_sp_id integer,
    p_hull_type varchar,
    p_alpha numeric,
    p_class integer
)
LANGUAGE plpgsql
AS $$

DECLARE
  v_schema_name TEXT;

BEGIN
    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    EXECUTE format('
        INSERT INTO rl_%s.base_hulls (hull_type, alpha, sp_id, class, geom)
        WITH hull_sightings AS (
            SELECT
                sightings.sp_id,
                sightings.geom,
                Extract(YEAR FROM sightings.start_date) AS year
            FROM sightings
            WHERE
                sightings.sp_id = %s
                AND sightings.class_specified IS NULL
        )
        SELECT
            %L AS hull_type,
            %s AS alpha,
            hulls.sp_id,
            %s AS class,
            ST_Multi(
                ST_Union(
                    ST_SetSRID(hulls.hull,4283))) AS geom
        FROM (
            SELECT
                hull_sightings.sp_id,
                ST_Multi(
                    ST_AlphaShape(
                        ST_Collect(hull_sightings.geom),
                        %s, false)) AS hull
            FROM hull_sightings
            GROUP BY
                hull_sightings.sp_id
        ) hulls
        GROUP BY
            hulls.sp_id',
        p_sp_id,      -- for table name prefix
        p_sp_id,      -- for first WHERE condition
        p_hull_type,  -- for hull_type column
        p_alpha,      -- for alpha column
        p_class,      -- for class column
        p_alpha       -- for ST_AlphaShape parameter
    );

    RAISE NOTICE 'Inserted hull for sp_id: %, hull_type: %, alpha: %, class: %',
                 p_sp_id, p_hull_type, p_alpha, p_class;

    COMMIT;
END;
$$;

CALL r_insert_core_hull_only(
    p_sp_id := 402,
    p_hull_type := 'alpha',
    p_alpha := 2.5,
    p_class := 1
);