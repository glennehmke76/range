DROP PROCEDURE IF EXISTS r_clip_hulls;
CREATE OR REPLACE PROCEDURE r_clip_hulls(
    p_sp_id integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_row_count INTEGER;
    v_column_list TEXT;
    v_sql TEXT;
BEGIN
    -- Input validation
    IF p_sp_id IS NULL THEN
        RAISE EXCEPTION 'Species ID cannot be null';
    END IF;

    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Check if schema exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = v_schema_name) THEN
        RAISE EXCEPTION 'Schema % does not exist', v_schema_name;
    END IF;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Check if processed_hulls table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = v_schema_name AND table_name = 'processed_hulls') THEN
        RAISE EXCEPTION 'Table processed_hulls does not exist in schema %', v_schema_name;
    END IF;

    -- Check if region_continental table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'public' AND table_name = 'region_continental') THEN
        RAISE EXCEPTION 'Table region_continental does not exist in public schema';
    END IF;

    -- Properly format the SQL with schema name
    EXECUTE format('
        DROP TABLE IF EXISTS %I.clipped_range', v_schema_name);

    EXECUTE format('
        CREATE TABLE %I.clipped_range AS
        WITH continent AS (
            SELECT
                id,
                ST_MakeValid(ST_Simplify(geom, 0.01)) AS geom
            FROM region_continental
        )
        SELECT
            processed_hulls.sp_id,
            processed_hulls.taxon_id_r,
            processed_hulls.class,
            processed_hulls.br_class,
            ST_Intersection(
                ST_MakeValid(processed_hulls.geom),
                continent.geom
            ) AS geom
        FROM processed_hulls
        JOIN continent ON ST_Intersects(
            ST_MakeValid(processed_hulls.geom), continent.geom)
        WHERE processed_hulls.sp_id = $1',
        v_schema_name) USING p_sp_id;

END;
$$;

-- CALL r_clip_hulls(652);


rl_402.processed_hulls
