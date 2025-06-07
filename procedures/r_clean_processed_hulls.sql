DROP PROCEDURE IF EXISTS r_clean_processed_hulls;
CREATE OR REPLACE PROCEDURE r_clean_processed_hulls(
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

    -- Get column list excluding 'id' and 'geom'
    WITH ordered_columns AS (
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = v_schema_name
        AND table_name = 'processed_hulls'
        AND column_name NOT IN ('id', 'geom')
        ORDER BY ordinal_position
    )
    SELECT string_agg(quote_ident(column_name), ', ')
    INTO v_column_list
    FROM ordered_columns;

    -- Create temporary table with same structure
    EXECUTE format('
        CREATE TEMP TABLE processed_hulls_tmp AS
        SELECT * FROM %I.processed_hulls WHERE 1=0
    ', v_schema_name);

    -- Add serial id
    ALTER TABLE processed_hulls_tmp ADD COLUMN tmp_id SERIAL PRIMARY KEY;

    -- Build and execute the dynamic INSERT query
    v_sql := format('
        INSERT INTO processed_hulls_tmp (%s, geom)
        SELECT %s,
            ST_Multi(ST_Union(ST_Buffer(ST_Buffer(geom, 0.0000001), -0.0000001))) AS geom
        FROM %I.processed_hulls
        WHERE sp_id = $1
        GROUP BY %s',
        v_column_list,
        v_column_list,
        v_schema_name,
        v_column_list
    );

    EXECUTE v_sql USING p_sp_id;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Dissolved % hull groups into temporary table', v_row_count;

    -- Delete existing records for this species
    DELETE FROM processed_hulls
    WHERE sp_id = p_sp_id;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % existing hull records', v_row_count;

    -- Insert dissolved geometries back
    EXECUTE format('
        INSERT INTO %I.processed_hulls (%s, geom)
        SELECT %s, geom
        FROM processed_hulls_tmp
        WHERE sp_id = $1',
        v_schema_name,
        v_column_list,
        v_column_list
    ) USING p_sp_id;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    -- Clean up
    DROP TABLE IF EXISTS processed_hulls_tmp;

    IF v_row_count = 0 THEN
        RAISE WARNING 'No hulls were processed for species %', p_sp_id;
    ELSE
        RAISE NOTICE 'Successfully processed % hulls for species %', v_row_count, p_sp_id;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- Clean up on error
        DROP TABLE IF EXISTS processed_hulls_tmp;
        RAISE EXCEPTION 'Error in clean_processed_hulls: % %', SQLERRM, SQLSTATE;
END;
$$;

-- CALL r_clean_processed_hulls(223);



