DROP PROCEDURE IF EXISTS r_processed_to_master_range;
CREATE OR REPLACE PROCEDURE r_processed_to_master_range(
    p_sp_id integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_row_count INTEGER;
    v_deleted_count INTEGER;
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

    -- Check if processed_hulls table exists in source schema
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = v_schema_name AND table_name = 'processed_hulls') THEN
        RAISE EXCEPTION 'Table processed_hulls does not exist in schema %', v_schema_name;
    END IF;

    -- Check if range table exists in public schema
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'public' AND table_name = 'range') THEN
        RAISE EXCEPTION 'Table range does not exist in public schema';
    END IF;

    -- Delete existing records for this species from public.range
    DELETE FROM public.range WHERE sp_id = p_sp_id;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % existing records for species % from public.range', v_deleted_count, p_sp_id;

    -- Insert processed_hulls data into public.range
    EXECUTE format('
        INSERT INTO public.range (
            hull_type,
            sp_id,
            taxon_id_r,
            class,
            geom
        )
        SELECT
            hull_type,
            sp_id,
            taxon_id_r,
            class,
            geom
        FROM %I.processed_hulls
        WHERE sp_id = $1
    ', v_schema_name)
    USING p_sp_id;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    IF v_row_count = 0 THEN
        RAISE WARNING 'No records were transferred for species %', p_sp_id;
    ELSE
        RAISE NOTICE 'Successfully transferred % records for species % to public.range', v_row_count, p_sp_id;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in r_processed_hulls_add_elicitation: % %', SQLERRM, SQLSTATE;
END;
$$;

-- CALL r_processed_to_master_range(223);