DROP PROCEDURE IF EXISTS r_processed_hulls_add_elicitation;
CREATE OR REPLACE PROCEDURE r_processed_hulls_add_elicitation(
    p_sp_id integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_row_count INTEGER;
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

    -- Add new columns if they don't exist
    BEGIN
        EXECUTE format('
            ALTER TABLE %I.processed_hulls
            ADD COLUMN IF NOT EXISTS embellished smallint,
            ADD COLUMN IF NOT EXISTS rationale TEXT
        ', v_schema_name);

        RAISE NOTICE 'Added embellished and rationale columns to processed_hulls table in schema %', v_schema_name;
    EXCEPTION
        WHEN duplicate_column THEN
            RAISE NOTICE 'One or both columns already exist in the table';
    END;

END;
$$;

-- CALL r_processed_hulls_add_elicitation(223);
