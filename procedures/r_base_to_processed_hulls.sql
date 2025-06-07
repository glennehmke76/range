DROP PROCEDURE IF EXISTS r_base_to_processed_hulls;
CREATE OR REPLACE PROCEDURE r_base_to_processed_hulls(
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

    -- Create the processed_hulls table if it doesn't exist
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.processed_hulls (
            id SERIAL PRIMARY KEY,
            sp_id INTEGER NOT NULL,
            hull_type VARCHAR NOT NULL,
            alpha NUMERIC,
            class INTEGER NOT NULL,
            geom geometry(MultiPolygon, 4283) NOT NULL
        )', v_schema_name);

    -- Clear existing processed hulls for this species
    EXECUTE format('
        DELETE FROM %I.processed_hulls WHERE sp_id = $1
    ', v_schema_name) USING p_sp_id;

    -- Insert from base_hulls to processed_hulls
    EXECUTE format('
        INSERT INTO %I.processed_hulls (sp_id, hull_type, alpha, class, geom)
        SELECT
            sp_id,
            concat(hull_type, ''_'', alpha::text) AS hull_type,
            alpha,
            class,
            geom
        FROM %I.base_hulls
        WHERE class > 0
        AND sp_id = $1
    ', v_schema_name, v_schema_name)
    USING p_sp_id;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    IF v_row_count = 0 THEN
        RAISE WARNING 'No hulls were processed for species %', p_sp_id;
    ELSE
        RAISE NOTICE 'Successfully processed % hulls for species %', v_row_count, p_sp_id;
    END IF;

    -- Add new columns if they don't exist
    BEGIN
        EXECUTE format('
            ALTER TABLE %I.processed_hulls
            ADD COLUMN IF NOT EXISTS taxon_id_r varchar DEFAULT NULL,
            ADD COLUMN IF NOT EXISTS br_class integer DEFAULT 0
        ', v_schema_name);

        RAISE NOTICE 'Added taxon_id_r and br_class columns to processed_hulls table in schema %', v_schema_name;
    EXCEPTION
        WHEN duplicate_column THEN
            RAISE NOTICE 'One or both columns already exist in the table';
    END;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in base_to_processed_hulls: % %', SQLERRM, SQLSTATE;
END;
$$;

-- CALL r_base_to_processed_hulls(223);