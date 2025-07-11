DROP PROCEDURE IF EXISTS r_process_historic_hull_subtraction;
CREATE OR REPLACE PROCEDURE r_process_historic_hull_subtraction(
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

    -- Check if base_hulls table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = v_schema_name AND table_name = 'base_hulls') THEN
        RAISE EXCEPTION 'Table base_hulls does not exist in schema %', v_schema_name;
    END IF;

    -- Step 1 - union to make new historic zone (with additional subtractions)
    -- Create temporary table with same structure
    EXECUTE format('
        DROP TABLE IF EXISTS %I.base_hulls_tmp;
        CREATE TABLE %I.base_hulls_tmp AS
        SELECT * FROM %I.base_hulls WHERE 1=0;
    ', v_schema_name, v_schema_name, v_schema_name);

    -- Build and execute the dynamic INSERT query
    EXECUTE format('
        INSERT INTO %I.base_hulls_tmp (sp_id, hull_type, alpha, class, geom)
        SELECT
            sp_id,
            hull_type,
            alpha,
            class,
            ST_Multi(ST_Union(ST_Buffer(ST_Buffer(geom, 0.0000001), -0.0000001))) AS geom
        FROM %I.base_hulls
        WHERE
            class = 4
        GROUP BY
            sp_id,
            hull_type,
            alpha,
            class
    ', v_schema_name, v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Step 1: Dissolved % historic hulls into temporary table', v_row_count;

    -- Delete existing records for this class
    EXECUTE format('
        DELETE FROM %I.base_hulls
        WHERE class IN (SELECT class FROM %I.base_hulls_tmp)
    ', v_schema_name, v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Step 1: Deleted % existing historic hull records', v_row_count;

    -- Insert dissolved geometries back
    EXECUTE format('
        INSERT INTO %I.base_hulls (sp_id, hull_type, alpha, class, geom)
        SELECT
            sp_id,
            hull_type,
            alpha,
            class,
            geom
        FROM %I.base_hulls_tmp
    ', v_schema_name, v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Step 1: Inserted % dissolved historic hull records', v_row_count;

    -- Clean up
    EXECUTE format('DROP TABLE IF EXISTS %I.base_hulls_tmp', v_schema_name);

    -- Step 2 - subtract new historic zone from core
    EXECUTE format('
        DROP TABLE IF EXISTS %I.base_hulls_tmp;
        CREATE TABLE %I.base_hulls_tmp AS
        SELECT * FROM %I.base_hulls WHERE 1=0
    ', v_schema_name, v_schema_name, v_schema_name);

    -- Build and execute the dynamic INSERT query
    EXECUTE format('
        INSERT INTO %I.base_hulls_tmp (sp_id, hull_type, alpha, class, geom)
        WITH core_hull AS
          (SELECT
            hull_type,
            alpha,
            sp_id,
            class,
            geom
          FROM %I.base_hulls
          WHERE class = 1
          ),
        historic_hull AS
          (SELECT
            sp_id,
            hull_type,
            alpha,
            class,
            geom
          FROM %I.base_hulls
          WHERE class = 4
          )
        SELECT
          core_hull.sp_id,
          core_hull.hull_type,
          core_hull.alpha,
          1 AS class,
          ST_Union(ST_Difference(core_hull.geom, historic_hull.geom)) AS geom
        FROM core_hull
        JOIN historic_hull ON ST_Intersects(core_hull.geom, historic_hull.geom)
        GROUP BY
          core_hull.hull_type,
          core_hull.alpha,
          core_hull.sp_id,
          core_hull.class
    ', v_schema_name, v_schema_name, v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Step 2: Created % subtracted core hull records', v_row_count;

    -- Delete existing records for this class
    EXECUTE format('
        DELETE FROM %I.base_hulls
        WHERE class IN (SELECT class FROM %I.base_hulls_tmp)
    ', v_schema_name, v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Step 2: Deleted % existing core hull records', v_row_count;

    -- Insert modified geometries back
    EXECUTE format('
        INSERT INTO %I.base_hulls (sp_id, hull_type, alpha, class, geom)
        SELECT
            sp_id,
            hull_type,
            alpha,
            class,
            geom
        FROM %I.base_hulls_tmp
    ', v_schema_name, v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Step 2: Inserted % subtracted core hull records', v_row_count;

    -- Clean up
    EXECUTE format('DROP TABLE IF EXISTS %I.base_hulls_tmp', v_schema_name);

    RAISE NOTICE 'Successfully processed historic hull subtractions for species %', p_sp_id;

EXCEPTION
    WHEN OTHERS THEN
        -- Clean up on error
        EXECUTE format('DROP TABLE IF EXISTS %I.base_hulls_tmp', v_schema_name);
        RAISE EXCEPTION 'Error in r_process_historic_hull_subtraction: % %', SQLERRM, SQLSTATE;
END;
$$;

-- Example usage:
CALL r_process_historic_hull_subtraction(402);