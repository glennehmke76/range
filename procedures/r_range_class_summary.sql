DROP PROCEDURE IF EXISTS r_range_class_summary;
CREATE OR REPLACE PROCEDURE r_range_class_summary(p_sp_id integer)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_row_count INTEGER;
    v_sql TEXT;
    v_total_area NUMERIC;
BEGIN
    -- Input validation
    IF p_sp_id IS NULL THEN
        RAISE EXCEPTION 'Species ID cannot be null';
    END IF;

    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Drop class_summary table if it exists before creating it
    EXECUTE format('DROP TABLE IF EXISTS %I.class_summary', v_schema_name);

    -- Create class_summary table
    EXECUTE format('
        CREATE TABLE %I.class_summary (
            id SERIAL PRIMARY KEY,
            sp_id INTEGER NOT NULL,
            taxon_id varchar,
            metric varchar,
            class varchar NOT NULL,
            area_sqkm NUMERIC(12,2) NOT NULL,
            perc_class NUMERIC(5,2)
        )
    ', v_schema_name);

    -- Calculate areas by class and populate the table
    EXECUTE format('
        INSERT INTO %I.class_summary (
            sp_id,
            metric,
            class,
            area_sqkm
        )
        SELECT
            %s AS sp_id,
            ''range'' AS metric,
            lut_class.description,
            ROUND(SUM(ST_Area(ST_Transform(ph.geom, 3112)) / 1000000)::NUMERIC, 2) AS area_sqkm
        FROM %I.processed_hulls ph
        JOIN lut_class ON ph.class = lut_class.id
        WHERE ph.geom IS NOT NULL
        GROUP BY
            lut_class.description
    ', v_schema_name, p_sp_id, v_schema_name);

    -- Get number of inserted rows
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows into %.class_summary', v_row_count, v_schema_name;

    -- Calculate total area per taxon_id for percentage calculations
    EXECUTE format('
        WITH totals AS (
            SELECT
                sp_id,
                SUM(area_sqkm) AS total_area
            FROM %I.class_summary
            WHERE sp_id = %s
            GROUP BY
                sp_id
        )
        UPDATE %I.class_summary cs
        SET perc_class = ROUND((cs.area_sqkm / t.total_area * 100)::NUMERIC, 2)
        FROM totals t
        WHERE cs.sp_id = %s
    ', v_schema_name, p_sp_id, v_schema_name, p_sp_id);

    -- Get number of updated rows
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Updated percentage for % rows in %.class_summary', v_row_count, v_schema_name;

    RAISE NOTICE 'Successfully created and populated class_summary for species ID: %', p_sp_id;
END;
$$;

CALL r_range_class_summary(402);


