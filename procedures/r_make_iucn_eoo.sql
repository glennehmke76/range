DROP PROCEDURE IF EXISTS make_iucn_eoo;
CREATE OR REPLACE PROCEDURE make_iucn_eoo(
    p_sp_id integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_eoo_area NUMERIC(12,2);
    v_row_count INTEGER;
    v_metric_name TEXT;
BEGIN
    -- Input validation
    IF p_sp_id IS NULL THEN
        RAISE EXCEPTION 'Species ID cannot be null';
    END IF;

    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Calculate the EOO (Extent of Occurrence) using convex hull
    EXECUTE '
        SELECT
            ROUND((ST_Area(ST_Transform(ST_ConvexHull(ST_Collect(ph.geom)), 3112)) / 1000000)::NUMERIC, 2) AS area_sqkm
        FROM processed_hulls ph
        WHERE ph.class IN (1, 5) AND ph.geom IS NOT NULL'
    INTO v_eoo_area;

    IF v_eoo_area IS NULL OR v_eoo_area = 0 THEN
        RAISE NOTICE 'No suitable data found for calculating EOO for species ID: %', p_sp_id;
        RETURN;
    END IF;

    -- Use a simple metric name without years
    v_metric_name := 'IUCN EOO';

    -- Insert into class_summary table with simple metric name
    EXECUTE format('
        INSERT INTO class_summary (
            sp_id,
            metric,
            class,
            area_sqkm
        )
        VALUES (
            %s,
            %L,
            ''core'',
            %s
        )',
        p_sp_id, v_metric_name, v_eoo_area);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    IF v_row_count > 0 THEN
        RAISE NOTICE 'Added IUCN EOO record for species ID: % with area: % sq km', p_sp_id, v_eoo_area;
    ELSE
        RAISE NOTICE 'Failed to insert IUCN EOO record for species ID: %', p_sp_id;
    END IF;

    -- Create a spatial table to store the EOO geometry (maintaining this functionality)
    EXECUTE format('DROP TABLE IF EXISTS iucn_eoo');

    EXECUTE format('
        CREATE TABLE iucn_eoo (
            id SERIAL PRIMARY KEY,
            sp_id INTEGER NOT NULL,
            geom GEOMETRY(POLYGON, 4283),
            area_sqkm NUMERIC(12,2) NOT NULL
        )');

    -- Insert new convex hull and calculate area using classes 1 and 5
    EXECUTE format('
        INSERT INTO iucn_eoo (
            sp_id,
            geom,
            area_sqkm
        )
        SELECT
            %s AS sp_id,
            ST_ConvexHull(ST_Collect(ph.geom)) AS geom,
            %s AS area_sqkm
        FROM processed_hulls ph
        WHERE ph.class IN (1, 5) AND ph.geom IS NOT NULL',
        p_sp_id, v_eoo_area);

    RAISE NOTICE 'Created IUCN EOO geometry table for species ID: %', p_sp_id;
END;
$$;

-- Example usage:
CALL make_iucn_eoo(402);
