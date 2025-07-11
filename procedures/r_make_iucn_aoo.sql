DROP PROCEDURE IF EXISTS make_iucn_aoo;
CREATE OR REPLACE PROCEDURE make_iucn_aoo(
    p_sp_id integer,
    p_class integer,
    p_start_year integer,  -- This is the start year for the start_year
    p_source_srid integer DEFAULT 4383  -- Default to WGS84 (common for geographic data)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_has_data BOOLEAN;
    v_aoo_count INTEGER;
    v_field_name TEXT;
    v_class_description TEXT;
    v_max_year INTEGER;
    v_metric_name TEXT;
BEGIN
    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Set the search path to sp schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Check if processed_hulls table has data for the specified class
    EXECUTE format('
        SELECT EXISTS (
            SELECT 1 FROM processed_hulls
            WHERE class = %s
        )',
        p_class) INTO v_has_data;

    IF NOT v_has_data THEN
        RAISE NOTICE 'No data found for species ID: % with class: %', p_sp_id, p_class;
        RETURN;
    END IF;

    -- Get the class description
    EXECUTE 'SELECT description FROM lut_class WHERE id = $1' INTO v_class_description USING p_class;

    -- Construct the field name for the start_year
    v_field_name := 'aoo_' || p_start_year;

    -- 1. Add the aoo_grid_id column if it doesn't exist
    EXECUTE format('ALTER TABLE %I.sightings ADD COLUMN IF NOT EXISTS aoo_grid_id INTEGER;', v_schema_name);

    -- 2. Update with intersecting public.region_grid_aoo id (centralized)
    EXECUTE format($fmt$
        UPDATE %1$I.sightings s
        SET aoo_grid_id = rg.id
        FROM public.region_grid_aoo rg
        WHERE ST_Intersects(rg.geom, s.geom)
        AND s.aoo_grid_id IS NULL -- only set if not already set
    $fmt$, v_schema_name);

    -- Get the maximum year from sightings
    EXECUTE format('
        SELECT COALESCE(MAX(year), %s)
        FROM %I.sightings
        WHERE sp_id = %s AND year >= %s',
        p_start_year, v_schema_name, p_sp_id, p_start_year) INTO v_max_year;

    -- Create the dynamic metric name
    v_metric_name := format('AOO (%s-%s)', p_start_year, v_max_year);

    -- Check if class_summary exists
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS class_summary (
            id SERIAL PRIMARY KEY,
            sp_id INTEGER NOT NULL,
            metric varchar,
            taxon_id varchar,
            class varchar,
            area_sqkm NUMERIC(12,2),
            perc_class NUMERIC(5,2)
        )');

    -- Calculate AOO using public.region_grid_aoo and sightings for the start_year
    EXECUTE format('
        WITH sightings_in_start_year AS (
            SELECT id, aoo_grid_id
            FROM %I.sightings
            WHERE sp_id = %s AND year >= %s AND aoo_grid_id IS NOT NULL
        ),
        grid_in_hull AS (
            SELECT
                ph.class,
                COUNT(DISTINCT s.aoo_grid_id) AS grid_count
            FROM processed_hulls ph
            JOIN sightings_in_start_year s
              ON ST_Intersects(
                    ST_Centroid((SELECT geom FROM public.region_grid_aoo WHERE id = s.aoo_grid_id)),
                    ph.geom
                 )
            WHERE ph.class = %s
            GROUP BY ph.class
        )
        SELECT grid_count * 4 FROM grid_in_hull',
        v_schema_name, p_sp_id, p_start_year, p_class) INTO v_aoo_count;

    -- Insert a new row with the AOO value and class
    IF v_aoo_count IS NOT NULL THEN
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
                %L,
                %s
            )',
            p_sp_id, v_metric_name, v_class_description, v_aoo_count);

        RAISE NOTICE 'Added new row with metric %, class %, and area_sqkm % for species ID: %',
            v_metric_name, v_class_description, v_aoo_count, p_sp_id;
    ELSE
        RAISE NOTICE 'No AOO data calculated for start_year % for species ID: % with class: %',
            p_start_year, p_sp_id, p_class;
    END IF;
END;
$$;

CALL make_iucn_aoo(
    p_sp_id := 402,
    p_class := 1,
    p_start_year := 2010
);