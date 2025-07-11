-- Create procedure to update sightings near polygon vertices with dynamic schema and configurable distance
DROP PROCEDURE IF EXISTS r_update_vertex_proximity;
CREATE OR REPLACE PROCEDURE r_update_vertex_proximity(
    p_sp_id INTEGER,
    distance_meters NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    polygon_table_exists BOOLEAN;
    count_updated INTEGER;
BEGIN
    -- Validate input parameters
    IF p_sp_id IS NULL THEN
        RAISE EXCEPTION 'Species ID parameter cannot be NULL';
    END IF;

    IF distance_meters IS NULL OR distance_meters <= 0 THEN
        RAISE EXCEPTION 'Distance parameter must be a positive number';
    END IF;

    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Check if the schema exists
    PERFORM 1
    FROM information_schema.schemata
    WHERE schema_name = format('rl_%s', p_sp_id::text);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Schema % does not exist', v_schema_name;
    END IF;

    -- Set search path
    EXECUTE format('SET search_path = %I, public', v_schema_name);

    -- Check if the sightings table exists
    PERFORM 1
    FROM information_schema.tables
    WHERE table_schema = v_schema_name
    AND table_name = 'sightings';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.sightings does not exist', v_schema_name;
    END IF;

    -- Check if polygons table exists (assuming it's called 'polygons')
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = v_schema_name
        AND table_name = 'processed_hulls'
    ) INTO polygon_table_exists;

    IF NOT polygon_table_exists THEN
        RAISE EXCEPTION 'Table %.processed_hulls does not exist. Please specify the correct polygon table name in the procedure.', v_schema_name;
    END IF;

    -- First, ensure the vertex_point column exists (add if it doesn't)
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = v_schema_name
        AND table_name = 'sightings'
        AND column_name = 'vertex_point'
    ) THEN
        EXECUTE format('ALTER TABLE %I.sightings ADD COLUMN vertex_point INTEGER', v_schema_name);
    END IF;

    -- Reset all values to NULL before updating
    EXECUTE format('UPDATE %I.sightings SET vertex_point = NULL', v_schema_name);

    -- Update sightings that are within the specified distance of any polygon vertex
    EXECUTE format('
        UPDATE %I.sightings s
        SET vertex_point = 1
        WHERE EXISTS (
            SELECT 1
            FROM (
                -- Extract vertices from processed_hulls
                SELECT (ST_DumpPoints(p.geom)).geom AS vertex_geom
                FROM %I.processed_hulls p
            ) AS vertices
            WHERE ST_DWithin(ST_Transform(s.geom, 3112), ST_Transform(vertices.vertex_geom, 3112), %s)
        )', v_schema_name, v_schema_name, distance_meters);

    -- Log the number of updated records
    EXECUTE format('SELECT COUNT(*) FROM %I.sightings WHERE vertex_point = 1', v_schema_name) INTO count_updated;

    RAISE NOTICE 'Updated % sightings as being within %m of polygon vertices in schema %',
        count_updated, distance_meters, v_schema_name;

    -- Reset search path
    SET search_path = public;
END;
$$;

-- Example execution
CALL r_update_vertex_proximity(402, 500);  -- For species ID 123, 500 meters