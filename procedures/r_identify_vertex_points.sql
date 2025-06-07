-- Create procedure to update sightings near polygon vertices with dynamic schema and configurable distance
DROP PROCEDURE IF EXISTS r_update_vertex_proximity;
CREATE OR REPLACE PROCEDURE r_update_vertex_proximity(
    p_sp_id INTEGER,
    distance_meters NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    schema_name TEXT;
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

    -- Set the dynamic schema path
    schema_name := format('rl_%s', p_sp_id::text);

    -- Check if the schema exists
    PERFORM 1
    FROM information_schema.schemata
    WHERE schema_name = format('rl_%s', p_sp_id::text);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Schema % does not exist', schema_name;
    END IF;

    -- Set search path
    EXECUTE format('SET search_path = %I, public', schema_name);

    -- Check if the sightings table exists
    PERFORM 1
    FROM information_schema.tables
    WHERE table_schema = schema_name
    AND table_name = 'sightings';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.sightings does not exist', schema_name;
    END IF;

    -- Check if polygons table exists (assuming it's called 'polygons')
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = schema_name
        AND table_name = 'polygons'
    ) INTO polygon_table_exists;

    IF NOT polygon_table_exists THEN
        RAISE EXCEPTION 'Table %.polygons does not exist. Please specify the correct polygon table name in the procedure.', schema_name;
    END IF;

    -- First, ensure the vertex_points column exists (add if it doesn't)
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = schema_name
        AND table_name = 'sightings'
        AND column_name = 'vertex_points'
    ) THEN
        EXECUTE format('ALTER TABLE %I.sightings ADD COLUMN vertex_points INTEGER', schema_name);
    END IF;

    -- Reset all values to NULL before updating
    EXECUTE format('UPDATE %I.sightings SET vertex_points = NULL', schema_name);

    -- Update sightings that are within the specified distance of any polygon vertex
    EXECUTE format('
        UPDATE %I.sightings s
        SET vertex_points = 1
        WHERE EXISTS (
            SELECT 1
            FROM (
                -- Extract vertices from polygons
                SELECT (ST_DumpPoints(p.geom)).geom AS vertex_geom
                FROM %I.polygons p
            ) AS vertices
            WHERE ST_DWithin(s.geom, vertices.vertex_geom, %s)
        )', schema_name, schema_name, distance_meters);

    -- Log the number of updated records
    EXECUTE format('SELECT COUNT(*) FROM %I.sightings WHERE vertex_points = 1', schema_name) INTO count_updated;

    RAISE NOTICE 'Updated % sightings as being within %m of polygon vertices in schema %',
        count_updated, distance_meters, schema_name;

    -- Reset search path
    SET search_path = public;
END;
$$;

-- Example execution
CALL r_update_vertex_proximity(223, 500);  -- For species ID 123, 500 meters



