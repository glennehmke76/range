DROP PROCEDURE IF EXISTS r_hull_differences;
CREATE OR REPLACE PROCEDURE r_hull_differences(
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

    -- Check if required tables exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = v_schema_name AND table_name = 'processed_hulls') THEN
        RAISE EXCEPTION 'Table processed_hulls does not exist in schema %', v_schema_name;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                  WHERE table_schema = v_schema_name AND table_name = 'base_hulls') THEN
        RAISE EXCEPTION 'Table base_hulls does not exist in schema %', v_schema_name;
    END IF;

    -- Drop the hull_differences table if it exists
    EXECUTE format('DROP TABLE IF EXISTS %I.hull_differences', v_schema_name);

    -- Create hull_differences table
    EXECUTE format('
        CREATE TABLE %I.hull_differences (
            id SERIAL PRIMARY KEY,
            sp_id INTEGER,
            class VARCHAR,
            difference VARCHAR,
            geom geometry(MultiPolygon, 4283)
        )', v_schema_name);

    -- Insert areas that are the same in both tables
    EXECUTE format('
        INSERT INTO %1$I.hull_differences (difference, geom)
        SELECT
            ''same'' as difference,
            ST_Multi(
                CASE
                    WHEN ST_GeometryType(ST_Intersection(ST_Union(b.geom), ST_Union(p.geom))) = ''ST_Polygon''
                    THEN ST_Intersection(ST_Union(b.geom), ST_Union(p.geom))
                    ELSE ST_CollectionExtract(ST_Intersection(ST_Union(b.geom), ST_Union(p.geom)), 3)
                END
            ) as geom
        FROM %1$I.base_hulls b
        JOIN %1$I.processed_hulls p ON ST_Intersects(b.geom, p.geom)
    ', v_schema_name)
    USING p_sp_id;

    -- Insert areas that exist in base_hulls but not in processed_hulls
    EXECUTE format('
        INSERT INTO %1$I.hull_differences (difference, geom)
        SELECT
            ''subtracted'' as difference,
            ST_Multi(
                CASE
                    WHEN ST_GeometryType(ST_Difference(ST_Union(b.geom), COALESCE(ST_Union(p.geom), ST_GeomFromText(''POLYGON EMPTY'', 4283)))) = ''ST_Polygon''
                    THEN ST_Difference(ST_Union(b.geom), COALESCE(ST_Union(p.geom), ST_GeomFromText(''POLYGON EMPTY'', 4283)))
                    ELSE ST_CollectionExtract(ST_Difference(ST_Union(b.geom), COALESCE(ST_Union(p.geom), ST_GeomFromText(''POLYGON EMPTY'', 4283))), 3)
                END
            ) as geom
        FROM %1$I.base_hulls b
        JOIN %1$I.processed_hulls p ON ST_Intersects(b.geom, p.geom)
    ', v_schema_name)
    USING p_sp_id;

    -- Insert areas that exist in processed_hulls but not in base_hulls
    EXECUTE format('
        INSERT INTO %1$I.hull_differences (difference, geom)
        SELECT
            ''added'' as difference,
            ST_Multi(
                CASE
                    WHEN ST_GeometryType(ST_Difference(ST_Union(p.geom), COALESCE(ST_Union(b.geom), ST_GeomFromText(''POLYGON EMPTY'', 4283)))) = ''ST_Polygon''
                    THEN ST_Difference(ST_Union(p.geom), COALESCE(ST_Union(b.geom), ST_GeomFromText(''POLYGON EMPTY'', 4283)))
                    ELSE ST_CollectionExtract(ST_Difference(ST_Union(p.geom), COALESCE(ST_Union(b.geom), ST_GeomFromText(''POLYGON EMPTY'', 4283))), 3)
                END
            ) as geom
        FROM %1$I.processed_hulls p
        JOIN %1$I.base_hulls b ON ST_Intersects(p.geom, b.geom)
    ', v_schema_name)
    USING p_sp_id;

    -- Remove any empty geometries
    EXECUTE format('
        DELETE FROM %I.hull_differences
        WHERE ST_IsEmpty(geom) OR geom IS NULL
    ', v_schema_name);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    RAISE NOTICE 'Created hull_differences table with % records', v_row_count;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in r_hull_differences: % %', SQLERRM, SQLSTATE;
END;
$$;

-- CALL r_hull_differences(223);