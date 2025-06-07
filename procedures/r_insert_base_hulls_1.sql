DROP PROCEDURE IF EXISTS r_insert_base_hulls;
CREATE OR REPLACE PROCEDURE r_insert_base_hulls(
  p_sp_id integer,
  p_hull_type varchar,
  p_alpha numeric,
  p_class integer
)
LANGUAGE plpgsql
AS $$

DECLARE
  v_schema_name TEXT;

BEGIN
    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    EXECUTE format('
    CREATE TABLE IF NOT EXISTS rl_%s.base_hulls (
    id SERIAL PRIMARY KEY,
    sp_id INTEGER NOT NULL,
    hull_type VARCHAR NOT NULL,
    alpha NUMERIC NOT NULL,
    class INTEGER NOT NULL,
    geom geometry NOT NULL
    );', p_sp_id);

    EXECUTE format('
        INSERT INTO rl_%s.base_hulls (hull_type, alpha, sp_id, class, geom)
        SELECT
          %L AS hull_type,
          %s AS alpha,
          %s AS sp_id,
          %s AS class,
          ST_Multi(
              ST_AlphaShape(
                  ST_Collect(sightings.geom),
                  %s,
                  false
              )
          ) AS hull
        FROM rl_%s.sightings
        WHERE class_specified IS NULL
    ',
    p_sp_id, -- for schema name
    p_hull_type, -- hull_type
    p_alpha, -- alpha value
    p_sp_id, -- sp_id
    p_class, -- class
    p_alpha, -- alpha value again for ST_AlphaShape
    p_sp_id -- for schema name in FROM clause
    );

    RAISE NOTICE 'Inserted hull for sp_id: %, hull_type: %, alpha: %, class: %',
                 p_sp_id, p_hull_type, p_alpha, p_class;

    COMMIT;
END;
$$;

-- CALL r_insert_base_hulls(
--     p_sp_id := 223,
--     p_hull_type := 'alpha',
--     p_alpha := 2.5,
--     p_class := 0
-- );