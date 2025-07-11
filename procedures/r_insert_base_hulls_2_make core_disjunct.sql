DROP PROCEDURE IF EXISTS r_insert_base_hulls_core_disjunct;
CREATE OR REPLACE PROCEDURE r_insert_base_hulls_core_disjunct(
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
    INSERT INTO rl_%1$s.base_hulls (hull_type, alpha, sp_id, class, geom)
    WITH disjunct AS
      (SELECT
        disjunct_pop_id,
        ST_Multi(
          ST_AlphaShape(
            ST_Collect(sightings.geom),
          %2$s, false)) AS hull
      FROM sightings
      WHERE
        (class_specified IS NULL OR class_specified NOT IN (0, 4, 9))
        AND disjunct_pop_id IS NOT NULL
      GROUP BY
        disjunct_pop_id
      )
    SELECT
      %3$L AS hull_type,
      %2$s AS alpha,
      %4$s AS sp_id,
      %5$s AS range_class,
      ST_Union(disjunct.hull) AS hull
    FROM disjunct
    ;',
    p_sp_id,       -- %1$s for schema name
    p_alpha,       -- %2$s for alpha value
    p_hull_type,   -- %3$L for hull_type (using L to properly escape string values)
    p_sp_id,       -- %4$s for sp_id
    p_class        -- %5$s for class
    );

    RAISE NOTICE 'Inserted hull for sp_id: %, hull_type: %, alpha: %, class: %',
                 p_sp_id, p_hull_type, p_alpha, p_class;

    COMMIT;
END;
$$;

CALL r_insert_base_hulls_core_disjunct(
    p_sp_id := 402,
    p_hull_type := 'alpha',
    p_alpha := 0.3,
    p_class := 1
);