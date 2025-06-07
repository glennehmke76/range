DROP PROCEDURE IF EXISTS r_insert_infrequent_hull;
CREATE OR REPLACE PROCEDURE r_insert_infrequent_hull(
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
      INSERT INTO rl_%s.base_hulls (hull_type, alpha, sp_id, class, geom)
      WITH core_hull AS
        (SELECT
          hull_type,
          alpha,
          sp_id,
          class,
          geom
        FROM rl_%s.base_hulls
        WHERE
          sp_id = %s
          AND class = 1
          AND hull_type = %L
        ),
      overall_hull AS
        (SELECT
          sp_id,
          hull_type,
          alpha,
          class,
          geom
        FROM rl_%s.base_hulls
        WHERE
          sp_id = %s
          AND class = 0
          AND hull_type = %L
        )
      SELECT
        overall_hull.hull_type,
        overall_hull.alpha,
        overall_hull.sp_id,
        3 AS class,
        ST_Difference(overall_hull.geom, core_hull.geom) AS geom
      FROM core_hull
      JOIN overall_hull ON ST_Intersects(core_hull.geom, overall_hull.geom)',
        p_sp_id,      -- for target table schema
        p_sp_id,      -- for core_hull schema
        p_sp_id,      -- for core_hull sp_id
        p_hull_type,  -- for core_hull hull_type
        p_sp_id,      -- for overall_hull schema
        p_sp_id,      -- for overall_hull sp_id
        p_hull_type   -- for overall_hull hull_type
);

    RAISE NOTICE 'Inserted hull for sp_id: %, hull_type: %, alpha: %, class: %',
                 p_sp_id, p_hull_type, p_alpha, p_class;

    COMMIT;
END;
$$;


-- CALL r_insert_infrequent_hull(
--     p_sp_id := 223,
--     p_hull_type := 'alpha',
--     p_alpha := 2.5,
--     p_class := 1
-- );