create procedure insert_base_hulls(
    p_sp_id integer,
    p_hull_type varchar,
    p_alpha real,
    p_class integer
)
language plpgsql
as
$$
BEGIN
    -- Construct schema name using the sp_id
    EXECUTE format('
        INSERT INTO rl_%s_base_hulls (hull_type, alpha, sp_id, class, geom)
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
