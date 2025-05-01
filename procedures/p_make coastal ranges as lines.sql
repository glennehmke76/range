CREATE OR REPLACE PROCEDURE process_coastal_range_lines()
LANGUAGE plpgsql
AS $$
DECLARE
    v_sp_id integer;
    v_count integer := 0;
BEGIN
    -- Loop through each species ID where coastal_range = 1
    FOR v_sp_id IN
        SELECT sp_id
        FROM wlist_sp
        WHERE coastal_range = 1
        ORDER BY sp_id
    LOOP
        -- Insert records for current species
        INSERT INTO range_line (sp_id, taxon_id_r, class, br_class, geom)
        SELECT
            range.sp_id,
            range.taxon_id_r,
            range.class,
            range.br_class,
            ST_Union(ST_Intersection(range.geom, region_coastline_simple.geom)) AS geom
        FROM range
        JOIN region_coastline_simple ON ST_Intersects(range.geom, region_coastline_simple.geom)
        JOIN wlist_sp ON range.sp_id = wlist_sp.sp_id
        WHERE wlist_sp.sp_id = v_sp_id
        GROUP BY
            range.sp_id,
            range.taxon_id_r,
            range.class,
            range.br_class;

        v_count := v_count + 1;
        RAISE NOTICE 'Processed sp_id: % (% sp_id processed)', v_sp_id, v_count;

    END LOOP;

    -- Commit the transaction
    COMMIT;

    RAISE NOTICE 'Processing completed. Total sp_id processed: %', v_count;
END;
$$;
