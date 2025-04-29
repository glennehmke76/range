DO $$
DECLARE
    v_sp_id integer;
BEGIN
    -- First, truncate or create the range_line table if needed
    -- Uncomment the next line if you want to clear the table before inserting
    TRUNCATE TABLE range_line;
    
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

        -- Log progress
        RAISE NOTICE 'Processed sp_id: %', v_sp_id;

    END LOOP;

    -- Commit the transaction
    COMMIT;

    -- Log completion
    RAISE NOTICE 'Processing completed for all coastal species';
END $$;
