-- regional RRs
  -- basins at sp level
  SELECT
    sub2.region_id,
    sub2.num_sightings / sub2.num_surveys :: decimal * 100 AS rr,
    sub2.geom
  FROM
      (SELECT
        region_basins.id AS region_id,
        Count(survey.id) AS num_surveys,
        sub.num_sightings,
        region_basins.geom
      FROM
          (SELECT
            region_basins.id AS region_id,
            Count(sighting.id) AS num_sightings
          FROM survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN region_basins ON ST_Intersects(survey.geom,region_basins.geom)
          WHERE
            sighting.species_id = 411
          GROUP BY
            region_basins.id
          )sub
    JOIN region_basins ON sub.region_id = region_basins.id
    JOIN survey ON ST_Intersects(survey.geom,region_basins.geom)
    GROUP BY
      region_basins.id,
      sub.num_sightings,
      region_basins.geom
    )sub2
  ;

-- ibras / sibras at sp level
  -- 162 sec for ibras with intersects
  -- xxxx sec for sibra = with intersects
  -- dWithin?
  SELECT
    sub2.region_id,
    sub2.num_sightings / sub2.num_surveys :: decimal * 100 AS rr,
    sub2.geom
  FROM
      (SELECT
        region_sibra.id AS region_id,
        Count(survey.id) AS num_surveys,
        sub.num_sightings,
        region_sibra.geom AS geom
      FROM
          (SELECT
            region_sibra.id AS region_id,
            Count(sighting.id) AS num_sightings
          FROM survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN region_sibra ON ST_Intersects(region_sibra.geom, survey.geom)
          -- JOIN region_sibra ON ST_DWithin(survey.geom,region_sibra.geom,0.005)
          WHERE
            sighting.species_id = 411
          GROUP BY
            region_sibra.id
          )sub
      JOIN region_sibra ON sub.region_id = region_sibra.id
      JOIN survey ON ST_Intersects(survey.geom,region_sibra.geom)
      -- JOIN survey ON ST_DWithin(survey.geom,region_sibra.geom,0.005)
      GROUP BY
        region_sibra.id,
        sub.num_sightings,
        region_sibra.geom
      )sub2
  ;

-- by ssp
  SELECT
    sub2.region_id,
    sub2.num_sightings / sub2.num_surveys :: decimal * 100 AS rr,
    sub2.geom
  FROM
      (SELECT
        region_sibra.id AS region_id,
        Count(survey.id) AS num_surveys,
        sub.num_sightings,
        ST_Simplify(region_sibra.geom, 0.05) AS geom
        -- ST_Simplify(region_sibra.geom :: geographic, 500) AS geom
      FROM
          (SELECT
            region_sibra.id AS region_id,
            Count(sighting.id) AS num_sightings
          FROM survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN region_sibra ON ST_Intersects(region_sibra.geom, survey.geom)
          JOIN range ON ST_Intersects(survey.geom, range.geom)
          JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
          -- JOIN region_sibra ON ST_DWithin(survey.geom,region_sibra.geom,0.005)
          WHERE
            sighting.species_id = 728
            AND wlab_range.taxon_id = 'u728b'
          GROUP BY
            region_sibra.id
          )sub
      JOIN region_sibra ON sub.region_id = region_sibra.id
      JOIN survey ON ST_Intersects(survey.geom,region_sibra.geom)
      -- JOIN survey ON ST_DWithin(survey.geom,region_sibra.geom,0.005)
      JOIN range ON ST_Intersects(survey.geom,range.geom)
      JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
      WHERE
        wlab_range.taxon_id = 'u728b'
      GROUP BY
        region_sibra.id,
        sub.num_sightings,
        region_sibra.geom
      )sub2
  ;

-- automation option 1 - vagrant/irregular range by reporting rate thresholds
  -- basins at sp level - 777 seconds on full geom vs 32 sec for simplified (using sp_id 728)
  -- because (apparently) you cannot have a simplified geometry and retain the original in the same table, use a simplified version of regionalisation
  -- ~11 sec

    -- make simplified regionalisation
    DROP TABLE IF EXISTS region_basins_simple;
    CREATE TABLE region_basins_simple (LIKE region_basins INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);

    -- take complex region with far too many vertices, project to GALCC > simplify > buffer to close any gaps (allows overalp which is ok) > simplify the buffer > transform back to GDA94 > cast as multipolygon
    -- substitute appropriate regionalisation for extent of taxon and adjust parameters to suit scale of regionalisation
    INSERT INTO region_basins_simple (geom, id, name)
    SELECT
      ST_Multi(
        ST_Transform(
          ST_Simplify(
            ST_Buffer(
              ST_Simplify(
                ST_Transform(
                  geom, 3112), 15000), 10000), 15000), 4283)),
      id,
      name
    FROM region_basins
    ;


    -- make temporary table of reporting rates per simplified/buffered regions 
    DROP TABLE IF EXISTS v_region_rr;
    CREATE TABLE v_region_rr(
      region_id int DEFAULT NULL,
      region_rr decimal DEFAULT NULL,
      suggested_irregular character varying(50),
      geom geometry(MultiPolygon,4283),
      CONSTRAINT v_region_rr_pkey PRIMARY KEY (region_id)
    );

    INSERT INTO v_region_rr (region_id, region_rr, suggested_irregular, geom)
    SELECT
      sub2.region_id AS region_id,
      sub2.num_sightings / sub2.num_surveys :: decimal * 100 AS region_rr,
      NULL,
      sub2.geom AS region_geom
    FROM
        (SELECT
          region_basins_simple.id AS region_id,
          Count(survey.id) AS num_surveys,
          sub.num_sightings,
          region_basins_simple.geom
        FROM
            (SELECT
              region_basins_simple.id AS region_id,
              Count(sighting.id) AS num_sightings
            FROM survey
            JOIN sighting ON survey.id = sighting.survey_id
            JOIN region_basins_simple ON ST_Intersects(survey.geom,region_basins_simple.geom)
            WHERE
              sighting.species_id = 728
              -- AND Extract(YEAR FROM survey.start_date) >= 1999 -- to limit to contemporary data
              -- etc
            GROUP BY
              region_basins_simple.id
            )sub
      JOIN region_basins_simple ON sub.region_id = region_basins_simple.id
      JOIN survey ON ST_Intersects(survey.geom,region_basins_simple.geom)
      GROUP BY
        region_basins_simple.id,
        sub.num_sightings,
        region_basins_simple.geom
      )sub2
    ;

    -- sugegst irregular zones based on various thresholds - in this case 5th 10th and 20th percentiles of RRs
    -- will probably change to ordinal numbers in the end
    UPDATE v_region_rr
    SET suggested_irregular = CASE
      WHEN region_rr < 
                  (SELECT
                    percentile_disc(0.05) WITHIN GROUP (ORDER BY region_rr)
                  FROM v_region_rr
                  )
      THEN 'are you kidding'
      WHEN region_rr < 
                  (SELECT
                    percentile_disc(0.1) WITHIN GROUP (ORDER BY region_rr)
                  FROM v_region_rr
                  )
      THEN 'very irrugular'
      WHEN region_rr <
                  (SELECT
                    percentile_disc(0.2) WITHIN GROUP (ORDER BY region_rr)
                  FROM v_region_rr
                  )
      THEN 'irrugular'
      ELSE ''
    END
    ;

    -- subtract chosen threshold from core range of automated sMCPs?


ST_Intersection
WHERE suggested_irregular > clcass 1 2 or 3
etc

or ST_Union

then 
rnge = 3
WHERE suggested_irregular >> whatever

-- or by ssp
    DROP TABLE IF EXISTS region_sibra_simple;
    CREATE TEMPORARY TABLE region_sibra_simple (LIKE region_sibra INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);

    INSERT INTO region_sibra_simple (geom, id, sub_name_7)
    SELECT
      ST_Transform(
        ST_Simplify(
          ST_Transform(
            geom, 3112), 15000), 4283),
      id,
      sub_name_7
    FROM region_sibra
    ;

    SELECT
      sub2.region_id AS region_id,
      sub2.num_sightings / sub2.num_surveys :: decimal * 100 AS region_rr,
      sub2.geom AS region_geom
    FROM
        (SELECT
          region_sibra_simple.id AS region_id,
          Count(survey.id) AS num_surveys,
          sub.num_sightings,
          region_sibra_simple.geom
        FROM
            (SELECT
              region_sibra_simple.id AS region_id,
              Count(sighting.id) AS num_sightings
            FROM survey
            JOIN sighting ON survey.id = sighting.survey_id
            JOIN region_sibra_simple ON ST_Intersects(survey.geom,region_sibra_simple.geom)
            JOIN range ON ST_Intersects(survey.geom, range.geom)
            JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
            WHERE
              sighting.species_id = 728
              AND wlab_range.taxon_id = 'u728b'
              -- AND Extract(YEAR FROM survey.start_date) >= 1999 -- to limit to contemporary data
            GROUP BY
              region_sibra_simple.id
            )sub
      JOIN region_sibra_simple ON sub.region_id = region_sibra_simple.id
      JOIN survey ON ST_Intersects(survey.geom,region_sibra_simple.geom)
      JOIN range ON ST_Intersects(survey.geom,range.geom)
      JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
      WHERE
        wlab_range.taxon_id = 'u728b'
      GROUP BY
        region_sibra_simple.id,
        sub.num_sightings,
        region_sibra_simple.geom
      )sub2
    ;




-- add to make hulls in one big transaction?
  -- perhaps with multiple suggested vag/irr options as rows in the one table?
  -- need to decide threshold for vag/irr - percentile?

  SELECT
    sub2.region_rr,
    HULLS.region_id,
    ST_Union(HULLS.geom, sub2.geom) AS unioned_geom
    +
  FROM
    HULLS, sub2


UPDATE target
SET suggested_rnge = CASE
  WHEN
    region_rr < some threshold
  THEN 3
  WHEN
    MAX of Extract(YEAR FROM survey.start_date) < 1990
  THEN 4

;



  UPDATE sighting
  SET species_id = CASE
    WHEN 
      ST_Intersects(survey_point.geom, range.geom)
      AND survey_point.id = survey.survey_point_id
      AND survey.id = sighting.survey_id
      AND sighting.sp_id = 728 -- the old species
      AND range.sp_id = 722 -- the new species
      THEN 722
    WHEN 
      ST_Intersects(survey_point.geom, range.geom)
      AND survey_point.id = survey.survey_point_id
      AND survey.id = sighting.survey_id
      AND sighting.sp_id = 728 -- the old species
      AND range.sp_id = 728 -- the new species
      THEN 728
    END
  ;


