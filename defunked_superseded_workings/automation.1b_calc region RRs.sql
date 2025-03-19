-- automation option 1 - vagrant/irregular range by reporting rate thresholds
  -- basins at sp level - 777 seconds on full geom vs 32 sec for simplified (using sp_id 728)
    -- make simplified regionalisation
    DROP TABLE IF EXISTS region_sibra_simple;
    CREATE TABLE region_sibra_simple (LIKE region_basins INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);

    -- take complex region with far too many vertices, project to GALCC > simplify > buffer to close any gaps (allows overalp which is ok) > simplify the buffer > transform back to GDA94 > cast as multipolygon
    -- substitute appropriate regionalisation for extent of taxon and adjust parameters to suit scale of regionalisation
    INSERT INTO region_sibra_simple (geom, id, name)
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
    FROM region_sibra
    ;

-- make regions with ranked percentile reporting rates
  -- switch regionalisaiton and sp_id in view subquery below
  DROP VIEW IF EXISTS region_rr;
  CREATE VIEW region_rr AS
  SELECT
        region_surveys_sightings.region_id AS region_id,
        region_surveys_sightings.num_sightings / region_surveys_sightings.num_surveys :: decimal * 100 AS reporting_rate,
        region_surveys_sightings.geom AS geom
      FROM
          (SELECT
            region_sibra_simple.id AS region_id,
            Count(survey.id) AS num_surveys,
            region_sightings.num_sightings,
            region_sibra_simple.geom
          FROM
              (SELECT
                region_sibra_simple.id AS region_id,
                Count(sighting.id) AS num_sightings
              FROM survey
              JOIN sighting ON survey.id = sighting.survey_id
              JOIN region_sibra_simple ON ST_Intersects(survey.geom,region_sibra_simple.geom)
              WHERE
                sighting.species_id = 621
                -- AND Extract(YEAR FROM survey.start_date) >= 1999 -- to limit to contemporary data
                -- etc
              GROUP BY
                region_sibra_simple.id
              )region_sightings
          JOIN region_sibra_simple ON region_sightings.region_id = region_sibra_simple.id
          JOIN survey ON ST_Intersects(survey.geom,region_sibra_simple.geom)
          GROUP BY
            region_sibra_simple.id,
            region_sightings.num_sightings,
            region_sibra_simple.geom
          )region_surveys_sightings
  ;
  SELECT
    region_rr.region_id,
    region_rr.geom,
    region_rr.reporting_rate,
    CASE
        WHEN region_rr.reporting_rate
                    < (SELECT
                        percentile_disc(0.05) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '5th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.1) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '10th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.2) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '20th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.3) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '30th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.4) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '40th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.5) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '50th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.6) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '60th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.7) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '70th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.8) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '80th percentile'
        WHEN reporting_rate
                    < (SELECT
                        percentile_disc(0.9) WITHIN GROUP (ORDER BY reporting_rate)
                      FROM region_rr
                      )
        THEN '90th percentile'                      
        ELSE '>90th percentile'
      END AS rr_class
  FROM region_rr
  GROUP BY
    region_rr.region_id,
    region_rr.geom,
    region_rr.reporting_rate
  ;