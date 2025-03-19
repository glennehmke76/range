-- make hulls with reporting rates

-- notes
  -- runtimes for sp_ids
    -- basins, 621; 626sec (10mins)
    -- ibras, 621; 1174sec
  -- loading postgres layers into QGIS is way too slow - export as temp layers?

-- create base hulls 
  -- switch regionalisation in CTE
  -- switch sp_id in CTE and hull_sightings subquery
  -- if using subspecies use ...

DROP VIEW IF EXISTS hull_rr;
CREATE VIEW hull_rr AS
WITH base_hull AS
  (SELECT
    hull_sightings.region_id,
    ST_SetSRID(hull_sightings.hull,4283) AS geom
  FROM
      (SELECT
        -- sighting.species_id,
        region_sibra_simple.id AS region_id, -- or substitute other regionalisation
        ST_AsText
          (ST_ConvexHull -- or ST_ConcaveHull
            (ST_Collect
              (survey.geom))) AS hull
      FROM
          survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN region_sibra_simple ON ST_Intersects(survey.geom, region_sibra_simple.geom)
          -- if using subspecies
          -- JOIN range ON ST_Intersects(survey.geom,range.geom)
          -- JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
        WHERE
          sighting.species_id = 621
          -- if using subspecies
          -- wlab_range.taxon_id = 'u728b'
        GROUP BY
          -- sighting.species_id,
          region_sibra_simple.id
        -- exclude point geoms from coming through
        HAVING
          Count(DISTINCT survey.geom) >2
        )hull_sightings
  )
-- populate base hull reporting rates
  SELECT
    hull_surveys_sightings.region_id AS region_id,
    hull_surveys_sightings.num_sightings / hull_surveys_sightings.num_surveys :: decimal * 100 AS reporting_rate,
    hull_surveys_sightings.geom AS geom
  FROM
      (SELECT
        base_hull.region_id,
        Count(survey.id) AS num_surveys,
        hull_sightings.num_sightings,
        base_hull.geom
      FROM
          (SELECT
            base_hull.region_id,
            Count(sighting.id) AS num_sightings
          FROM survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN base_hull ON ST_Intersects(survey.geom,base_hull.geom)
          WHERE
            sighting.species_id = 621
            -- AND Extract(YEAR FROM survey.start_date) >= 1999 -- to limit to contemporary data
            -- etc
          GROUP BY
            base_hull.region_id
          )hull_sightings
      JOIN base_hull ON hull_sightings.region_id = base_hull.region_id
      JOIN survey ON ST_Intersects(survey.geom,base_hull.geom)
      GROUP BY
        base_hull.region_id,
        hull_sightings.num_sightings,
        base_hull.geom
      )hull_surveys_sightings
;

SELECT
  hull_rr.region_id,
  hull_rr.geom,
  hull_rr.reporting_rate,
  CASE
      WHEN hull_rr.reporting_rate
                  < (SELECT
                      percentile_disc(0.05) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '5th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.1) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '10th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.2) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '20th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.3) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '30th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.4) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '40th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.5) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '50th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.6) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '60th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.7) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '70th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.8) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '80th percentile'
      WHEN reporting_rate
                  < (SELECT
                      percentile_disc(0.9) WITHIN GROUP (ORDER BY reporting_rate)
                    FROM hull_rr
                    )
      THEN '90th percentile'
      ELSE '>90th percentile'
    END AS rr_class
  FROM hull_rr
  GROUP BY
  hull_rr.region_id,
  hull_rr.geom,
  hull_rr.reporting_rate
;