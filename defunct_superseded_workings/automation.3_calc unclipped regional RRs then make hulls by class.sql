
-- unclipped region embellishment is no simple region or clipper in tmp_region instead make tmp_region here
  -- need to finish region continental to incl major islands
DROP TABLE IF EXISTS tmp_region;
CREATE TABLE tmp_region AS
WITH clipped_region AS
  (SELECT
    region_basins.id,
    ST_Union(ST_Intersection(region_basins.geom,region_continental.geom)) AS geom
  FROM region_basins
  JOIN region_continental ON ST_Intersects(region_basins.geom,region_continental.geom)
  GROUP BY
    region_basins.id
  )
SELECT
  id,
  ST_SimplifyPreserveTopology(geom, 0.01) AS geom
FROM clipped_region
;
CREATE INDEX IF NOT EXISTS idx_tmp_tmp_region_id
ON tmp_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_region USING gist (geom) TABLESPACE pg_default;
alter table tmp_region
    add num_sightings integer;

DROP TABLE IF EXISTS tmp_region_sightings;
CREATE TABLE tmp_region_sightings AS
SELECT DISTINCT
  row_number() over () id,
  tmp_region.id region_id,
  sighting.id sighting_id,
  survey_point.geom
FROM survey
JOIN sighting ON survey.id = sighting.survey_id
JOIN survey_point ON survey.survey_point_id = survey_point.id
JOIN tmp_region ON ST_Intersects(survey_point.geom,tmp_region.geom)
-- JOIN tmp_region ON ST_DWithin(survey_point.geom, tmp_region.geom, 0.05)
WHERE sighting.species_id = 574
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_sightings_id
  ON tmp_region_sightings (region_id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_region_sightings USING gist (geom) TABLESPACE pg_default;

UPDATE tmp_region
SET num_sightings = count.num_sightings
FROM
  (SELECT
    tmp_region_sightings.region_id,
    Count(tmp_region_sightings.sighting_id) AS num_sightings
  FROM tmp_region_sightings
  GROUP BY
    tmp_region_sightings.region_id
  )count
WHERE
  count.region_id = tmp_region.id
;

-- INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
-- SELECT
--   'alpha' AS hull_type,
--   'basins' AS regionalisation,
--   2.5 AS alpha,
--   574,
--   0 AS range_class,
--   ST_Multi(
--     ST_AlphaShape(
--   ST_Collect(tmp_region_sightings.geom),
--     2.5, false)) AS hull
-- FROM tmp_region_sightings
-- JOIN tmp_region ON tmp_region_sightings.region_id = tmp_region.id
-- WHERE
--   tmp_region.num_sightings > 4
-- ;

INSERT INTO range_region_rr (geom, regionalisation, region_id, sp_id, num_surveys, num_sightings, rr)
SELECT
  ST_Multi(region_surveys_sightings.geom),
  'basins',
  region_surveys_sightings.region_id,
  region_surveys_sightings.sp_id,
  region_surveys_sightings.num_surveys,
  region_surveys_sightings.num_sightings,
  region_surveys_sightings.num_sightings / region_surveys_sightings.num_surveys :: decimal * 100 AS rr
FROM
    (SELECT
      tmp_region.geom,
      tmp_region.id AS region_id,
      region_sightings.sp_id,
      Count(survey.id) AS num_surveys,
      region_sightings.num_sightings
    FROM
        (SELECT
          tmp_region.id AS region_id,
          sighting.species_id AS sp_id,
          Count(sighting.id) AS num_sightings
        FROM survey
        JOIN sighting ON survey.id = sighting.survey_id
        JOIN survey_point ON survey.survey_point_id = survey_point.id
        JOIN tmp_region ON ST_Intersects(survey_point.geom,tmp_region.geom)
        WHERE
          sighting.species_id = 547
       -- AND Extract(YEAR FROM survey.start_date) >= 1999 -- to limit to contemporary data
        GROUP BY
          tmp_region.id,
          sighting.species_id
        )region_sightings
    JOIN tmp_region ON region_sightings.region_id = tmp_region.id
    JOIN survey ON ST_Intersects(survey.geom,tmp_region.geom)
    GROUP BY
      tmp_region.geom,
      region_sightings.sp_id,
      tmp_region.id,
      region_sightings.num_sightings
    )region_surveys_sightings
;

-- remember to set predicate so existing trends are not updated... or do this in a temp table then add it to the master...
UPDATE range_region_rr
SET rr_percentile = CASE
  WHEN rr <
        (SELECT
          percentile_disc(0.05) WITHIN GROUP (ORDER BY rr)
        FROM range_region_rr)
  THEN 5
  WHEN rr <
          (SELECT
            percentile_disc(0.1) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 10
  WHEN rr <
          (SELECT
            percentile_disc(0.2) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 20
  WHEN rr <
          (SELECT
            percentile_disc(0.3) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 30
  WHEN rr <
          (SELECT
            percentile_disc(0.4) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 40
  WHEN rr <
          (SELECT
            percentile_disc(0.5) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 50
  WHEN rr <
          (SELECT
            percentile_disc(0.6) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 60
  WHEN rr <
          (SELECT
            percentile_disc(0.7) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 70
  WHEN rr <
          (SELECT
            percentile_disc(0.8) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 80
  WHEN rr <
          (SELECT
            percentile_disc(0.9) WITHIN GROUP (ORDER BY rr)
          FROM range_region_rr)
  THEN 90
  ELSE 100
END
WHERE
  regionalisation = 'basins'
  AND sp_id = 547
;

-- core hulls (sMCP) by class
  -- specify core RR threshold
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, core_rr_precentile, geom)
WITH hull_sightings AS
  (SELECT
    tmp_region.id AS region_id,
    sighting.species_id,
    sighting.id AS sighting_id,
    survey.geom,
    Extract(YEAR FROM survey.start_date) AS year
  FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  JOIN tmp_region ON ST_Intersects(survey.geom,tmp_region.geom)
  JOIN range
    ON sighting.species_id = range.sp_id
    AND ST_Intersects(survey.geom,range.geom)
  WHERE
    sighting.species_id = 574
    AND range.class = 1 -- in this case filtering to investigate how close this automation comes to what I originally did (albeit about 10 years ago on different data)
  )
  SELECT
    'sMCP_core' AS hull_type,
    'basins' AS regionalisation,
    NULL AS alpha,
    hulls.species_id,
    1 AS class,
    20 AS core_rr_precentile,
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.species_id,
        range_region_rr.region_id,
        NULL,
        ST_ConvexHull(
          ST_Collect(hull_sightings.geom)) AS hull
      FROM hull_sightings
      JOIN range_region_rr ON ST_Intersects(hull_sightings.geom, range_region_rr.geom)
      WHERE
        range_region_rr.regionalisation = 'basins'
        AND range_region_rr.rr_percentile >= 20
      GROUP BY
        hull_sightings.species_id,
        range_region_rr.region_id
        -- exclude point geoms from coming through
      HAVING
        Count(DISTINCT hull_sightings.geom) >2
      )hulls
  GROUP BY
    hulls.species_id
;
