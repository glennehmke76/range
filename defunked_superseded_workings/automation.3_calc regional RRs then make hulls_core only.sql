DROP TABLE IF EXISTS tmp_region;
CREATE TABLE tmp_region AS
  SELECT
    *
  FROM region_basins
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_region_id
ON tmp_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_region USING gist (geom) TABLESPACE pg_default;

-- core hulls (sMCP) - no vagrant zones
-- 574 ~1.5 mins
-- with range limiter ~2 mins
-- trick procedure into using only current core records
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
    AND range.sp_id = 574
    AND range.class = 1 -- in this case filtering to investigate how close this automation comes to what I originally did (albeit about 10 years ago on different data).
  )
  SELECT
    'sMCP' AS hull_type,
    'basins' AS regionalisation,
    NULL AS alpha,
    hulls.species_id,
    1 AS class,
    NULL AS core_rr_precentile,
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.species_id,
        tmp_region.id,
        NULL,
        ST_ConvexHull(
          ST_Collect(hull_sightings.geom)) AS hull
      FROM hull_sightings
      JOIN tmp_region ON ST_Intersects(hull_sightings.geom, tmp_region.geom)
      -- if using subspecies
      -- JOIN range ON ST_Intersects(survey.geom,range.geom)
      -- JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
      GROUP BY
        hull_sightings.species_id,
        tmp_region.id
        -- exclude point geoms from coming through
      HAVING
        Count(DISTINCT hull_sightings.geom) >2
      )hulls
  GROUP BY
    hulls.species_id
;

-- core hulls (alpha)
  -- this routine is not the same as the python one you use James so we probably need to include that in place of this. Having said that, this produce similar results to my sMCP method (in some cases). But it still produces errors in some cases so the sMCP method will be the main one
-- 574 ~1 min
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
    AND range.sp_id = 574
    AND range.rnge = 1 -- in this case filtering to investigate how close this automation comes to what I originally did (albeit about 10 years ago on different data).
  )
SELECT
  'alpha' AS hull_type,
  'basins' AS regionalisation,
  2.5 AS alpha,
  hull_sightings.species_id,
  1 AS range_class,
  NULL AS core_rr_precentile,
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(hull_sightings.geom),
    2.5, false)) AS hull
FROM hull_sightings
JOIN tmp_region ON ST_Intersects(hull_sightings.geom, tmp_region.geom)
GROUP BY
  species_id
;
