-- 4.1 alteration is yearly constrained rrs

-- make overall hull 1st for regional clipper
-- overall hull as determined by alpha shape
  -- make overall hull first then subtract core
-- overall alpha shape
DROP TABLE IF EXISTS tmp_simple_region;
CREATE TABLE tmp_simple_region AS
SELECT
  id,
  ST_SimplifyPreserveTopology(geom, 0.01) AS geom
FROM region_basins
;
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_id
ON tmp_simple_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_geom
  ON tmp_simple_region USING gist (geom) TABLESPACE pg_default;
alter table tmp_simple_region
    add num_sightings integer;

DROP TABLE IF EXISTS tmp_constrained_region;
CREATE TABLE tmp_constrained_region AS
SELECT
  tmp_simple_region.id,
  ST_Intersection(tmp_simple_region.geom, base_hulls.geom) AS geom
FROM tmp_simple_region
JOIN base_hulls ON ST_Intersects(tmp_simple_region.geom, base_hulls.geom)
WHERE
  base_hulls.class = 0
  AND base_hulls.sp_id = 634
--   AND regionalisation = 'sibra'
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_region_id
ON tmp_constrained_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_constrained_region USING gist (geom) TABLESPACE pg_default;

-- create region table and populate global statistics
  -- remember to set predicates so existing rrs are not updated... or do this in a temp table then add it to the master...
-- CTEs produce an insert error (columns more than expressions) for some reason - som temp tables used
-- 634 execution: execution: 4 m 48 s 786 ms
CREATE TEMPORARY TABLE surveys_by AS
  (SELECT
    tmp_constrained_region.id AS region_id,
    extract(year from survey.start_date) AS year,
--       extract(month from survey.start_date) AS month,
    COUNT(survey.id) AS num_surveys
  FROM survey
  JOIN source ON survey.source_id = source.id
  JOIN survey_type ON survey.survey_type_id = survey_type.id
  JOIN tmp_constrained_region ON ST_Intersects(survey.geom, tmp_constrained_region.geom)
  WHERE
    survey.start_date BETWEEN '1999-01-11' AND '2024-12-31'
--       AND
--         (survey.survey_type_id = 1
--         OR survey.survey_type_id = 2
--         )
  GROUP BY
    tmp_constrained_region.id,
    extract(year from survey.start_date)
  );
CREATE TEMPORARY TABLE sightings_by AS
  (SELECT
    tmp_constrained_region.id AS region_id,
    extract(year from survey.start_date) AS year,
    sighting.sp_id,
--       extract(month from survey.start_date) AS month,
    COUNT(survey.id) AS num_sightings
  FROM survey
  JOIN sighting
    ON survey.id = sighting.survey_id
    AND survey.data_source = sighting.data_source
  JOIN source ON survey.source_id = source.id
  JOIN survey_type ON survey.survey_type_id = survey_type.id
  JOIN tmp_constrained_region ON ST_Intersects(survey.geom, tmp_constrained_region.geom)
  WHERE
    survey.start_date BETWEEN '1999-01-11' AND '2024-12-31'
--       AND
--         (survey.survey_type_id = 1
--         OR survey.survey_type_id = 2
--         )
    AND sighting.sp_id = 634
  GROUP BY
    tmp_constrained_region.id,
    sighting.sp_id,
    extract(year from survey.start_date)
  );
INSERT INTO range_region (geom, regionalisation, region_id, sp_id, num_surveys, num_sightings, num_sighting_years, mean_yearly_rr)
  SELECT
    ST_Multi(ST_Union(tmp_constrained_region.geom)) AS geom,
   'basins_clipped' AS regionalisation,
    yearly.region_id,
    yearly.sp_id,
    SUM(yearly.num_surveys) AS num_surveys,
    SUM(yearly.num_sightings) AS num_sightings,
    COUNT(yearly.year) AS num_sighting_years,
    AVG(yearly.mean_yearly_rr) AS mean_yearly_rr
  FROM
      (SELECT
        surveys_by.region_id,
        sightings_by.sp_id,
        surveys_by.year,
        surveys_by.num_surveys,
        sightings_by.num_sightings,
        sightings_by.num_sightings / surveys_by.num_surveys :: decimal * 100 AS mean_yearly_rr
      FROM sightings_by
      JOIN surveys_by
        ON sightings_by.region_id = surveys_by.region_id
        AND sightings_by.year = surveys_by.year
      )yearly
  JOIN tmp_constrained_region ON yearly.region_id = tmp_constrained_region.id
  GROUP BY
  yearly.region_id,
  yearly.sp_id
;

UPDATE range_region
SET global_rr = num_sightings / num_surveys :: decimal * 100;

-- set percentiles
UPDATE range_region
  SET mean_yearly_rr_percentile = CASE
    WHEN mean_yearly_rr <
          (SELECT
            percentile_disc(0.05) WITHIN GROUP (ORDER BY mean_yearly_rr)
          FROM range_region)
    THEN 5
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.1) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 10
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.2) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 20
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.3) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 30
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.4) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 40
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.5) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 50
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.6) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 60
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.7) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 70
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.8) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 80
    WHEN mean_yearly_rr <
            (SELECT
              percentile_disc(0.9) WITHIN GROUP (ORDER BY mean_yearly_rr)
            FROM range_region)
    THEN 90
    ELSE 100
  END
  WHERE
    regionalisation = 'basins_clipped'
    AND sp_id = 634
;

UPDATE range_region
  SET global_rr_percentile = CASE
    WHEN global_rr <
          (SELECT
            percentile_disc(0.05) WITHIN GROUP (ORDER BY global_rr)
          FROM range_region)
    THEN 5
    WHEN global_rr <
            (SELECT
              percentile_disc(0.1) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 10
    WHEN global_rr <
            (SELECT
              percentile_disc(0.2) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 20
    WHEN global_rr <
            (SELECT
              percentile_disc(0.3) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 30
    WHEN global_rr <
            (SELECT
              percentile_disc(0.4) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 40
    WHEN global_rr <
            (SELECT
              percentile_disc(0.5) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 50
    WHEN global_rr <
            (SELECT
              percentile_disc(0.6) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 60
    WHEN global_rr <
            (SELECT
              percentile_disc(0.7) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 70
    WHEN global_rr <
            (SELECT
              percentile_disc(0.8) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 80
    WHEN global_rr <
            (SELECT
              percentile_disc(0.9) WITHIN GROUP (ORDER BY global_rr)
            FROM range_region)
    THEN 90
    ELSE 100
  END
  WHERE
    regionalisation = 'basins_clipped'
    AND sp_id = 634
;
