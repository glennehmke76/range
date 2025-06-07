-- for now doing this in the public schema and with manually coded arguments

SET search_path TO public;

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

DROP TABLE IF EXISTS rl_214.tmp_constrained_region;
CREATE TABLE rl_214.tmp_constrained_region AS
SELECT
  tmp_simple_region.id,
  ST_Intersection(tmp_simple_region.geom, rl_214.base_hulls.geom) AS geom
FROM tmp_simple_region
JOIN rl_214.base_hulls ON ST_Intersects(tmp_simple_region.geom, rl_214.base_hulls.geom)
CREATE INDEX IF NOT EXISTS idx_tmp_region_region_id
ON tmp_constrained_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_constrained_region USING gist (geom) TABLESPACE pg_default;

-- runtime = 6,338 rows affected in 30 m 19 s 474 ms for sp_id 214
CREATE TEMPORARY TABLE surveys_by AS
-- calculate num species in total and in focal species bird group per survey for use as filter
WITH bird_group_count AS
  (SELECT
    survey.id as survey_id,
    COUNT(DISTINCT sighting.sp_id) FILTER (WHERE wlist_sp.bird_group = 'Wetland') AS bird_group_species,
    COUNT(DISTINCT sighting.sp_id) AS total_species
  FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  JOIN wlist_sp ON sighting.sp_id = wlist_sp.sp_id
  JOIN tmp_constrained_region ON ST_Intersects(survey.geom, tmp_constrained_region.geom)
  GROUP BY survey.id
  )
SELECT
  tmp_constrained_region.id AS region_id,
  extract(year from survey.start_date) AS year,
  COUNT(survey.id) AS num_surveys
FROM survey
JOIN source ON survey.source_id = source.id
JOIN sighting
  ON survey.id = sighting.survey_id
  AND survey.data_source = sighting.data_source
JOIN survey_type ON survey.survey_type_id = survey_type.id
JOIN tmp_constrained_region ON ST_Intersects(survey.geom, tmp_constrained_region.geom)
JOIN bird_group_count ON survey.id = bird_group_count.survey_id
WHERE
  extract(year from survey.start_date) >= 1999 AND extract(year from survey.finish_date) <= 2022
  AND
    (
    (bird_group_count.bird_group_species::float / NULLIF(bird_group_count.total_species, 0) * 100) >= 25 -- a filter to suppress inflated base-rates from unsuitable surveys as a percentage
    OR sighting.sp_id = 214 -- in order to ensure surveys containing the focal species are not excluded from the base rate
    )
  GROUP BY
    tmp_constrained_region.id,
    extract(year from survey.start_date)
;
DROP TABLE IF EXISTS sightings_by;
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
    AND sighting.sp_id = 214
  GROUP BY
    tmp_constrained_region.id,
    sighting.sp_id,
    extract(year from survey.start_date)
  );
CREATE TABLE rl_214.range_region_yearly AS
  SELECT
    ST_Multi(ST_Union(tmp_constrained_region.geom)) AS geom,
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
;
ALTER TABLE rl_214.range_region_yearly ADD mean_rr_percentile decimal;

UPDATE range_region
SET mean_yearly_rr_percentile = CASE
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.05) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 5
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.1) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 10
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.2) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 20
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.3) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 30
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.4) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 40
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 50
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.6) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 60
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.7) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 70
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.8) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 80
  WHEN mean_yearly_rr < (SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 90
  ELSE 100
END;