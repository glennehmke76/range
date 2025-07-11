-- 4.1 alteration is yearly constrained trends

-- make overall hull 1st for regional clipper
-- overall hull as determined by alpha shape
  -- make overall hull first then subtract core
-- overall alpha shape
DROP TABLE IF EXISTS rl_634.tmp_simple_region;
CREATE TABLE rl_634.tmp_simple_region AS
SELECT
  id,
  ST_SimplifyPreserveTopology(geom, 0.01) AS geom
FROM region_sibra
;
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_id
ON rl_634.tmp_simple_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_geom
  ON rl_634.tmp_simple_region USING gist (geom) TABLESPACE pg_default;
alter table rl_634.tmp_simple_region
    add num_sightings integer;

-- 214 run 1,243,600 rows affected in 8 m 1 s 204 ms
DROP TABLE IF EXISTS rl_634.sightings;
CREATE TABLE rl_634.region_sightings AS
SELECT DISTINCT
  row_number() over () as id,
  tmp_simple_region.id region_id,
  survey.id AS survey_id,
  survey.data_source,
	survey.source_id,
	source.name AS source_name,
	survey.source_ref,
  coalesce(extract(year from survey.start_date),0) :: integer AS year,
	survey.start_date,
	survey.start_time,
	survey.finish_date,
	survey.duration_in_minutes,
	survey.survey_type_id,
	survey_type.name AS survey_type_name,
	sighting.id AS sighting_id,
	sighting.sp_id AS sp_id,
-- 	species.common_name,
-- 	species.scientific_name,
	sighting.individual_count AS count,
	sighting.breeding_activity_id,
	sighting.vetting_status_id,
	CASE -- add other vetting categories as required
    WHEN sighting.vetting_status_id = 3 THEN 0
	  ELSE NULL :: integer
  END AS class_specified,
	survey.geom
FROM survey
JOIN sighting
  ON survey.id = sighting.survey_id
  AND survey.data_source = sighting.data_source
JOIN source ON survey.source_id = source.id
JOIN survey_type ON survey.survey_type_id = survey_type.id
-- JOIN tmp_simple_region ON ST_Intersects(survey.geom, tmp_simple_region.geom)
WHERE
  sighting.sp_id = 214
;
alter table rl_634.sightings
    add constraint tmp_region_sightings_pk
        primary key (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_sightings_id
  ON rl_634.sightings (region_id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_sp_id
  ON rl_634.sightings (sp_id);
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_geom
  ON rl_634.sightings USING gist (geom) TABLESPACE pg_default;

-- vet out erroneous sightings (this is a loop - re-run above after update below)
          -- define data points to exclude in GIS (or code)
--           drop table if exists exclusions;
--           create table exclusions
--           (
--               sighting_id integer
--           );
--           copy exclusions FROM '/Users/glennehmke/MEGA/RangeLayers/range_paper_1/exemplars/634_exclusions.csv'
--           DELIMITER ',' CSV header;
--
--           -- import
--           -- define in sightings
--           UPDATE sightings
--           SET class_specified = 99
--           FROM exclusions
--           WHERE
--             sightings.sighting_id = exclusions.sighting_id
          ;

INSERT INTO rl_xxx_base_hulls (hull_type, alpha, sp_id, class, geom)
SELECT
  'alpha' AS hull_type,
  2.5 AS alpha,
  214 AS sp_id,
  0 AS class,
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(sightings.geom),
    2.5, false)) AS hull
FROM sightings
WHERE class_specified IS NULL
;

DROP TABLE IF EXISTS tmp_constrained_region;
CREATE TABLE tmp_constrained_region AS
SELECT
  tmp_simple_region.id,
  ST_Intersection(tmp_simple_region.geom, base_hulls.geom) AS geom
FROM tmp_simple_region
JOIN base_hulls ON ST_Intersects(tmp_simple_region.geom, base_hulls.geom)
WHERE
  base_hulls.class = 0
  AND base_hulls.sp_id = 214
--   AND regionalisation = 'sibra'
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_region_id
ON tmp_constrained_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_constrained_region USING gist (geom) TABLESPACE pg_default;

-- create region table and populate global statistics
  -- remember to set predicates so existing trends are not updated... or do this in a temp table then add it to the master...
-- CTEs produce an insert error (columns more than expressions) for some reason - some temp tables used
-- 214 execution: execution: 4 m 48 s 786 ms

-- ADD INDEXES?
  -- without (214) =


DROP TABLE IF EXISTS surveys_by;
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
INSERT INTO range_region (geom, regionalisation, region_id, sp_id, num_surveys, num_sightings, num_sighting_years, mean_yearly_rr)
  SELECT
    ST_Multi(ST_Union(tmp_constrained_region.geom)) AS geom,
   'sibra_clipped' AS regionalisation,
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
    regionalisation = 'sibra_clipped'
    AND sp_id = 214
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
    regionalisation = 'sibra_clipped'
    AND sp_id = 214
;

-- code non-core regions in range_region_rr
  -- example bulk update based on a parameter
  UPDATE range_region
  SET class = 3
  WHERE
    sp_id = 214
    AND regionalisation = 'sibra_clipped'
    AND mean_yearly_rr_percentile <=20`
  ;

-- core as alpha shape
  -- 214 took 2 m 35 s 165 ms
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    range_region.id AS region_id,
    sightings.sp_id,
    sightings.geom,
    Extract(YEAR FROM sightings.start_date) AS year
  FROM sightings
  JOIN range_region
    ON sightings.sp_id = range_region.sp_id
    AND ST_Intersects(sightings.geom, range_region.geom)
  WHERE
    sightings.sp_id = 214
    AND range_region.sp_id = 214
    AND range_region.regionalisation = 'sibra_clipped'
    AND range_region.class IS NULL -- as class = 1 (core)
  )
  SELECT
    'alpha' AS hull_type,
    'sibra' AS regionalisation,
    2.5 AS alpha,
    hulls.sp_id,
    1 AS class, -- ie core
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.sp_id,
        ST_Multi(
          ST_AlphaShape(
        ST_Collect(hull_sightings.geom),
          2.5, false)) AS hull
      FROM hull_sightings
      GROUP BY
        hull_sightings.sp_id
      )hulls
  GROUP BY
    hulls.sp_id
;

-- subtract core from alpha shape and add to base_hulls as non-core range class
INSERT INTO base_hulls (hull_type, alpha, sp_id, class, geom)
WITH core_hull AS
  (SELECT
    hull_type,
    alpha,
    sp_id,
    class,
    geom
  FROM base_hulls
  WHERE
    sp_id = 214
    AND class = 1 -- as core hull
    AND hull_type = 'alpha'
  ),
overall_hull AS
  (SELECT
    sp_id,
    hull_type,
    alpha,
    class,
    geom
  FROM base_hulls
  WHERE
    sp_id = 214
    AND class = 0 -- as overall hull
    AND hull_type = 'alpha'
  )
SELECT
  overall_hull.hull_type,
  overall_hull.alpha,
  overall_hull.sp_id,
  3 AS class, -- ie infrequent
  ST_Difference(overall_hull.geom, core_hull.geom) AS geom
FROM core_hull
JOIN overall_hull ON ST_Intersects(core_hull.geom, overall_hull.geom)
;

-- if vagrant range required
  -- delete previous overall hull????
--               DELETE FROM base_hulls
--               WHERE
--                 sp_id = 214
--                 AND class = 0
--               ;

  -- code legitimate vagrant sightings - re-code 99s to 9
  UPDATE sightings
  SET class_specified = 9
  WHERE
    sp_id = 214
    AND vetting_status_id = 2 -- ie accepted
  ;

  -- manually code other sightings as appropriate

-- make new overall hull including vagrancy
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    sightings.sp_id,
    sightings.geom
  FROM sightings
  WHERE
    sightings.class_specified < 9
  )
  SELECT
    'alpha' AS hull_type,
    'sibra' AS regionalisation,
    2.5 AS alpha,
    hulls.sp_id,
    0 AS class,
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.sp_id,
        ST_Multi(
          ST_AlphaShape(
        ST_Collect(hull_sightings.geom),
          2.5, false)) AS hull
      FROM hull_sightings
      GROUP BY
        hull_sightings.sp_id
      )hulls
  GROUP BY
    hulls.sp_id
;


-- alteration 26 April to make vagrant hull without creating an overall
INSERT INTO base_hulls (hull_type, alpha, sp_id, class, geom)
WITH overall_hull AS
    (SELECT
      'alpha' AS hull_type,
      2.5 AS alpha,
      hulls.sp_id,
      0 AS class,
      ST_Multi(
        ST_Union(
          ST_SetSRID(hulls.hull,4283))) AS geom
    FROM
        (SELECT
          hull_sightings.sp_id,
          ST_Multi(
            ST_AlphaShape(
          ST_Collect(hull_sightings.geom),
            2.5, false)) AS hull
        FROM hull_sightings
        GROUP BY
          hull_sightings.sp_id
        )hulls
    GROUP BY
      hulls.sp_id
  ),
non_vagrant_hull AS
  (SELECT
    ST_Union(geom) AS geom
  FROM base_hulls
  WHERE
    class BETWEEN 1 AND 3 -- as core + infrequent
    AND hull_type = 'alpha'
  )
SELECT
  overall_hull.hull_type,
  overall_hull.alpha,
  overall_hull.sp_id,
  9 AS class, -- ie vagrant
  ST_Difference(overall_hull.geom, non_vagrant_hull.geom) AS geom
FROM non_vagrant_hull
JOIN overall_hull ON ST_Intersects(non_vagrant_hull.geom, overall_hull.geom)
;










-- subtract core from alpha shape and add to base_hulls as non-core range class
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH non_vagrant_hull AS
  (SELECT
    ST_Union(geom) AS geom
  FROM base_hulls
  WHERE
    sp_id = 214
    AND class BETWEEN 1 AND 3-- as core + infrequent
    AND hull_type = 'alpha'
    AND regionalisation = 'sibra'
  ),
overall_hull AS
  (SELECT
    sp_id,
    hull_type,
    alpha,
    regionalisation,
    class,
    geom
  FROM base_hulls
  WHERE
    sp_id = 214
    AND class = 0 -- as new overall hull including vagrancy
    AND hull_type = 'alpha'
  )
SELECT
  overall_hull.hull_type,
  overall_hull.regionalisation,
  overall_hull.alpha,
  overall_hull.sp_id,
  9 AS class, -- ie vagrant
  ST_Difference(overall_hull.geom, non_vagrant_hull.geom) AS geom
FROM non_vagrant_hull
JOIN overall_hull ON ST_Intersects(non_vagrant_hull.geom, overall_hull.geom)
;
