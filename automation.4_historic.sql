-- make overall hull 1st for regional clipper
-- overall hull as determined by alpha shape
  -- make overall hull first then subtract core
-- overall alpha shape
DROP TABLE IF EXISTS tmp_simple_region;
CREATE TABLE tmp_simple_region AS
SELECT
  id,
  ST_SimplifyPreserveTopology(geom, 0.01) AS geom
FROM region_sibra
;
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_id
ON tmp_simple_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_geom
  ON tmp_simple_region USING gist (geom) TABLESPACE pg_default;
alter table tmp_simple_region
    add num_sightings integer;

DROP TABLE IF EXISTS tmp_region_sightings;
CREATE TABLE tmp_region_sightings AS
SELECT DISTINCT
  row_number() over () as id,
  tmp_simple_region.id region_id,
  sighting.class_specified,
  survey.id AS survey_id,
	survey.source_id,
	source.name AS source_name,
	survey.source_ref,
-- 	st_y(survey_point.geom) AS latitude,
-- 	st_x(survey_point.geom) AS longitude,
  coalesce(extract(year from survey.start_date),0) :: integer AS year,
	survey.start_date,
	survey.start_time,
	survey.finish_date,
	survey.duration_in_minutes,
	survey.survey_type_id,
	survey_type.name AS survey_type_name,
	sighting.id AS sighting_id,
	-- sighting_location.lat AS sighting_latitude,
	-- sighting_location.lon AS sighting_longitude,
	sighting.sp_id AS sp_id,
-- 	species.common_name,
-- 	species.scientific_name,
	sighting.individual_count AS count,
	sighting.breeding_activity_id,
	breeding_activity.name AS breeding_activity_name,
	sighting.vetting_status_id,
	survey.geom AS geom
FROM survey
JOIN sighting ON survey.id = sighting.survey_id
LEFT JOIN source ON survey.source_id = source.id
LEFT JOIN survey_type ON survey.survey_type_id = survey_type.id
LEFT JOIN breeding_activity ON sighting.breeding_activity_id = breeding_activity.id
JOIN tmp_simple_region ON ST_DWithin(survey.geom, tmp_simple_region.geom, 0.05)
WHERE
  sighting.sp_id = 967
;
alter table public.tmp_region_sightings
    add constraint tmp_region_sightings_pk
        primary key (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_sightings_id
  ON tmp_region_sightings (region_id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_sp_id
  ON tmp_region_sightings (sp_id);
CREATE INDEX IF NOT EXISTS idx_tmp_simple_region_geom
  ON tmp_region_sightings USING gist (geom) TABLESPACE pg_default;

UPDATE tmp_simple_region
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
  count.region_id = tmp_simple_region.id
;

-- exclude outliers.... populate class_specified = 0 or non null value

INSERT INTO base_hulls (hull_type, alpha, sp_id, class, geom)
SELECT
  'alpha' AS hull_type,
  2.5 AS alpha,
  967 AS sp_id,
  0 AS range_class,
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(tmp_region_sightings.geom),
    2.5, false)) AS hull
FROM tmp_region_sightings
WHERE class_specified IS NULL
;



-- add disjunct population identifiers
alter table public.tmp_region_sightings
    add disjunct_pop_id integer;


-- core by region for disjunct
INSERT INTO base_hulls (hull_type, alpha, sp_id, class, geom)
WITH disjunct AS
  (SELECT
    disjunct_pop_id,
    ST_Multi(
      ST_AlphaShape(
    ST_Collect(tmp_region_sightings.geom),
      2.5, false)) AS hull
  FROM tmp_region_sightings
  WHERE
    class_specified IS NULL
    AND disjunct_pop_id IS NOT NULL
  GROUP BY
    disjunct_pop_id
  )
SELECT
  'alpha' AS hull_type,
  2.5 AS alpha,
  967 AS sp_id,
  1 AS range_class,
  ST_Union(disjunct.hull) AS hull
FROM disjunct
;


-- above with subtract core from alpha shape and add to base_hulls as non-core range class
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
-- still dodgy - better ways to do this - more dev reqd.
-- anyway returns the non-core regions so that works
WITH core_hull AS
  (SELECT
    hull_type,
    regionalisation,
    alpha,
    sp_id,
    class,
    geom
  FROM base_hulls
  WHERE
    sp_id = 967
    AND class = 1 -- as core hull
    AND hull_type = 'alpha'
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
    sp_id = 967
    AND class = 0 -- as overall hull
    AND hull_type = 'alpha'
  )
SELECT
  overall_hull.hull_type,
  core_hull.regionalisation,
  overall_hull.alpha,
  overall_hull.sp_id,
  4 AS class, -- ie historic
  ST_Difference(overall_hull.geom, core_hull.geom) AS geom
FROM core_hull
JOIN overall_hull ON ST_Intersects(core_hull.geom, overall_hull.geom)
;

-- insert final geoms - specify regionalisation and other predicates
INSERT INTO processed_hulls (hull_type, regionalisation, sp_id, class, geom)
SELECT
  concat(hull_type, '_', alpha) AS hull_type,
  regionalisation,
  sp_id,
  class,
  geom
FROM base_hulls
WHERE
  sp_id = 967
  AND class > 0
;


