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


MUST DUAL JOIN NOW!!!
SELECT
  id,
  count(*) AS num_id
FROM survey
GROUP BY
  id
HAVING   count(*) > 1



-- 634 run 1,243,600 rows affected in 8 m 1 s 204 ms
DROP TABLE IF EXISTS tmp_region_sightings;
CREATE TABLE tmp_region_sightings AS
SELECT DISTINCT
  row_number() over () as id,
  tmp_simple_region.id region_id,
  sighting.class_specified,
  survey.id AS survey_id,
  survey.data_source,
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
	sighting.vetting_status_id,
-- 	survey.geom
	survey_point.geom
FROM survey
JOIN sighting
  ON survey.id = sighting.survey_id
  AND survey.data_source = sighting.data_source
JOIN source ON survey.source_id = source.id
JOIN survey_type ON survey.survey_type_id = survey_type.id
JOIN tmp_simple_region ON ST_DWithin(survey.geom, tmp_simple_region.geom, 0.05)
WHERE
  sighting.sp_id = 634
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

-- vet out erroneous sightings (this is a loop - re-run above after update below)
          -- define data points to exclude in GIS (or code)
          drop table if exists exclusions;
          create table exclusions
          (
              sighting_id integer
          );
          copy exclusions FROM '/Users/glennehmke/MEGA/RangeLayers/range_paper_1/exemplars/634_exclusions.csv'
          DELIMITER ',' CSV header;

          -- import
          -- define in sightings
          UPDATE tmp_region_sightings
          SET class_specified = 99
          FROM exclusions
          WHERE
            tmp_region_sightings.sighting_id = exclusions.sighting_id
          ;

UPDATE tmp_simple_region
SET num_sightings = count.num_sightings
FROM
  (SELECT
    tmp_region_sightings.region_id,
    Count(tmp_region_sightings.sighting_id) AS num_sightings
  FROM tmp_region_sightings
  -- potentially limit to contemporary years etc
  WHERE
    tmp_region_sightings.class_specified IS NULL
  GROUP BY
    tmp_region_sightings.region_id
  )count
WHERE
  count.region_id = tmp_simple_region.id
;


INSERT INTO base_hulls (hull_type, alpha, sp_id, class, geom)
SELECT
  'alpha' AS hull_type,
  2.5 AS alpha,
  634 AS sp_id,
  0 AS range_class,
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(tmp_region_sightings.geom),
    2.5, false)) AS hull
FROM tmp_region_sightings
WHERE class_specified IS NULL
;

DROP TABLE IF EXISTS tmp_region;
CREATE TABLE tmp_region AS
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
ON tmp_region (id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_geom
  ON tmp_region USING gist (geom) TABLESPACE pg_default;

    -- down the track have a table of species and preferred regionalisation for repeating with new data... populate as re-do layers...
    -- ... also need different reporting rate thresholds by species...
    -- this may allow a trigger to repeat some proceedure to be triggered upon submission of a record in a new spatial location (say and out of range record that is accepted)

    -- add with switch for regionalisation
    -- class sightings by RR percentile

-- use alpha shape of all data initially
INSERT INTO range_region (geom, regionalisation, region_id, sp_id, num_surveys, num_sightings, rr)
SELECT
  ST_Multi(region_surveys_sightings.geom),
  'sibra_clipped',
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
          sighting.sp_id AS sp_id,
          Count(sighting.id) AS num_sightings
        FROM survey
        JOIN sighting ON survey.id = sighting.survey_id
        JOIN survey_point ON survey.survey_point_id = survey_point.id
        JOIN tmp_region ON ST_Intersects(survey_point.geom,tmp_region.geom)
        WHERE
          sighting.sp_id = 634
       -- AND Extract(YEAR FROM survey.start_date) >= 1999 -- to limit to contemporary data
        GROUP BY
          tmp_region.id,
          sighting.sp_id
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

-- remember to set predicate so existing rrs are not updated... or do this in a temp table then add it to the master...
UPDATE range_region
SET rr_percentile = CASE
  WHEN rr <
        (SELECT
          percentile_disc(0.05) WITHIN GROUP (ORDER BY rr)
        FROM range_region)
  THEN 5
  WHEN rr <
          (SELECT
            percentile_disc(0.1) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 10
  WHEN rr <
          (SELECT
            percentile_disc(0.2) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 20
  WHEN rr <
          (SELECT
            percentile_disc(0.3) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 30
  WHEN rr <
          (SELECT
            percentile_disc(0.4) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 40
  WHEN rr <
          (SELECT
            percentile_disc(0.5) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 50
  WHEN rr <
          (SELECT
            percentile_disc(0.6) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 60
  WHEN rr <
          (SELECT
            percentile_disc(0.7) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 70
  WHEN rr <
          (SELECT
            percentile_disc(0.8) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 80
  WHEN rr <
          (SELECT
            percentile_disc(0.9) WITHIN GROUP (ORDER BY rr)
          FROM range_region)
  THEN 90
  ELSE 100
END
WHERE
  regionalisation = 'sibra_clipped'
  AND sp_id = 634
;

-- code non-core regions in range_region_rr
  -- example bulk update based on a parameter
  UPDATE range_region
  SET class = 3
  WHERE
    sp_id = 634
    AND regionalisation = 'sibra_clipped'
    AND rr_percentile <=20
  ;

-- core as alpha shape
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    range_region.id                                    AS region_id,
    tmp_region_sightings.sp_id,
    tmp_region_sightings.geom,
    Extract(YEAR FROM tmp_region_sightings.start_date) AS year
  FROM tmp_region_sightings
  JOIN range_region
    ON tmp_region_sightings.sp_id = range_region.sp_id
    AND ST_Intersects(tmp_region_sightings.geom, range_region.geom)
  WHERE
    tmp_region_sightings.sp_id = 634
    AND range_region.sp_id = 634
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
    sp_id = 634
    AND class = 1 -- as core hull
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
    sp_id = 634
    AND class = 0 -- as overall hull
    AND hull_type = 'alpha'
  )
SELECT
  overall_hull.hull_type,
  core_hull.regionalisation,
  overall_hull.alpha,
  overall_hull.sp_id,
  3 AS class, -- ie infrequent
  ST_Difference(overall_hull.geom, core_hull.geom) AS geom
FROM core_hull
JOIN overall_hull ON ST_Intersects(core_hull.geom, overall_hull.geom)
;

-- if vagrant range required
  -- delete previous overall hull
  DELETE FROM base_hulls
  WHERE
    sp_id = 634
    AND class = 0
  ;

  -- code legitimate vagrant sightings - re-code 99s to 9
  UPDATE tmp_region_sightings
  SET class_specified = 9
  WHERE
    sp_id = 634
    AND vetting_status_id = 2 -- ie accepted
  ;

  -- manually code other sightings as appropriate

-- make new overall hull including vagrancy
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    tmp_region_sightings.sp_id,
    tmp_region_sightings.geom
  FROM tmp_region_sightings
  WHERE
    tmp_region_sightings.class_specified < 99
    OR tmp_region_sightings.class_specified IS NULL
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

-- subtract core from alpha shape and add to base_hulls as non-core range class
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH non_vagrant_hull AS
  (SELECT
    ST_Union(geom) AS geom
  FROM base_hulls
  WHERE
    sp_id = 634
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
    sp_id = 634
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



-- other options for subtraction if above is not efficient enough
WITH temp AS
  (SELECT
    b.id,
    st_union(a.geom) AS geom
    FROM buffers b JOIN blanks a ON st_intersects(a.geom, b.geom)
    GROUP BY
      b.id
  )
SELECT
  b.buffer_id,
  st_difference(b.geom,t.geom) AS newgeom
FROM buffers b
LEFT JOIN temp t ON b.id = t.id;


SELECT
  a.field_i_need,
  ST_Multi(COALESCE(ST_Difference(a.geom, output.geom),a.geom)) AS geom
FROM buffers AS a
CROSS JOIN LATERAL
  (SELECT ST_Union(b.geom) AS geom
  FROM blanks AS b
  WHERE ST_Intersects(a.geom, b.geom)
  )output;

-- add ST_Dump to optimise
CREATE TABLE difference AS
SELECT
  a.id,
  a.type,
  ST_Multi(COALESCE(ST_Difference(a.geom, output.geom),a.geom)) AS geom
FROM t2 AS a
CROSS JOIN LATERAL
  (SELECT ST_Union(b.geom) AS geom
  FROM t1 AS b
  WHERE ST_Intersects(a.geom, b.geom)
  ) AS output;

