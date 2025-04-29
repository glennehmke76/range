-- make overall hull 1st for regional clipper
-- overall hull as determined by alpha shape
  -- make overall hull first then subtract core
-- overall alpha shape



-- surveys with a coastal taxa was recorded

DROP TABLE IF EXISTS tmp_region_coastal;
-- CREATE TEMPORARY TABLE tmp_region_coastal AS
CREATE TABLE tmp_region_coastal AS
  SELECT
    clipped.region_id,
    clipped.region_name,
    clipped.geom AS geom,
    ST_Area(ST_Transform(clipped.geom, 3112)) / 10000 AS area
  FROM
      (SELECT
        region_sibra_simple.id AS region_id,
        region_sibra_simple.name AS region_name,
        ST_Intersection(ST_Transform(buffered.geom, 4283), region_sibra_simple.geom) AS geom
      FROM
          (SELECT
            ST_Union(buffered_sub.geom) AS geom
          FROM
              (SELECT
                ST_Buffer(
                  ST_Simplify(
                    ST_Transform(
                      region_coastline_simple.geom, 3112), 200), 3000, 'quad_segs=2') AS geom
              FROM region_coastline_simple
              )buffered_sub
          )buffered
      JOIN region_sibra_simple ON ST_Intersects(ST_Transform(buffered.geom, 4283), region_sibra_simple.geom)
      )clipped
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_coastal_region_id
ON tmp_region_coastal (region_id);
CREATE INDEX IF NOT EXISTS idx_tmp_region_coastal_geom
  ON tmp_region_coastal USING gist
  (geom)
  TABLESPACE pg_default;

-- 61,902 rows affected in 21 m 15 s 919 ms
DROP TABLE IF EXISTS tmp_coastal_sp_surveys;
-- CREATE TEMPORARY TABLE tmp_region_coastal AS
CREATE TABLE tmp_coastal_sp_surveys AS
SELECT
  survey.geom,
  count(survey.id) as num_surv
FROM survey
JOIN sighting ON survey.id = sighting.survey_id
JOIN wlist_sp ON sighting.sp_id = wlist_sp.sp_id
JOIN region_coastline_simple
  ON ST_DWithin(
                ST_Transform(survey.geom, 3112),
                ST_Transform(region_coastline_simple.geom, 3112),3000
                )
WHERE
  wlist_sp.coastal_range_ge = 1
--   wlist_sp.sp_id = 138
GROUP BY
  survey.geom
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_coastal_geom
  ON tmp_coastal_sp_surveys USING gist
  (geom)
  TABLESPACE pg_default;







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

DROP TABLE IF EXISTS tmp_region_sightings;
CREATE TABLE tmp_region_sightings AS
SELECT DISTINCT
  row_number() over () id,
  tmp_simple_region.id region_id,
  sighting.id sighting_id,
  survey_point.geom
FROM survey
JOIN sighting ON survey.id = sighting.survey_id
JOIN survey_point ON survey.survey_point_id = survey_point.id
JOIN tmp_simple_region ON ST_Intersects(survey_point.geom,tmp_simple_region.geom)
JOIN range
  ON sighting.species_id = range.sp_id
  AND ST_Intersects(survey.geom,range.geom)
-- JOIN tmp_simple_region ON ST_DWithin(survey_point.geom, tmp_simple_region.geom, 0.05)
WHERE
  sighting.species_id = 32
  AND range.sp_id = 32
--   AND range.class = 1
;
CREATE INDEX IF NOT EXISTS idx_tmp_region_sightings_id
  ON tmp_region_sightings (region_id);
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

INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
SELECT
  'alpha' AS hull_type,
  'basins' AS regionalisation,
  2.5 AS alpha,
  32,
  0 AS range_class,
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(tmp_region_sightings.geom),
    2.5, false)) AS hull
FROM tmp_region_sightings
JOIN tmp_simple_region ON tmp_region_sightings.region_id = tmp_simple_region.id
WHERE
  tmp_simple_region.num_sightings > 4
;

-- if [XX000] ERROR: lwgeom_intersection_prec: GEOS Error: TopologyException: side location conflict at 148.79368951130778 -18.695546476133298. This can occur if the input geometry is invalid.
-- or similar fix geom error manually I guess...
DROP TABLE IF EXISTS tmp_region;
CREATE TABLE tmp_region AS
SELECT
  tmp_simple_region.id,
  ST_Intersection(tmp_simple_region.geom, base_hulls.geom) AS geom
FROM tmp_simple_region
JOIN base_hulls ON ST_Intersects(tmp_simple_region.geom, base_hulls.geom)
WHERE
  base_hulls.class = 0
  AND base_hulls.sp_id = 32
  AND regionalisation = 'basins'
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


WITH coastal_surveys
  (SELECT
    survey.geom
  FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  JOIN wlist_sp ON sighting.sp_id = wlist_sp.sp_id
  WHERE
    wlist_sp.coastal_range_ge = 1
  )



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
          sighting.species_id = 32
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

-- remember to set predicate so existing rrs are not updated... or do this in a temp table then add it to the master...
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
  AND sp_id = 32
;

-- core as alpha shape
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    range_region_rr.id AS region_id,
    sighting.species_id,
    sighting.id AS sighting_id,
    survey_point.geom,
    Extract(YEAR FROM survey.start_date) AS year
  FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  JOIN survey_point ON survey.survey_point_id = survey_point.id
  JOIN range_region_rr ON ST_Intersects(survey_point.geom,range_region_rr.geom)
  WHERE
    sighting.species_id = 32
    AND range_region_rr.sp_id = 32
    AND range_region_rr.class IS NULL
  )
  SELECT
    'alpha' AS hull_type,
    'basins' AS regionalisation,
    2.5 AS alpha,
    hulls.species_id,
    1 AS class,
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.species_id,
        ST_Multi(
          ST_AlphaShape(
        ST_Collect(hull_sightings.geom),
          2.5, false)) AS hull
      FROM hull_sightings
      JOIN range_region_rr ON ST_Intersects(hull_sightings.geom, range_region_rr.geom)
--       WHERE
--         range_region_rr.rr_percentile >= 20
      WHERE
        range_region_rr.num_sightings > 4
      GROUP BY
        hull_sightings.species_id
      )hulls
  GROUP BY
    hulls.species_id
;

-- above with subtract core from alpha shape and add to base_hulls as non-core range class
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
-- still dodgy - better ways to do this - more dev reqd
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
    sp_id = 32
    AND class = 1
    AND hull_type = 'alpha'
    AND regionalisation = 'basins'
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
    sp_id = 32
    AND class = 0
    AND hull_type = 'alpha'
    AND regionalisation = 'basins'
  )
SELECT
  overall_hull.hull_type,
  overall_hull.regionalisation,
  overall_hull.alpha,
  overall_hull.sp_id,
  3 AS class,
  ST_Difference(overall_hull.geom, core_hull.geom) AS geom
FROM core_hull
JOIN overall_hull ON ST_Intersects(core_hull.geom, overall_hull.geom)
;






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

