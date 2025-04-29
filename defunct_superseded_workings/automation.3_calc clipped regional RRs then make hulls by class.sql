
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
-- JOIN tmp_simple_region ON ST_Intersects(survey_point.geom,tmp_simple_region.geom) -- 221,271 rows affected in 42 s 924 ms
JOIN tmp_simple_region ON ST_DWithin(survey_point.geom, tmp_simple_region.geom, 0.05) -- 244,645 rows affected in 15 s 630 ms
WHERE sighting.sp_id = 32
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
  'basins_clipped' AS regionalisation,
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
  AND regionalisation = 'basins_clipped'
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
INSERT INTO range_region_rr (geom, regionalisation, region_id, sp_id, num_surveys, num_sightings, rr)
SELECT
  ST_Multi(region_surveys_sightings.geom),
  'basins_clipped',
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
          sighting.sp_id = 32
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
  regionalisation = 'basins_clipped'
  AND sp_id = 32
;

-- core hulls (sMCP) by class
  -- specify core RR threshold
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, core_rr_precentile, geom)
WITH hull_sightings AS
  (SELECT
    tmp_region.id AS region_id,
    sighting.sp_id,
    sighting.id AS sighting_id,
    survey.geom,
    Extract(YEAR FROM survey.start_date) AS year
  FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  JOIN tmp_region ON ST_Intersects(survey.geom,tmp_region.geom)
  JOIN range
    ON sighting.sp_id = range.sp_id
    AND ST_Intersects(survey.geom,range.geom)
  WHERE
    sighting.sp_id = 32
--     AND range.class = 1 -- in this case filtering to investigate how close this automation comes to what I originally did (albeit about 10 years ago on different data).
  )
  -- sMCP_core means limited to pre-existing core range
  SELECT
    'sMCP' AS hull_type,
    'basins_clipped' AS regionalisation,
    NULL AS alpha,
    hulls.sp_id,
    1 AS class,
    20 AS core_rr_precentile,
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.sp_id,
        range_region_rr.region_id,
        NULL,
        ST_ConvexHull(
          ST_Collect(hull_sightings.geom)) AS hull
      FROM hull_sightings
      JOIN range_region_rr ON ST_Intersects(hull_sightings.geom, range_region_rr.geom)
      WHERE
        range_region_rr.rr_percentile >= 20
      GROUP BY
        hull_sightings.sp_id,
        range_region_rr.region_id
        -- exclude point geoms from coming through
      HAVING
        Count(DISTINCT hull_sightings.geom) >2
      )hulls
  GROUP BY
    hulls.sp_id
;








-- above with subtract core from alpha shape
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH core_hull AS
  (SELECT
     sp_id,
     xxxx,
     base_hulls.geom
  FROM base_hulls
  WHERE
    base_hulls.sp_id = 32
    AND base_hulls.class = 0
  ),
overall_hull AS
  (SELECT
    sp_id,
    xxxx,
    base_hulls.geom
  FROM base_hulls
  WHERE
    base_hulls.sp_id = 32
    AND base_hulls.class = 1
  )
SELECT
  sp_id,
  xxxx,
  ST_Difference(overall_hull.geom, core_hull.geom) AS geom
FROM core_hull
LEFT JOIN overall_hull ON ST_Intersects(core_hull.geom, overall_hull.geom)
WHERE
  xxxx.id IS NULL
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

