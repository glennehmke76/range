

-- switches
        -- not needed
--         DROP TABLE IF EXISTS tmp_range;
--         CREATE TABLE tmp_range AS
--         SELECT
--           range.sp_id,
--           range.taxon_id_r,
--           range.rnge,
--           range.br_rnge,
--           range.geom
--         FROM range
--         JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
--         JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
--         WHERE
--           wlab.coastal IS NULL
--           AND wlab.population <> 'Vagrant'
--           AND wlab.bird_group <> 'Marine'
--           AND wlab.bird_group = 'Wetland'
--         ;
--         CREATE INDEX IF NOT EXISTS idx_tmp_range_taxon_id
--         ON tmp_range (taxon_id_r);
--         CREATE INDEX IF NOT EXISTS idx_tmp_range_sp_id
--         ON tmp_range (sp_id);
--         CREATE INDEX IF NOT EXISTS idx_tmp_range_range_id
--         ON tmp_range (rnge);
--         CREATE INDEX IF NOT EXISTS idx_tmp_range_breeding_range_id
--         ON tmp_range (br_rnge);
--         CREATE INDEX IF NOT EXISTS idx_tmp_tmp_range_geom
--           ON tmp_range USING gist
--           (geom)
--           TABLESPACE pg_default;

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

-- down the track have a table of species and prefered regioanlsations for repeating with new data... populate as re-do layers...
-- ... also need different reporting rate thresholds by species...
-- this may allow a trigger to repeat some proceedure to be triggered upon submission of a record in a new spatial location (say and out of range record that is accepted)

-- add with switch for regionalisation
-- class sightings by RR percentile

-- MAKE THIS REGION BY RANGE???

DROP TABLE IF EXISTS tmp_region_rr;
CREATE TABLE tmp_region_rr AS
SELECT
  region_surveys_sightings.geom,
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
CREATE INDEX IF NOT EXISTS idx_tmp_region_rr_geom
  ON tmp_region_rr USING gist (geom) TABLESPACE pg_default;

alter table tmp_region_rr
  add percentile integer;
UPDATE tmp_region_rr
SET percentile = CASE
  WHEN rr <
        (SELECT
          percentile_disc(0.05) WITHIN GROUP (ORDER BY rr)
        FROM tmp_region_rr)
  THEN 5
  WHEN rr <
          (SELECT
            percentile_disc(0.1) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 10
  WHEN rr <
          (SELECT
            percentile_disc(0.2) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 20
  WHEN rr <
          (SELECT
            percentile_disc(0.3) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 30
  WHEN rr <
          (SELECT
            percentile_disc(0.4) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 40
  WHEN rr <
          (SELECT
            percentile_disc(0.5) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 50
  WHEN rr <
          (SELECT
            percentile_disc(0.6) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 60
  WHEN rr <
          (SELECT
            percentile_disc(0.7) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 70
  WHEN rr <
          (SELECT
            percentile_disc(0.8) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 80
  WHEN rr <
          (SELECT
            percentile_disc(0.9) WITHIN GROUP (ORDER BY rr)
          FROM tmp_region_rr)
  THEN 90
  ELSE 100
END
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
  )
  SELECT
    'sMCP' AS hull_type,
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
        tmp_region_rr.region_id,
        NULL,
        ST_ConvexHull(
          ST_Collect(hull_sightings.geom)) AS hull
      FROM hull_sightings
      JOIN tmp_region_rr ON ST_Intersects(hull_sightings.geom, tmp_region_rr.geom)
      WHERE
        tmp_region_rr.percentile >= 20
      GROUP BY
        hull_sightings.species_id,
        tmp_region_rr.region_id
        -- exclude point geoms from coming through
      HAVING
        Count(DISTINCT hull_sightings.geom) >2
      )hulls
  GROUP BY
    hulls.species_id
;

-- vagrant hulls
  -- make overall hull first then subtract core
-- overall alpha shape
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    sighting.species_id,
    survey.geom
    FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  WHERE
    sighting.species_id = 574
  )
SELECT
  'alpha' AS hull_type,
  'basins' AS regionalisation,
  2.5 AS alpha,
  hull_sightings.species_id,
  0 AS range_class,
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(hull_sightings.geom),
    2.5, false)) AS hull
FROM hull_sightings
JOIN tmp_region_rr ON ST_Intersects(hull_sightings.geom, tmp_region_rr.geom)
WHERE
--   tmp_region_rr.rr > 0.01
  tmp_region_rr.num_sightings > 4
GROUP BY
  species_id
;

-- above with subtract core from alpha shape
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom)
WITH hull_sightings AS
  (SELECT
    sighting.species_id,
    survey.geom
    FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  WHERE
    sighting.species_id = 574
  ),
existing_hull AS
  (SELECT
    base_hulls.geom
  FROM base_hulls
  WHERE
    base_hulls.sp_id = 574
    AND base_hulls.hull_type = 'sMCP'
    AND base_hulls.class = 1
    AND base_hulls.core_rr_precentile = 20
  )
SELECT
  ST_SymDifference(existing_hull.geom, alpha.hull) AS geom
FROM
  (SELECT
    'alpha' AS hull_type,
    'basins' AS regionalisation,
    2.5 AS alpha,
    hull_sightings.species_id,
    0 AS range_class,
    ST_Multi(
      ST_AlphaShape(
     ST_Collect(hull_sightings.geom),
      2.5, false)) AS hull
  FROM hull_sightings
  JOIN tmp_region_rr ON ST_Intersects(hull_sightings.geom, tmp_region_rr.geom)
  WHERE
    tmp_region_rr.num_sightings > 4
  GROUP BY
    species_id
  )alpha
JOIN existing_hull ON NOT ST_Intersects(alpha.hull, existing_hull.geom)
;

WITH temp AS
(
  SELECT   b.id, st_union(a.geom) AS geom
  FROM     buffers b JOIN blanks a ON st_intersects(a.geom, b.geom)
  GROUP BY b.id
)
SELECT b.buffer_id, st_difference(b.geom,t.geom) AS newgeom
FROM buffers b LEFT JOIN temp t ON b.id = t.id;


SELECT a.field_i_need, ST_Multi(COALESCE(ST_Difference(a.geom, output.geom),a.geom)) AS geom
FROM buffers AS a
CROSS JOIN LATERAL (
  SELECT ST_Union(b.geom) AS geom
  FROM blanks AS b
  WHERE ST_Intersects(a.geom, b.geom)
) AS output ;


-- add ST_Dump to optimise
CREATE TABLE difference AS
SELECT a.id, a. type, ST_Multi(COALESCE(ST_Difference(a.geom, output.geom),a.geom)) AS geom
FROM t2 AS a
CROSS JOIN LATERAL (
  SELECT ST_Union(b.geom) AS geom
  FROM t1 AS b
  WHERE ST_Intersects(a.geom, b.geom)
) AS output ;

