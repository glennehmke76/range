-- do this after all other primary range classes are defined

-- if vagrant range required
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
    tmp_region_sightings.class_specified = 9
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
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom, permutation)
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
  ST_Difference(overall_hull.geom, non_vagrant_hull.geom) AS geom,
  0 AS permutation
FROM non_vagrant_hull
JOIN overall_hull ON ST_Intersects(non_vagrant_hull.geom, overall_hull.geom)
;

