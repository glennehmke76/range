-- regionally clipped ranges

-- make alphas around non-core region vertices as a tighter core range constraint

-- step 1 - make expanded non-core
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom, permutation)
WITH
region_hull AS
  (SELECT
    ST_Union(sub.geom) AS geom
  FROM
      (SELECT
        id,
        ST_Multi(
          ST_AlphaShape(
        ST_Collect(geom),
          0.1, false)) AS geom -- using very tight alpha parameter to basically smooth off the regions
      FROM range_region
      WHERE
        sp_id = 634
        AND "regionalisation" = 'sibra_clipped'
        AND (class = 3
        OR class = 9)
      GROUP BY
        id
      )sub
  ),
irregular_hull AS -- ie the pre-existing irregular/vagrant range
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
        AND (class = 3
        OR class = 9)
    AND hull_type = 'alpha'
  )
SELECT
  irregular_hull.hull_type,
  irregular_hull.regionalisation,
  irregular_hull.alpha,
  irregular_hull.sp_id,
  3 AS class, -- ie expanded_irregular
  ST_Union(irregular_hull.geom, region_hull.geom) AS geom,
  2 AS permutation
FROM region_hull, irregular_hull
;

-- subtract expanded non-core from initial core to produce clipped core
INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, geom, permutation)
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
expanded_irregular_hull AS
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
    AND class = 3 -- as overall hull
    AND permutation = 2
    AND hull_type = 'alpha'
  )
SELECT
  core_hull.hull_type,
  core_hull.regionalisation,
  core_hull.alpha,
  core_hull.sp_id,
  1 AS class,
  ST_Difference(core_hull.geom,expanded_irregular_hull.geom) AS geom,
  2 AS permutation
FROM core_hull
JOIN expanded_irregular_hull ON ST_Intersects(core_hull.geom, expanded_irregular_hull.geom)
;

-- trial
SELECT
  class * permutation AS new,
  ST_Union(geom) AS geom
FROM base_hulls
WHERE
  sp_id = 634
  AND class > 0
GROUP BY
  class * permutation
;

