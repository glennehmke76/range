-- regionally clipped ranges

-- make alphas around non-core region vertices as a tighter core range constraint

alter table public.tmp_region_sightings
    add disjunct_pop_id integer;

WITH hull_sightings AS
  (SELECT
    range_region.id AS region_id,
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
    AND
      (range_region.class = 3
      OR range_region.class = 9)
  )
SELECT
  sub.hull_type,
  sub.regionalisation,
  sub.alpha,
  sub.sp_id,
  sub.class,
  ST_Union(sub.geom) AS geom
FROM
    (SELECT
      'alpha' AS hull_type,
      'sibra' AS regionalisation,
      0.1 AS alpha,
      hulls.sp_id,
      hulls.region_id,
      3 AS class, -- ie core
      ST_Multi(
        ST_Union(
          ST_SetSRID(hulls.hull,4283))) AS geom
    FROM
        (SELECT
          hull_sightings.sp_id,
          hull_sightings.region_id,
          ST_Multi(
            ST_AlphaShape(
          ST_Collect(hull_sightings.geom),
            2.5, false)) AS hull
        FROM hull_sightings
        GROUP BY
          hull_sightings.sp_id,
          hull_sightings.region_id
        )hulls
    GROUP BY
      hulls.sp_id,
      hulls.region_id
    )sub
GROUP BY
  sub.hull_type,
  sub.regionalisation,
  sub.alpha,
  sub.sp_id,
  sub.class
;


