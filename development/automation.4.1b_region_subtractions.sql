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




-- add areas back in

  -- via regions or points?

-- make core hull polys to add back in as another permutation

-- select points to re-include
alter table public.tmp_region_sightings
    add class_specified integer;

INSERT INTO base_hulls (hull_type, regionalisation, alpha, sp_id, class, permutation, geom)
WITH hull_sightings AS
  (SELECT
    tmp_region_sightings.sp_id,
    tmp_region_sightings.geom
  FROM tmp_region_sightings
  WHERE
    tmp_region_sightings.class_specified = 1
  )
  SELECT
    'alpha' AS hull_type,
    'sibra' AS regionalisation,
    0.1 AS alpha,
    hulls.sp_id,
    1 AS class,
    2 AS permutation,
    ST_Multi(
      ST_Union(
        ST_SetSRID(hulls.hull,4283))) AS geom
  FROM
      (SELECT
        hull_sightings.sp_id,
        ST_Multi(
          ST_AlphaShape(
        ST_Collect(hull_sightings.geom),
          0.1, false)) AS hull
      FROM hull_sightings
      GROUP BY
        hull_sightings.sp_id
      )hulls
  GROUP BY
    hulls.sp_id
;

-- add core range re-inclusions into permutation 2 core range
  -- delete perm 2 core range
DROP TABLE IF EXISTS base_hulls_tmp;
CREATE TABLE base_hulls_tmp (LIKE base_hulls INCLUDING ALL);

INSERT INTO base_hulls_tmp (hull_type, regionalisation, sp_id, class, geom, permutation)
SELECT
  hull_type,
  regionalisation,
  sp_id,
  class,
  ST_Multi(ST_Union(geom)) AS geom,
  2 AS permutation
FROM base_hulls
WHERE
  sp_id = 634
  AND class = 1
  AND permutation = 2
GROUP BY
  hull_type,
  regionalisation,
  sp_id,
  class
;

-- then delete the old (non-dissolved) polygons and insert the final unioned set from the temp table
DELETE FROM base_hulls
USING base_hulls_tmp
WHERE
  base_hulls.sp_id = base_hulls_tmp.sp_id
  AND base_hulls.permutation = 2
;

INSERT INTO base_hulls (hull_type, regionalisation, sp_id, class, geom, permutation)
SELECT
  base_hulls_tmp.hull_type,
  base_hulls_tmp.regionalisation,
  base_hulls_tmp.sp_id,
  base_hulls_tmp.class,
  base_hulls_tmp.geom,
  base_hulls_tmp.permutation
FROM base_hulls_tmp
;
DROP TABLE IF EXISTS base_hulls_tmp;

-- delete old perm 2 infrequent
DELETE FROM base_hulls
WHERE
  permutation = 2
  AND class = 3
;

-- clip core from overall to create new non-core
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
    AND permutation = 2
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
  overall_hull.regionalisation,
  overall_hull.alpha,
  overall_hull.sp_id,
  3 AS class,
  ST_Difference(overall_hull.geom,core_hull.geom) AS geom,
  2 AS permutation
FROM overall_hull
JOIN core_hull ON ST_Intersects(overall_hull.geom, core_hull.geom)
;




processing.run("gdal:rasterize", {'INPUT':'memory://MultiPolygon?crs=EPSG:3112&field=OBJECTID:long(10,0)&field=PA_ID:string(20,0)&field=PA_PID:string(20,0)&field=NAME:string(254,0)&field=TYPE:string(60,0)&field=TYPE_ABBR:string(10,0)&field=IUCN:string(5,0)&field=NRS_PA:string(5,0)&field=GAZ_AREA:double(30,15)&field=GIS_AREA:double(30,15)&field=GAZ_DATE:string(20,0)&field=LATEST_GAZ:string(20,0)&field=STATE:string(4,0)&field=AUTHORITY:string(15,0)&field=DATASOURCE:string(20,0)&field=GOVERNANCE:string(3,0)&field=COMMENTS:string(120,0)&field=ENVIRON:string(3,0)&field=OVERLAP:string(3,0)&field=MGT_PLAN:string(3,0)&field=RES_NUMBER:string(15,0)&field=EPBC:string(15,0)&field=LONGITUDE:double(30,15)&field=LATITUDE:double(30,15)&field=SHAPE_Leng:double(30,15)&field=SHAPE_Area:double(30,15)&uid={3f2cb6b2-534a-4585-944f-1ce4ab601800}','FIELD':'','BURN':1,'USE_Z':False,'UNITS':1,'WIDTH':500,'HEIGHT':500,'EXTENT':None,'NODATA':0,'OPTIONS':'','DATA_TYPE':0,'INIT':None,'INVERT':False,'EXTRA':'','OUTPUT':'TEMPORARY_OUTPUT'})

processing.run("native:pixelstopoints", {'INPUT_RASTER':'/private/var/folders/df/w_2zy89s46g71543mf819y1w0000gn/T/processing_PBcdPJ/616e571bb342481ea4c67624fb09c8cc/OUTPUT.tif','RASTER_BAND':1,'FIELD_NAME':'VALUE','OUTPUT':'TEMPORARY_OUTPUT'})

processing.run("gdal:rasterize", {'INPUT':'/Users/glennehmke/Downloads/mallee parks_line.gpkg','FIELD':'','BURN':1,'USE_Z':False,'UNITS':1,'WIDTH':500,'HEIGHT':500,'EXTENT':None,'NODATA':0,'OPTIONS':'','DATA_TYPE':0,'INIT':None,'INVERT':False,'EXTRA':'','OUTPUT':'/Users/glennehmke/Downloads/mallee parks_line.tif'})

-- import then run


SELECT
  ST_Multi(
    ST_AlphaShape(
  ST_Collect(geom),
    2, false)) AS geom -- using very tight alpha parameter to basically smooth off the regions
FROM parks_clipper


