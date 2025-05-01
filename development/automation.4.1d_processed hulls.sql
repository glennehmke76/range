-- add to final hull table
create sequence processed_hulls_seq
  as integer;
DROP TABLE IF EXISTS processed_hulls;
CREATE TABLE processed_hulls (
  id integer primary key not null default nextval('processed_hulls_seq'::regclass),
  hull_type text,
  regionalisation text,
  sp_id integer,
  taxon_id_r varchar,
  class integer,
  br_class integer,
  geom geometry(MultiPolygon,4283)
);
alter sequence processed_hulls_seq owned by processed_hulls.id;
create index idx_processed_hulls_geom on processed_hulls using gist (geom);

alter table public.processed_hulls
    add taxon_id_r_v2 varchar;
alter table public.processed_hulls
    add permutation integer default 1;
comment on column public.processed_hulls.permutation is 'default is 1 which is strict, 2 is irregular regions subtracted from overall as alpha hulls';

-- insert final geoms - specify regionalisation and other predicates
INSERT INTO processed_hulls (hull_type, regionalisation, sp_id, class, geom, permutation)
SELECT
  concat(hull_type, '_', alpha) AS hull_type,
  regionalisation,
  sp_id,
  class,
  geom,
  permutation
FROM base_hulls
WHERE
  sp_id = 634
  AND regionalisation = 'sibra'
  AND class > 0
;

-----------------------
-- clean-up split geometries
DROP TABLE IF EXISTS processed_hulls_tmp;
CREATE TABLE processed_hulls_tmp (LIKE processed_hulls INCLUDING ALL);

INSERT INTO processed_hulls_tmp (hull_type, regionalisation, sp_id, taxon_id_r, class, geom, taxon_id_r_v2, permutation)
SELECT
  hull_type,
  regionalisation,
  sp_id,
  taxon_id_r,
  class,
  ST_Multi(ST_Union(geom)) AS geom,
  taxon_id_r_v2,
  permutation
FROM processed_hulls
WHERE
  sp_id = 634
--   OR sp_id = 8300
GROUP BY
  hull_type,
  regionalisation,
  sp_id,
  taxon_id_r,
  class,
  taxon_id_r_v2,
  permutation
;

-- then delete the old (non-dissolved) polygons and insert the final unioned set from the temp table
DELETE FROM processed_hulls
USING processed_hulls_tmp
WHERE
  processed_hulls.sp_id = processed_hulls_tmp.sp_id
;

INSERT INTO processed_hulls (hull_type, regionalisation, sp_id, taxon_id_r, class, geom, taxon_id_r_v2, permutation)
SELECT
  processed_hulls_tmp.hull_type,
  processed_hulls_tmp.regionalisation,
  processed_hulls_tmp.sp_id,
  processed_hulls_tmp.taxon_id_r,
  processed_hulls_tmp.class,
  processed_hulls_tmp.geom,
  processed_hulls_tmp.taxon_id_r_v2,
  processed_hulls_tmp.permutation
FROM processed_hulls_tmp
LEFT JOIN processed_hulls ON processed_hulls_tmp.sp_id = processed_hulls.sp_id
;
DROP TABLE IF EXISTS processed_hulls_tmp;
-----------------------



DROP VIEW IF EXISTS processed_hulls_sp;
CREATE VIEW processed_hulls_sp AS
SELECT
  row_number() over () AS id,
  hull_type,
  regionalisation,
  sp_id,
  ST_Multi(ST_Union(geom)) AS geom
FROM processed_hulls
GROUP BY
  hull_type,
  regionalisation,
  sp_id
;

-- import and replace in mater range
DELETE FROM range
WHERE
  sp_id = 641
  OR sp_id = 8300
;

INSERT INTO range (geom, taxon_id_r, sp_id, class, breeding_class)
SELECT
  geom,
  taxon_id_r,
  sp_id,
  class,
  br_class
FROM processed_hulls
WHERE
  sp_id = 641
  OR sp_id = 8300
;

