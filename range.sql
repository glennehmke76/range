-- ##### because some taxa have inland core ranges I'm only digitising core ranges for coastal taxa initially. This means some taxa loose 'vagrant' ranges - but these are so irrelevent for these birds they are not worth worrying about from a conservation perspective. They are:
u152,Eastern Black-tailed Godwit
u161,Curlew Sandpiper
u129,Palaearctic Ruddy Turnstone
129,Ruddy Turnstone
u165,Great Knot
166,Sanderling

153,Bar-tailed Godwit
u153a,Alaskan Bar-tailed Godwit
u153b,Yakutian Bar-tailed Godwit
+ u153c


Hybrid coastal inland are problems...
presently

u157,Common Sandpiper
u143,Red-capped Plover
u159,Marsh Sandpiper
u162,Red-necked Stint


-- make line table and port linear geom to that from (polygonal) range table
  -- this is a once off and usual editing will be of the line geoms which can then be scripted back into the 'main' range table
SELECT
  *
FROM wlab
WHERE coastal = 1

DROP TABLE IF EXISTS range_line
create table public.range_line (
  id integer primary key not null default nextval('range_seq'::regclass),
  geom geometry(MultiLineString,4283),
  taxon_id_r character varying,
  sp_id integer,
  rnge integer,
  br_rnge integer
);
create index idx_range_line_geom on range_line using gist (geom);
create index range_line_sp_id_taxon_id_r_rnge_br_rnge_index on range_line using btree (sp_id, taxon_id_r, rnge, br_rnge);
create index range_line_br_rnge_index on range_line using btree (br_rnge);
create index range_line_rnge_index on range_line using btree (rnge);
create index range_line_sp_id_index on range_line using btree (sp_id);
create index range_line_taxon_id_r_index on range_line using btree (taxon_id_r);

INSERT INTO range_line (geom, sp_id, taxon_id_r, rnge, br_rnge)
WITH unioned_coast AS
  (SELECT
     ST_Union(geom) AS geom
   FROM region_coastline_simple
  )
SELECT
  ST_Multi
    (ST_Union
      (ST_Intersection(range_backup.geom, unioned_coast.geom))) AS geom,
  range_backup.sp_id,
  range_backup.taxon_id_r,
  range_backup.rnge,
  range_backup.br_rnge
FROM unioned_coast
JOIN range_backup ON ST_Intersects(range_backup.geom, unioned_coast.geom)
JOIN wlab_range ON range_backup.taxon_id_r = wlab_range.taxon_id_r
JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
WHERE
  wlab.coastal = 1
GROUP BY
  range_backup.sp_id,
  range_backup.taxon_id_r,
  range_backup.rnge,
  range_backup.br_rnge
;

-- create buffer oly from line
  -- check (if needed)
--   DROP TABLE IF EXISTS tmp;
--   -- 58 rows affected in 4 m 0 s 278 ms
--   CREATE TABLE tmp AS
--   SELECT
--     row_number() over () AS id,
--     taxon_id_r,
--     rnge,
--     br_rnge,
--     ST_Transform
--       (ST_Union
--         (ST_Buffer
--             (ST_Transform(geom, 3112), 10000, 'quad_segs=6, endcap=flat')), 4283) AS geom
--   FROM range_line
--   GROUP BY
--     taxon_id_r,
--     rnge,
--     br_rnge
--   ;
--   create index idx_tmp_geom on tmp using gist (geom);
--   DROP TABLE IF EXISTS tmp;

  -- 65 rows affected in 3 m 22 s 59 ms
  UPDATE range
  SET geom = sub.geom
  FROM
    -- 58 rows retrieved starting from 1 in 4 m 12 s 213 ms (execution: 4 m 12 s 131 ms, fetching: 82 ms)
    (SELECT
      taxon_id_r,
      rnge,
      br_rnge,
      ST_Multi
        (ST_Transform
          (ST_Union
            (ST_Buffer
              (ST_Transform(geom, 3112), 10000, 'quad_segs=6, endcap=flat')), 4283)) AS geom
    FROM range_line
    GROUP BY
      taxon_id_r,
      rnge,
      br_rnge
    )sub
  WHERE
    sub.taxon_id_r = range.taxon_id_r
    AND sub.rnge = range.rnge
    AND sub.br_rnge = range.br_rnge
  ;

DROP TABLE IF EXISTS range_point;
create table public.range_point (
  id integer primary key not null default nextval('range_seq'::regclass),
  geom geometry(Point,4283),
  taxon_id_r character varying,
  sp_id integer,
  notes text
);
create index idx_range_point_geom on range_point using gist (geom);
create index range_point_sp_id_index on range_point using btree (sp_id);
create index range_point_taxon_id_r_index on range_point using btree (taxon_id_r);
comment on table range_point is 'to denote breeding islands for seabirds initially';
