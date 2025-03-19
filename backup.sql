-- backup
create table range_backup (
  id integer,
  geom geometry(MultiPolygon,4283),
  taxon_id_r character varying,
  sp_id integer,
  class integer,
  breeding_class integer,
  geom_valid boolean,
  geom_invalid_detail character varying,
  num_vertices integer
);
create index idx_range_backup_geom on range_backup using gist (geom);
create index range_backup_breeding_class_index on range_backup using btree (breeding_class);
create index range_backup_class_index on range_backup using btree (class);
create index range_backup_sp_id_index on range_backup using btree (sp_id);
create index range_backup_sp_id_taxon_id_r_class_breeding_class_index on range_backup using btree (sp_id, taxon_id_r, class, breeding_class);
create index range_backup_taxon_id_r_index on range_backup using btree (taxon_id_r);
insert into range_backup (id, geom, taxon_id_r, sp_id, class, breeding_class, geom_valid, geom_invalid_detail, num_vertices)
SELECT *
FROM range;

-- repopulate from backup
INSERT INTO public.range (geom, taxon_id_r, sp_id, class, breeding_class)
SELECT
  range_backup.geom,
  range_backup.taxon_id_r,
  range_backup.sp_id,
  range_backup.class,
  range_backup.breeding_class
FROM range_backup
LEFT JOIN range ON range_backup.taxon_id_r = range.taxon_id_r
WHERE
  range.taxon_id_r IS NULL
;