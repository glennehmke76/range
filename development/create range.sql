create table public.range (
  id integer primary key not null default nextval('range_seq'::regclass),
  geom geometry(MultiPolygon,4283),
  taxon_id_r character varying,
  sp_id integer,
  class integer,
  breeding_class integer,
  geom_valid boolean,
  geom_invalid_detail character varying,
  num_vertices integer
);
create index idx_range_geom on range using gist (geom);
create index range_sp_id_taxon_id_r_class_breeding_class_index on range using btree (sp_id, taxon_id_r, class, breeding_class);
create index range_breeding_class_index on range using btree (breeding_class);
create index range_class_index on range using btree (class);
create index range_sp_id_index on range using btree (sp_id);
create index range_taxon_id_r_index on range using btree (taxon_id_r);

-- create sequence autonumber on id if required
create sequence range_seq as integer;
alter sequence range_seq owned by range.id;
alter table range
  alter column id set default nextval('range_seq');

DROP VIEW IF EXISTS wlist_sp CASCADE;
CREATE VIEW wlab_sp AS
  SELECT *
  FROM wlab
  WHERE taxon_level = 'sp';


-- change class to class post backup 16 July 2024
alter table range
    rename column class to class;
alter table range
    rename column breeding_class to breeding_class;
alter index range_sp_id_taxon_id_r_class_breeding_class_index rename to range_sp_id_taxon_id_r_class_breeding_class_index;
alter index range_breeding_class_index rename to range_breeding_class_index;
alter index range_class_index rename to range_class_index;


