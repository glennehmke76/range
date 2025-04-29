create sequence range_point_seq
  as integer;
alter sequence range_point_seq owned by range_point.id;

DROP TABLE IF EXISTS range_point;
create table public.range_point (
  id integer primary key not null default nextval('range_point_seq'::regclass),
  geom geometry(Point,4283),
  taxon_id_r character varying,
  sp_id integer,
  notes text
);
create index idx_range_point_geom on range_point using gist (geom);
create index range_point_sp_id_index on range_point using btree (sp_id);
create index range_point_taxon_id_r_index on range_point using btree (taxon_id_r);
comment on table range_point is 'to denote breeding islands for seabirds initially';


