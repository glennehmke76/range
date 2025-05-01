
create sequence range_region_seq
  as integer;
drop table if exists range_region;
create table range_region (
  id integer primary key not null default nextval('range_region_seq'::regclass),
  geom geometry(MultiPolygon,4283),
  regionalisation text,
  region_id integer,
  sp_id integer,
  num_surveys bigint,
  num_sightings bigint,
  num_sighting_years bigint,
  mean_yearly_rr numeric,
  mean_yearly_rr_percentile integer,
  global_rr numeric,
  global_rr_percentile integer
);
alter sequence range_region_seq owned by range_region.id;
create index idx_range_region_rr_geom on range_region using gist (geom);
alter table range_region
    add class integer;
comment on column range_region.class is 'as user-specified expert class for running a core/irregular hull';

create table public.base_hulls (
  id integer primary key not null default nextval('base_hulls_seq'::regclass),
  hull_type text,
  regionalisation text,
  alpha double precision,
  sp_id integer,
  class integer,
  core_rr_precentile integer,
  geom geometry(MultiPolygon,4283)
);

-- add permutation field for tighter core areas via irregular region subtractions
-- default is 1 which is strict, 2 is irregular regions subtracted from overall as alpha hulls
alter table public.base_hulls
    add permutation integer default 1;
comment on column public.base_hulls.permutation is 'default is 1 which is strict, 2 is irregular regions subtracted from overall as alpha hulls';


create sequence base_hulls_processed_seq
  as integer;
DROP TABLE IF EXISTS base_hulls_processed;
CREATE TABLE base_hulls_processed (
  id integer primary key not null default nextval('base_hulls_processed_seq'::regclass),
  hull_type text,
  regionalisation text,
  alpha float,
  sp_id integer,
  taxon_id_r varchar,
  class integer,
  core_rr_precentile integer,
  br_class integer,
  geom geometry(MultiPolygon,4283)
);
alter sequence base_hulls_processed_seq owned by base_hulls_processed.id;


-- make schema
create schema range_634;





