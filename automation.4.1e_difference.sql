
create table public.range_change (
  id integer primary key not null default nextval('range_change_seq'::regclass),
  geom geometry(MultiPolygon,4283),
  taxon_id_r character varying,
  change character varying
);

-- create sequence autonumber on id if required
create sequence range_change_seq as integer;
alter sequence range_change_seq owned by range_change.id;
alter table range_change
  alter column id set default nextval('range_change_seq');

INSERT INTO range_change (taxon_id_r, change, geom)
  WITH new_hull AS
    (SELECT
      taxon_id_r,
      geom
    FROM processed_hulls
    WHERE
      sp_id = 967
      AND class = 1
    ),
  previous_hull AS
    (SELECT
      taxon_id_r,
      geom
    FROM range_birdata
    WHERE
      sp_id = 967
      AND class = 1
    )
  SELECT
    previous_hull.taxon_id_r,
    'added' AS change,
    ST_Difference(new_hull.geom, previous_hull.geom) AS geom
  FROM new_hull
  JOIN previous_hull ON ST_Intersects(new_hull.geom, previous_hull.geom)
;
INSERT INTO range_change (taxon_id_r, change, geom)
WITH new_hull AS
  (SELECT
    taxon_id_r,
    geom
  FROM processed_hulls
  WHERE
    sp_id = 967
    AND class = 1
  ),
previous_hull AS
  (SELECT
    taxon_id_r,
    geom
  FROM range_birdata
  WHERE
    sp_id = 967
    AND class = 1
  )
SELECT
  previous_hull.taxon_id_r,
  'subtracted' AS change,
  ST_Difference(previous_hull.geom, new_hull.geom) AS geom
FROM new_hull
JOIN previous_hull ON ST_Intersects(previous_hull.geom, new_hull.geom)
;


