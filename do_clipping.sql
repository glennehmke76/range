-- seabird clipping
          INCORRECT
          WITH continent AS
            (SELECT
              id,
              ST_Transform
                (ST_Buffer
                  (ST_Transform
                    (geom, 3112), 1000, 'quad_segs=2, side=right'), 4283) AS geom
            FROM region_continental
            )
          SELECT DISTINCT
            range.id,
            range.taxon_id_r,
            range.sp_id,
            range.class,
            range.breeding_class,
            ST_Difference(range.geom, continent.geom) AS geom
          FROM range
          JOIN continent ON ST_Disjoint(range.geom, continent.geom)
          JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
          JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
          WHERE
            wlab.bird_group = 'Marine'
          ;


WITH continent AS
  (SELECT
    id,
    ST_Transform
      (ST_Buffer
        (ST_Transform
          (geom, 3112), 1000, 'quad_segs=2, side=right'), 4283) AS geom
  FROM region_continental
  )
SELECT DISTINCT
  range.id,
  range.taxon_id_r,
  range.sp_id,
  range.class,
  range.breeding_class,
  ST_Difference(range.geom, continent.geom) AS geom
FROM range
JOIN continent ON ST_Disjoint(range.geom, continent.geom)
JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
WHERE
  wlab.bird_group = 'Marine'
;


-- done manually in QGIS using CTE above on layers below and then excluding various error polygons
SELECT
  range.id,
  range.taxon_id_r,
  range.sp_id,
  range.rnge,
  range.br_rnge,
  range.geom
  FROM range
JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
WHERE
  wlab.bird_group = 'Marine'
;

-- import as range_seabirds_clipped
alter table range_seabirds_clipped
    drop column _uid_;

-- continental clipping
make another continent layer or add substantiative islands and exclude from seabird clipping etc
DROP TABLE IF EXISTS tmp_continent;
CREATE TABLE tmp_continent AS
  (SELECT
    ST_Union
      (ST_Transform
        (ST_Buffer
          (ST_Transform(geom, 3112), 2000, 'quad_segs=2, side=left'), 4283)) AS geom
  FROM region_continental
  );
CREATE INDEX idx_tmp_continent_geom ON tmp_continent USING gist (geom);
SELECT DISTINCT
  range.id,
  range.taxon_id_r,
  range.sp_id,
  range.rnge,
  range.br_rnge,
  ST_Intersection(range.geom, tmp_continent.geom) AS geom
FROM range
JOIN tmp_continent ON ST_Intersects(range.geom, tmp_continent.geom)
JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
WHERE
  (wlab.bird_group <> 'Marine'
  AND coastal IS NULL)
  AND
    -- exclude species with distributions on nearshore islands
    (range.sp_id <> 97
    AND range.sp_id <> 99
    AND range.sp_id <> 100
    AND range.sp_id <> 110
    AND range.sp_id <> 146
    AND range.sp_id <> 168
    AND range.sp_id <> 187
    AND range.sp_id <> 188
    AND range.sp_id <> 8415
    );
DROP TABLE IF EXISTS tmp_continent;


