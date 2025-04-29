-- create buffer only from line
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

