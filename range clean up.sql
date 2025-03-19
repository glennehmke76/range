-- Query returned successfully in 1 min 35 secs.
BEGIN;
  DROP TABLE IF EXISTS range_;
  CREATE TABLE range_ (LIKE range INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);

  INSERT INTO range_ (geom, taxon_id_r, sp_id, rnge, br_rnge)
  SELECT
    ST_Multi
      (ST_Union(geom)),
    taxon_id_r,
    sp_id,
    rnge,
    br_rnge
  FROM range
  GROUP BY
    taxon_id_r,
    sp_id,
    rnge,
    br_rnge
  ;

  TRUNCATE TABLE range;
  INSERT INTO range (geom, taxon_id_r, sp_id, rnge, br_rnge)
  SELECT
    geom,
    taxon_id_r,
    sp_id,
    rnge,
    br_rnge
  FROM range_
  ;

  UPDATE range
  SET
    geom_valid = sub1.ST_IsValid,
    geom_invalid_detail = sub1.ST_IsValidDetail
  FROM
  (SELECT id, ST_IsValid (geom), ST_IsValidDetail (geom)
  FROM range
  ) sub1
  WHERE range.id = sub1.id;

  DROP TABLE IF EXISTS range_;
COMMIT;


-- dissolve an added polygon
  DROP TABLE IF EXISTS range_;
  CREATE TEMPORARY TABLE range_ AS
  SELECT
    taxon_id_r,
    sp_id,
    rnge,
    br_rnge,
    ST_Union(geom) AS geom
  FROM range
  WHERE
    sp_id = 112
  GROUP BY
    taxon_id_r,
    sp_id,
    rnge,
    br_rnge
  ;
  DELETE FROM range
  WHERE
    sp_id = 112
  ;
  INSERT INTO range (id, geom, taxon_id_r, sp_id, rnge, br_rnge, geom_valid, geom_invalid_detail)
  SELECT
    nextval(range_id_seq),
    taxon_id_r,
    sp_id,
    rnge,
    br_rnge,
    geom,
    NULL,
    NULL
  FROM range_
  ;
  DROP TABLE IF EXISTS range_;

