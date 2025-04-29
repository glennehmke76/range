-- get differences INCOMPLETE
-- below only gets me added... how to get deleted... presumably another sub-query with a function of some sort... or union.
SELECT
  sub.added,
  sub.deleted,
  ST_Intersection(sub.sym_diff, region_aus.geom)
FROM
  region_aus,
  (SELECT
    range_old.id AS added,
    range.id AS deleted,
    ST_SymDifference(range.geom, range_old.geom) AS sym_diff
  FROM range, range_old
  WHERE
    range.sp_id = 411
    AND range_old.sp_id = 411
  )sub
;








SELECT
  sub.added,
  sub.deleted,
  ST_Intersection(sub.sym_diff, region_aus.geom)
FROM
  region_aus,


  SELECT
    range_old.id AS added,
    range.id AS deleted,

  FROM range, range_old
  WHERE
    ST_SymDifference


-- still no good
CREATE VIEW range_selected AS
SELECT
  range.*
FROM range
WHERE sp_id = 411
;

CREATE VIEW range_old_selected AS
SELECT
  range_old.*
FROM range_old
WHERE sp_id = 411
;

SELECT
  range_old_selected.id AS added,
  range_selected.id AS deleted,
  ST_Union(range_selected.geom, range_old_selected.geom) AS sym_diff
FROM range_selected, range_old_selected
;

then to get added erase range_old from range


-- no good
SELECT
  range.taxon_id_r,
  range_old.taxon_id_r,
  range.rnge,
  range_old.rnge.
  ST_Union(range.geom,range_old.geom)
FROM
  range, range_old
WHERE
  range.sp_id = 411
  AND range_old.sp_id = 411
;
