

-- # species
SELECT
  count(distinct wlist_range.sp_id)
FROM wlist_range
JOIN wlist_sp ON wlist_range.sp_id = wlist_sp.sp_id



-- # ultrataxa
SELECT
  count(distinct wlist.taxon_id)
FROM wlist_range
JOIN wlist ON wlist_range.sp_id = wlist.sp_id
WHERE wlist.is_ultrataxon = 1

-- # subsoecies
SELECT
  count(wlist_range.taxon_id)
FROM wlist_range
JOIN wlist ON wlist_range.taxon_id = wlist.taxon_id
WHERE wlist.taxon_level = 'ssp'
;


-- # taxa
SELECT DISTINCT
  wlist.taxon_level,
  count(wlist_range.taxon_id)
FROM wlist_range
JOIN wlist ON wlist_range.taxon_id = wlist.taxon_id
GROUP BY
  wlist.taxon_level





SELECT DISTINCT
  wlist.taxon_name,
  lut_class.description,
  range.geom
FROM range
JOIN wlist_range ON range.taxon_id_r = wlist_range.taxon_id_r
JOIN wlist ON wlist_range.taxon_id = wlist.taxon_id
JOIN lut_class ON range.class = lut_class.id





SELECT DISTINCT
  range.*,
  wlist.taxon_name
FROM range
JOIN wlist_range ON range.taxon_id_r = wlist_range.taxon_id_r
JOIN wlist ON wlist_range.taxon_id = wlist.taxon_id
WHERE range.class = 5
;



-- summarise
-- select taxa with class = 3 (or other class) as CTE
SELECT
  wlist_sp.bird_group,
  COUNT(sp_id) AS num_sp_group,
  num_has_3.num_sp AS num_sp_with_3,
  round(num_has_3.num_sp / COUNT(sp_id) :: numeric * 100, 2) AS perc_with_3
FROM
    (WITH has_class_3 AS
        (SELECT DISTINCT
          wlist_sp.*
        FROM range
        JOIN wlist_sp ON range.sp_id = wlist_sp.sp_id
        JOIN wlist_covariates ON wlist_sp.taxon_id = wlist_covariates.taxon_id_cov
        WHERE
          range.class = 3
--           AND wlist_covariates.coastal_range_ge IS NULL
        )
    SELECT
      has_class_3.bird_group,
      COUNT(has_class_3.sp_id) AS num_sp
    FROM has_class_3
    GROUP BY
      has_class_3.bird_group
    )num_has_3
JOIN wlist_sp ON num_has_3.bird_group = wlist_sp.bird_group
JOIN wlist_covariates ON wlist_sp.taxon_id = wlist_covariates.taxon_id_cov
WHERE
  wlist_covariates.coastal_range_ge IS NULL
GROUP BY
  wlist_sp.bird_group,
  num_has_3.num_sp
;

-- taxa with distinct mapped breeding non-breeding classes
DROP VIEW IF EXISTS has_br_class;
CREATE VIEW has_br_class AS
SELECT
  taxon_sort,
  is_ultrataxon,
  taxon_level,
  sp_id,
  taxon_id,
  taxon_name,
  bird_group,
  string_agg(description, ', ') AS breeding_range_types
FROM
    (SELECT DISTINCT
      range.br_class,
      lut_breeding_class.description,
      wlist.*
    FROM range
    JOIN wlist ON range.sp_id = wlist.sp_id
    JOIN lut_breeding_class ON range.br_class = lut_breeding_class.id
    WHERE
      range.br_class > 0
    )sub
GROUP BY
  taxon_sort,
  is_ultrataxon,
  taxon_level,
  sp_id,
  taxon_id,
  bird_group,
  taxon_name
ORDER BY
  taxon_sort
;



SELECT
  wlist.bird_group,
  COUNT(sp_id) AS num_sp_group,
  num_has_br_class.num_sp AS num_sp_with_3,
  round(num_has_br_class.num_sp / COUNT(sp_id) :: numeric * 100, 2) AS perc_with_br_class
FROM
    (WITH has_br_class AS
        (SELECT DISTINCT
          wlist.*
        FROM range
        JOIN wlist ON range.sp_id = wlist.sp_id
        JOIN wlist_covariates ON wlist.taxon_id = wlist_covariates.taxon_id_cov
        WHERE
          range.br_class > 0
        )
    SELECT
      has_br_class.bird_group,
      COUNT(has_br_class.sp_id) AS num_sp
    FROM has_br_class
    GROUP BY
      has_br_class.bird_group
    )num_has_br_class
JOIN wlist ON num_has_br_class.bird_group = wlist.bird_group
GROUP BY
  wlist.bird_group,
  num_has_br_class.num_sp
;








-- is coastal
SELECT
  *
FROM wlist
LEFT JOIN wlist_covariates ON wlist.taxon_id = wlist_covariates.taxon_id_cov
WHERE
--   coalesce(wlist_covariates.coastal_range_ge, wlist_covariates.coastal_range, NULL) IS NOT NULL
  wlist_covariates.coastal_range_ge IS NOT NULL


SELECT DISTINCT
  bird_group
from wlist
ORDER by
  bird_group,
  bird_sub_group


SELECT DISTINCT
  population
from wlist


SELECT DISTINCT
  wlist.taxon_id,
  wlist.bird_group,
  wlist.taxon_name
FROM range
JOIN wlist_range ON range.taxon_id_r = wlist_range.taxon_id_r
JOIN wlist ON wlist_range.taxon_id = wlist.taxon_id
WHERE
  range.class = 4
;

SELECT DISTINCT
  wlist_sp.sp_id,
  wlist_sp.bird_group,
  wlist_sp.taxon_name
FROM range
JOIN wlist_sp ON range.sp_id = wlist_sp.sp_id
WHERE
  range.class = 4
;


