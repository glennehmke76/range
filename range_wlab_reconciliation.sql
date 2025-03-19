-- identify wlab entries that do not have range geometries
  -- resolve these issues before going to the subsequent step
DROP VIEW IF EXISTS wlab_check_wlab_no_range;
CREATE VIEW wlab_check_wlab_no_range AS
WITH range_all AS
  (SELECT DISTINCT
    COALESCE(range.sp_id, range_line.sp_id) AS sp_id,
    COALESCE(range.taxon_id_r, range_line.taxon_id_r) AS taxon_id_r
  FROM range
  FULL OUTER JOIN range_line ON range.taxon_id_r = range_line.taxon_id_r
  WHERE
    COALESCE(range.taxon_id_r, range_line.taxon_id_r) IS NOT NULL
  )
SELECT
  wlab.taxon_id,
  wlab.to_resolve,
  wlab.taxon_name,
  wlab.population,
  wlab.supplementary,
  wlab.coastal
FROM
    (SELECT DISTINCT
      wlab.sp_id,
      wlab.taxon_id,
      wlab.taxon_name
    FROM wlab
    JOIN wlab_range ON wlab.taxon_id = wlab_range.taxon_id
    JOIN range ON wlab_range.taxon_id_r = range.taxon_id_r
    )range_taxon_ids
FULL OUTER JOIN wlab ON range_taxon_ids.taxon_id = wlab.taxon_id
WHERE
  range_taxon_ids.taxon_id IS NULL
  AND wlab.is_ultrataxon = 1
  AND
    -- filer to core taxa
    (wlab.population = 'Non-breeding'
    OR wlab.population = 'Endemic (breeding only)'
    OR wlab.population = 'Australian'
    OR wlab.population = 'Introduced'
    OR wlab.population IS NULL)
;

-- identify range geometries that do not have wlab_range entries (has range but not wlab_range)
  -- where wlab_taxon_id is not null a wlab_range entry is missing (except for hybrid taxon_ids which cannot be matched to wlab directly), but where wlab_taxon_id is not null are genuine orphans
DROP VIEW IF EXISTS wlab_check_range_no_wlab_range;
CREATE VIEW wlab_check_range_no_wlab_range AS
WITH range_all AS
  (SELECT DISTINCT
    COALESCE(range.sp_id, range_line.sp_id) AS sp_id,
    COALESCE(range.taxon_id_r, range_line.taxon_id_r) AS taxon_id_r
  FROM range
  FULL OUTER JOIN range_line ON range.taxon_id_r = range_line.taxon_id_r
  WHERE
    COALESCE(range.taxon_id_r, range_line.taxon_id_r) IS NOT NULL
  )
SELECT DISTINCT
  range_all.taxon_id_r AS range_taxon_id_r,
  CASE
    WHEN range_all.taxon_id_r LIKE '%.%'
      THEN 'is hybrid - further investigation required'
    WHEN wlab.taxon_id IS NULL
      THEN 'orphan'
    ELSE 'missing from wlab_range'
  END AS likely_issue,
  wlab_range.taxon_id_r AS wlab_range_taxon_id_r,
  wlab.taxon_id AS wlab_taxon_id,
  wlab.taxon_name,
  wlab.population,
  wlab.supplementary,
  wlab.coastal
  FROM range_all
FULL OUTER JOIN wlab_range ON range_all.taxon_id_r = wlab_range.taxon_id_r
FULL OUTER JOIN wlab ON range_all.taxon_id_r = wlab.taxon_id
WHERE
  wlab_range.taxon_id_r IS NULL
  AND range_all.taxon_id_r IS NOT NULL
  AND wlab.to_resolve IS NULL
ORDER BY
  wlab.taxon_id
;

-- THIS IS NO DIFFERENT TO 1st QUERY SURELY?
DROP VIEW IF EXISTS wlab_check_unmapped_species;
CREATE VIEW wlab_check_unmapped_species AS
SELECT DISTINCT
  wlab_sp.taxon_sort,
  wlab_sp.sp_id AS wlab_sp_id,
  wlab_sp.taxon_name,
  wlab_sp.taxon_scientific_name,
  wlab_sp.population,
  wlab_sp.bird_group,
  range.sp_id  AS range_sp_id
FROM wlab_sp
LEFT JOIN range ON wlab_sp.sp_id = range.sp_id
WHERE
 -- to identify unmapped species
  range.sp_id IS NULL
  AND
  (wlab_sp.population = 'Australian'
  OR wlab_sp.population = 'Endemic'
  OR wlab_sp.population = 'Endemic (breeding only)'
  OR wlab_sp.population = 'Non-breeding'
  OR wlab_sp.population = 'Introduced'
  )
ORDER BY wlab_sp.bird_group ASC
;

-- perhaps not neceessary in published version?
DROP VIEW IF EXISTS wlab_check_no_taxon_id;
CREATE VIEW wlab_check_no_taxon_id AS
SELECT DISTINCT
  wlab.*
FROM range
LEFT JOIN wlab_range ON range.sp_id = wlab_range.sp_id
JOIN wlab ON range.sp_id = wlab.sp_id
WHERE
  range.taxon_id_r IS NULL
;


