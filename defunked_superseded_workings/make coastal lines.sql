-- make master coastline - low'ish res
--  = region_coastline - very simplified with external islands but not internal islands


--   take proceedure from biennial and make lines for layers where = shoreline + coastal


SELECT
  ST_Union(sub.geom) AS geom
FROM
    (SELECT
      range.taxon_id_r,
      range.sp_id,
      range.rnge,
      range.br_rnge,
      ST_CollectionExtract
        (ST_Intersection(region_coastline.geom, range.geom)) AS geom
    FROM region_coastline
    JOIN range ON ST_Intersects(region_coastline.geom, range.geom)
    JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
    JOIN wlab ON wlab_range.taxon_id = wlab.taxon_id
    JOIN wlab_covariates ON wlab.taxon_id = wlab_covariates.taxon_id_cov
    WHERE
      wlab_covariates.coastal_range = 1
      AND wlab.bird_group LIKE 'Shoreline%'
    )sub
;

-- buffer out coastal ranges to areas

