SELECT
	range.id,
	range.geom,
	range.sp_id
	range.taxon_id_r,
	range.rnge,
	range.br_rnge,
	range.geom_valid,
	range.geom_invalid_detail,
  wlab

FROM range
JOIN
JOIN



SELECT
	range.*,
	range_birdata.*
FROM range
JOIN range_birdata
ON ST_Intersects(range.geom,range_birdata.geom)
WHERE
	range.sp_id = 355
	AND range_birdata.sp_id = 355
;

SELECT
	range.sp_id,
	range_birdata.sp_id,
	range.taxon_id_r,
	range_birdata.taxon_id_r,
	range.rnge,
	range_birdata.rnge,
	range.br_rnge,
	range_birdata.br_rnge,
	ST_SymDifference(range.geom,range_birdata.geom)
FROM
	range,
	range_birdata
WHERE
	range.sp_id = 355
	AND range_birdata.sp_id = 355
;

SELECT
	range.sp_id,
	range_birdata.sp_id,
	range.taxon_id_r,
	range_birdata.taxon_id_r,
	range.rnge,
	range_birdata.rnge,
	range.br_rnge,
	range_birdata.br_rnge,
	ST_Union(range.geom,range_birdata.geom)
FROM
	range,
	range_birdata
WHERE
	range.sp_id = 355
	AND range_birdata.sp_id = 355
;



create table tmp_extract_lainie_sp
(
    taxon_name text
);

COPY tmp_extract_lainie_sp FROM '/Users/glennehmke/Downloads/lainie_extract_sp.csv' DELIMITER ';' CSV HEADER;



