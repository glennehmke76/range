create table tmp_extract_lainie_sp
(taxon_name text
);
COPY tmp_extract_lainie_sp FROM '/Users/glennehmke/Downloads/lainie_extract_sp.csv' DELIMITER ';' CSV HEADER;

-- check/fix common names
SELECT
  tmp_extract_lainie_sp.*,
  wlist_sp.sp_id
FROM tmp_extract_lainie_sp
LEFT JOIN wlist_sp ON tmp_extract_lainie_sp.taxon_name = wlist_sp.taxon_name

-- White-bellied Sea-eagle is null - change to White-bellied Sea-Eagle

SELECT
  extraction.sp_id,
  extraction.taxon_name,
  extraction.class,
  lut_class.description AS class_desc,
  extraction.breeding_class,
  lut_breeding_class.description AS lut_breeding_class_desc,
  ST_Union(extraction.geom) AS geom
FROM
    (SELECT
      range.sp_id,
      wlist_sp.taxon_name,
      range.class,
      range.breeding_class,
      (ST_Intersection(range.geom, region_continental.geom)) AS geom
    FROM range
    JOIN wlist_sp ON range.sp_id = wlist_sp.sp_id
    JOIN tmp_extract_lainie_sp ON wlist_sp.taxon_name = tmp_extract_lainie_sp.taxon_name
    JOIN region_continental ON ST_Intersects(range.geom, region_continental.geom)
    )extraction
JOIN lut_class ON extraction.class = lut_class.class
JOIN lut_breeding_class ON extraction.breeding_class = lut_breeding_class.breeding_class
GROUP BY
  extraction.sp_id,
  extraction.taxon_name,
  extraction.class,
  lut_class.description,
  extraction.breeding_class,
  lut_breeding_class.description
;

