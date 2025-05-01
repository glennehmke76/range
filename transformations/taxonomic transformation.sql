


-- embellishment to sighting
alter table public.sighting
  add sp_id_v4 integer;

create index sighting_sp_id_v4_index
  on public.sighting (sp_id_v4);

-- for 641 = 193,417 rows affected in 6 m 26 s 336 ms
WITH
selected_range AS -- select the relevent new range geometries
  (SELECT
    *
  FROM range
  WHERE
    sp_id = 641
    OR range.sp_id = 8300
  ),
selected_surveys AS -- sub-select the sightings to be transformed + geometries points from surveys (or survey_points)
  (SELECT
    survey.id,
    survey.geom
  FROM survey
  JOIN sighting ON survey.id = sighting.survey_id
  WHERE
    sighting.sp_id = 641
  )
UPDATE sighting
SET sp_id_v4 = sub.sp_id
FROM
    (SELECT
      sighting.id,
      selected_range.sp_id
    FROM selected_range
    JOIN selected_surveys ON ST_Intersects(selected_range.geom, selected_surveys.geom)
    JOIN sighting ON selected_surveys.id = sighting.survey_id
    WHERE
      sighting.sp_id = 641 -- filter to old sp_ids to be transformed
    )sub
WHERE
  sub.id = sighting.id
;

SELECT
  *
FROM sighting
WHERE
  sp_id_v4 IS NULL
  AND sp_id = 641