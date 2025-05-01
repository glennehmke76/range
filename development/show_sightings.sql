
DROP VIEW IF EXISTS tmp_sightings;
CREATE VIEW tmp_sightings AS
SELECT
  row_number() over () as id,
  survey.id AS survey_id,
	survey.source_id,
	source.name AS source_name,
	survey.source_ref,
	st_y(survey.geom) AS latitude,
	st_x(survey.geom) AS longitude,
  coalesce(extract(year from survey.start_date),0) :: integer AS year,
	survey.start_date,
	survey.start_time,
	survey.finish_date,
	survey.duration_in_minutes,
	survey.survey_type_id,
	survey_type.name AS survey_type_name,
	sighting.id AS sighting_id,
	-- sighting_location.lat AS sighting_latitude,
	-- sighting_location.lon AS sighting_longitude,
	sighting.sp_id AS sp_id,
	sighting.individual_count AS count,
	sighting.breeding_activity_id,
	breeding_activity.name AS breeding_activity_name,
	survey.geom AS geom
FROM survey
JOIN sighting ON survey.id = sighting.survey_id
LEFT JOIN source ON survey.source_id = source.id
LEFT JOIN survey_type ON survey.survey_type_id = survey_type.id
LEFT JOIN breeding_activity ON sighting.breeding_activity_id = breeding_activity.id
WHERE sighting.sp_id = 967
;

-- does distinct with a count of surveys per year speed query and display up?
DROP VIEW IF EXISTS tmp_sightings;
CREATE VIEW tmp_sightings AS
SELECT
  row_number() over () AS id,
  survey.id AS survey_id,
  coalesce(extract(year from survey.start_date),0) :: integer AS year,
	survey.geom AS geom
FROM survey
JOIN sighting ON survey.id = sighting.survey_id
WHERE
  sighting.sp_id = 967;

-- range constrained
DROP VIEW IF EXISTS tmp_sightings;
CREATE VIEW tmp_sightings AS
SELECT
  row_number() over () AS id,
  survey.id AS survey_id,
	extract(year from survey.start_date) AS year,
	range.taxon_id_r,
	range.class,
	survey_point.geom AS geom
FROM survey
JOIN survey_point ON survey.survey_point_id = survey_point.id
JOIN sighting ON survey.id = sighting.survey_id
-- to constrain to current range...
JOIN range
  ON sighting.sp_id = range.sp_id
  AND ST_Intersects(survey.geom,range.geom)
WHERE
  sighting.sp_id = [specify]
  -- to constrain to current range...
  AND range.sp_id = [specify]
--   AND range.rnge = 1
;