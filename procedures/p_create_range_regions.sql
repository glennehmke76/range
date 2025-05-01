CREATE OR REPLACE PROCEDURE create_regions_spc_methods(
  p_sp_id INTEGER,
  p_start_year INTEGER,
  p_end_year INTEGER,
  p_survey_types INTEGER[],
  p_region_suffix TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  regionalisation TEXT;
  v_schema_name TEXT;
BEGIN
    -- Validate inputs
    IF p_sp_id IS NULL OR p_start_year IS NULL OR p_end_year IS NULL OR
       p_survey_types IS NULL OR p_region_suffix IS NULL THEN
        RAISE EXCEPTION 'All parameters must be provided';
    END IF;

    -- Set schema name
    v_schema_name := 'rl_' || p_sp_id;

    -- Define regionalisation name
    regionalisation := 'region_' || p_region_suffix;

    -- Validate regionalisation
    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_name = regionalisation
    ) THEN
      RAISE EXCEPTION 'Regionalisation does not exist', regionalisation;
    END IF;

    -- Set the search path to the new schema then public
    EXECUTE format('SET search_path TO %I, public', v_schema_name);

    -- Create constrained regions
    EXECUTE format('
    CREATE TABLE tmp_constrained_region AS
    WITH tmp_simple_region AS (
        SELECT
          id,
          ST_SimplifyPreserveTopology(geom, 0.01) AS geom
        FROM %I
    )
    SELECT
      tmp_simple_region.id,
      ST_Intersection(tmp_simple_region.geom, base_hulls.geom) AS geom
    FROM tmp_simple_region
    JOIN base_hulls ON ST_Intersects(tmp_simple_region.geom, base_hulls.geom)', regionalisation);
    CREATE INDEX IF NOT EXISTS idx_tmp_region_region_id ON tmp_constrained_region (id);
    CREATE INDEX IF NOT EXISTS idx_tmp_region_geom ON tmp_constrained_region USING gist (geom);

   -- Create range_region.
   -- temp tables used (tried CTE which failed). Perhaps useful to add indexes to speed up processing
    CREATE TEMPORARY TABLE surveys_by AS
    SELECT
      tmp_constrained_region.id AS region_id,
      extract(year from survey.start_date) AS year,
      COUNT(survey.id) AS num_surveys
    FROM survey
    JOIN source ON survey.source_id = source.id
    JOIN survey_type ON survey.survey_type_id = survey_type.id
    JOIN tmp_constrained_region ON ST_Intersects(survey.geom, tmp_constrained_region.geom)
    WHERE extract(year from survey.start_date) >= p_start_year
    AND extract(year from survey.finish_date) <= p_end_year
    AND survey.survey_type_id = ANY(p_survey_types)
    GROUP BY
      tmp_constrained_region.id,
      extract(year from survey.start_date);

    -- Create sightings_by temporary table
    CREATE TEMPORARY TABLE sightings_by AS
    SELECT
      tmp_constrained_region.id AS region_id,
      extract(year from survey.start_date) AS year,
      sightings.sp_id,
      COUNT(survey.id) AS num_sightings
    FROM survey
    JOIN sightings
      ON survey.id = sightings.survey_id
      AND survey.data_source = sightings.data_source
  JOIN source ON survey.source_id = source.id
    JOIN survey_type ON survey.survey_type_id = survey_type.id
    JOIN tmp_constrained_region ON ST_Intersects(survey.geom, tmp_constrained_region.geom)
    WHERE extract(year from survey.start_date) >= p_start_year
    AND extract(year from survey.finish_date) <= p_end_year
    AND survey.survey_type_id = ANY(p_survey_types)
    AND sightings.sp_id = p_sp_id
    GROUP BY
        tmp_constrained_region.id,
        sightings.sp_id,
        extract(year from survey.start_date);
    DROP TABLE IF EXISTS range_region;
    CREATE TABLE range_region AS
    SELECT
      ST_Multi(ST_Union(tmp_constrained_region.geom)) AS geom,
      regionalisation AS regionalisation,
      yearly.region_id,
      yearly.sp_id,
      SUM(yearly.num_surveys) AS num_surveys,
      SUM(yearly.num_sightings) AS num_sightings,
      COUNT(yearly.year) AS num_sighting_years,
      AVG(yearly.mean_yearly_rr) AS mean_yearly_rr
    FROM (
        SELECT
          surveys_by.region_id,
          sightings_by.sp_id,
          surveys_by.year,
          surveys_by.num_surveys,
          sightings_by.num_sightings,
          sightings_by.num_sightings / surveys_by.num_surveys::decimal * 100 AS mean_yearly_rr
        FROM sightings_by
        JOIN surveys_by
          ON sightings_by.region_id = surveys_by.region_id
          AND sightings_by.year = surveys_by.year
    ) yearly
    JOIN tmp_constrained_region ON yearly.region_id = tmp_constrained_region.id
    GROUP BY
      yearly.region_id,
      yearly.sp_id;

    -- Add and update percentile field
    ALTER TABLE range_region ADD mean_yearly_rr_percentile decimal;

    UPDATE range_region
    SET mean_yearly_rr_percentile = CASE
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.05) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 5
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.1) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 10
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.2) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 20
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.3) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 30
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.4) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 40
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 50
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.6) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 60
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.7) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 70
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.8) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 80
      WHEN mean_yearly_rr < (SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY mean_yearly_rr) FROM range_region) THEN 90
      ELSE 100
    END;

    -- Cleanup temp tables
    DROP TABLE IF EXISTS tmp_constrained_region;
    DROP TABLE IF EXISTS surveys_by;
    DROP TABLE IF EXISTS sightings_by;

    RAISE NOTICE 'Created range_region for sp_id: %, region: %, years: % to %',
                 p_sp_id, regionalisation, p_start_year, p_end_year;

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE 'Error occurred: %', SQLERRM;
    ROLLBACK;
    RAISE;
END;
$$;

-- CALL create_regions_spc_methods(
--   p_sp_id := 2,
--   p_start_year := 1900,
--   p_end_year := 2023,
--   p_survey_types := ARRAY[1,2],
--   p_region_suffix := 'ibra'
-- );
