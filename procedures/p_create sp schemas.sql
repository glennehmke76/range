CREATE OR REPLACE PROCEDURE create_sp_schemas(p_sp_ids integer[])
LANGUAGE plpgsql
AS $$
DECLARE
  sp_id integer;
  schema_name varchar;
  success_count integer := 0;
  error_count integer := 0;
BEGIN
    -- Loop through each sp_id in array
    FOREACH sp_id IN ARRAY p_sp_ids
    LOOP
        BEGIN
            -- Validate individual sp_id
            IF sp_id IS NULL THEN
              RAISE WARNING 'Skipping NULL sp_id';
              error_count := error_count + 1;
              CONTINUE;
            END IF;

            -- Set schema name for current sp_id
            schema_name := 'rl_' || sp_id;

            -- Create schema
            EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);

            -- Create sightings
            EXECUTE format('
                CREATE TABLE %I.sightings AS
                SELECT DISTINCT
                  row_number() OVER () as id,
                  survey.id AS survey_id,
                  survey.data_source,
                  survey.source_id,
                  source.name AS source_name,
                  survey.source_ref,
                  coalesce(extract(year from survey.start_date),0) :: integer AS year,
                  survey.start_date,
                  survey.start_time,
                  survey.finish_date,
                  survey.duration_in_minutes,
                  survey.survey_type_id,
                  survey_type.name AS survey_type_name,
                  sighting.id AS sighting_id,
                  sighting.sp_id AS sp_id,
                  sighting.individual_count AS count,
                  sighting.breeding_activity_id,
                  sighting.vetting_status_id,
                  CASE
                      WHEN sighting.vetting_status_id = 3 THEN 0
                      ELSE NULL :: integer
                  END AS class_specified,
                  survey.geom
                FROM survey
                JOIN sighting
                    ON survey.id = sighting.survey_id
                    AND survey.data_source = sighting.data_source
                JOIN source ON survey.source_id = source.id
                JOIN survey_type ON survey.survey_type_id = survey_type.id
                WHERE sighting.sp_id = %s
            ', schema_name, sp_id);

            EXECUTE format('
                ALTER TABLE %I.sightings
                ADD CONSTRAINT tmp_region_sightings_pk
                PRIMARY KEY (id)
            ', schema_name);
            EXECUTE format('
                CREATE INDEX IF NOT EXISTS idx_sightings_sp_id
                ON %I.sightings (sp_id)
            ', schema_name);
            EXECUTE format('
                CREATE INDEX IF NOT EXISTS idx_sightings_geom
                ON %I.sightings USING gist (geom)
                TABLESPACE pg_default
            ', schema_name);

            success_count := success_count + 1;
            RAISE NOTICE 'Created schema and sightings table for sp_id: %', sp_id;

        EXCEPTION WHEN OTHERS THEN
            error_count := error_count + 1;
            RAISE WARNING 'Error processing sp_id %: %', sp_id, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE 'Processing complete. Successfully processed: %, Errors: %', success_count, error_count;
END;
$$;
-- CALL create_sp_schemas(ARRAY[7, 8, 9]);