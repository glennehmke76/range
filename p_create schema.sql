CREATE OR REPLACE PROCEDURE create_sp_schemas()
LANGUAGE plpgsql
AS $$
DECLARE
    v_sp_id varchar;
    schema_name varchar;
BEGIN
    -- Loop through selected sp_ids
    FOR v_sp_id IN
        SELECT
          sp_id::varchar
        FROM wlist
        WHERE -- set criteria, this is more likely to be some grouping of species such as bird_group = Wetland etc
          sp_id = 7
          OR sp_id = 8
    LOOP
        -- Set schema name for current sp_id
        schema_name := 'rl_' || v_sp_id;

        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);

        EXECUTE format('DROP TABLE IF EXISTS %I.sightings', schema_name);

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
        ', schema_name, v_sp_id);

        -- Add pk
        EXECUTE format('
            ALTER TABLE %I.sightings
            ADD CONSTRAINT tmp_region_sightings_pk
            PRIMARY KEY (id)
        ', schema_name);

        -- Create indexes
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_sightings_sp_id
            ON %I.sightings (sp_id)
        ', schema_name);

        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_sightings_geom
            ON %I.sightings USING gist (geom)
            TABLESPACE pg_default
        ', schema_name);

        RAISE NOTICE 'Created schema and sighting table for sp_id: %', v_sp_id;
    END LOOP;

    COMMIT;
END;
$$;