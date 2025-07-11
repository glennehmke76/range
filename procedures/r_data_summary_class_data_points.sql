DROP PROCEDURE IF EXISTS r_data_summary_class_data_points;
CREATE OR REPLACE PROCEDURE r_data_summary_class_data_points(p_sp_id integer)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name text;
    v_sql text;
    v_periods jsonb := '[
        ["2000-01-01", null],
        ["1990-01-01", null],
        [null, "1989-12-31"],
        [null, "1969-12-31"],
        [null, "1949-12-31"]
    ]'::jsonb;
    v_period_names text[] := array[
        'post-2000',
        'post-1990',
        'pre-1990',
        'pre-1970',
        'pre-1950'
    ];
    period_idx int;
    period_from date;
    period_to date;
    dyn_table_name text;
    class_record RECORD;
    sightings_srid integer;
    range_srid integer;
    period_total integer;
BEGIN
    -- Dynamic schema/table name
    v_schema_name := format('rl_%s', p_sp_id::text);
    dyn_table_name := quote_ident(v_schema_name) || '.sightings';

    -- Get the SRID of the sightings geometry
    EXECUTE format('
        SELECT ST_SRID(geom) FROM %s LIMIT 1
    ', dyn_table_name) INTO sightings_srid;

    -- If we couldn't determine the SRID, default to 4283
    IF sightings_srid IS NULL THEN
        sightings_srid := 4283;
    END IF;

    -- Get the SRID of the range geometry
    EXECUTE 'SELECT ST_SRID(geom) FROM range WHERE sp_id = $1 LIMIT 1'
    INTO range_srid USING p_sp_id;

    -- If we couldn't determine the SRID, default to 4283
    IF range_srid IS NULL THEN
        range_srid := 4283;
    END IF;

    RAISE NOTICE 'Using SRID % for sightings geometry and % for range geometry', sightings_srid, range_srid;

    -- Create output table
    v_sql := format('DROP TABLE IF EXISTS %I.r_data_summary_class_data_points', v_schema_name);
    EXECUTE v_sql;

    -- Create the table with period and class columns
    v_sql := format('
        CREATE TABLE %I.r_data_summary_class_data_points (
            period text NOT NULL,
            class text NOT NULL,
            count integer NOT NULL,
            percentage numeric(5,2) NOT NULL
        )
    ', v_schema_name);
    EXECUTE v_sql;

    -- For each period, calculate counts and percentages by class from range
    FOR period_idx IN 1..jsonb_array_length(v_periods) LOOP
        -- Get current period
        period_from := nullif(v_periods -> (period_idx-1) ->> 0, '')::date;
        period_to := nullif(v_periods -> (period_idx-1) ->> 1, '')::date;

        -- Get total sightings for this period
        EXECUTE format('
            SELECT COUNT(*)
            FROM %s
            WHERE %s
        ',
        dyn_table_name,
        CASE
            WHEN period_from IS NOT NULL AND period_to IS NOT NULL THEN
                format('start_date >= %L AND start_date <= %L', period_from, period_to)
            WHEN period_from IS NOT NULL THEN
                format('start_date >= %L', period_from)
            WHEN period_to IS NOT NULL THEN
                format('start_date <= %L', period_to)
            ELSE 'TRUE'
        END) INTO period_total;

        RAISE NOTICE 'Period %: total sightings = %', v_period_names[period_idx], period_total;

        -- Get all classes in range table for this species
        FOR class_record IN EXECUTE 'SELECT DISTINCT class FROM range WHERE sp_id = $1' USING p_sp_id
        LOOP
            -- Calculate count and percentage for this class and period
            EXECUTE format('
                WITH class_sightings AS (
                    SELECT s.*
                    FROM %s s
                    JOIN range r ON
                        r.sp_id = %L AND
                        r.class = %L AND
                        ST_Intersects(
                            s.geom,
                            ST_Transform(r.geom, %s)
                        )
                    WHERE %s
                )
                INSERT INTO %I.r_data_summary_class_data_points (period, class, count, percentage)
                SELECT
                    %L AS period,
                    %L AS class,
                    COUNT(*) AS count,
                    CASE
                        WHEN %s > 0 THEN ROUND((COUNT(*) * 100.0 / %s)::numeric, 2)
                        ELSE 0.00
                    END AS percentage
                FROM class_sightings
            ',
            dyn_table_name,
            p_sp_id,
            class_record.class,
            sightings_srid,
            CASE
                WHEN period_from IS NOT NULL AND period_to IS NOT NULL THEN
                    format('s.start_date >= %L AND s.start_date <= %L', period_from, period_to)
                WHEN period_from IS NOT NULL THEN
                    format('s.start_date >= %L', period_from)
                WHEN period_to IS NOT NULL THEN
                    format('s.start_date <= %L', period_to)
                ELSE 'TRUE'
            END,
            v_schema_name,
            v_period_names[period_idx],
            class_record.class,
            period_total,
            period_total);
        END LOOP;
    END LOOP;

    -- Add a row for "All classes" for each period
    FOR period_idx IN 1..jsonb_array_length(v_periods) LOOP
        -- Get current period
        period_from := nullif(v_periods -> (period_idx-1) ->> 0, '')::date;
        period_to := nullif(v_periods -> (period_idx-1) ->> 1, '')::date;

        -- Get total sightings for this period
        EXECUTE format('
            SELECT COUNT(*)
            FROM %s
            WHERE %s
        ',
        dyn_table_name,
        CASE
            WHEN period_from IS NOT NULL AND period_to IS NOT NULL THEN
                format('start_date >= %L AND start_date <= %L', period_from, period_to)
            WHEN period_from IS NOT NULL THEN
                format('start_date >= %L', period_from)
            WHEN period_to IS NOT NULL THEN
                format('start_date <= %L', period_to)
            ELSE 'TRUE'
        END) INTO period_total;

        -- Add the "All classes" row with 100% if there are any sightings in this period
        IF period_total > 0 THEN
            EXECUTE format('
                INSERT INTO %I.r_data_summary_class_data_points (period, class, count, percentage)
                VALUES (%L, ''All classes'', %s, 100.00)
            ',
            v_schema_name,
            v_period_names[period_idx],
            period_total);
        ELSE
            -- If no sightings in this period, add a row with 0 count and 0%
            EXECUTE format('
                INSERT INTO %I.r_data_summary_class_data_points (period, class, count, percentage)
                VALUES (%L, ''All classes'', 0, 0.00)
            ',
            v_schema_name,
            v_period_names[period_idx]);
        END IF;
    END LOOP;

    -- Create an index on the period and class columns for better query performance
    EXECUTE format('
        CREATE INDEX idx_%I_r_data_summary_class_data_points_period_class
        ON %I.r_data_summary_class_data_points(period, class)
    ', v_schema_name, v_schema_name);
END
$$;

CALL r_data_summary_class_data_points(652);
