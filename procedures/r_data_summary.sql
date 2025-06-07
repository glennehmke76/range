DROP PROCEDURE IF EXISTS r_summarise_data_used;
CREATE OR REPLACE PROCEDURE r_summarise_data_used(p_sp_id integer)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name text;
    v_table_name text;
    v_sql text;
    v_periods jsonb := '[ ["2020-01-01", "2024-12-31"],
                          ["2015-01-01", "2019-12-31"],
                          ["2010-01-01", "2014-12-31"],
                          ["1999-01-01", "2009-12-31"],
                          ["1990-01-01", "1998-12-31"],
                          [null, "1989-12-31"] ]'::jsonb;
    v_period_names text[] := array[
        '2020-2024',
        '2015-2020',
        '2010-2015',
        '1999-2010',
        '1990-1999',
        'pre-1999'
    ];
    period_idx int;
    period_from date;
    period_to date;
    region RECORD;
    column_list text := '';
    dyn_table_name text;
    sightings_srid integer;
BEGIN
    -- Dynamic schema/table name
    v_schema_name := format('rl_%s', p_sp_id::text);
    v_table_name := 'sightings';
    dyn_table_name := quote_ident(v_schema_name) || '.sightings';

    -- Get the SRID of the sightings geometry
    EXECUTE format('
        SELECT ST_SRID(geom) FROM %s LIMIT 1
    ', dyn_table_name) INTO sightings_srid;

    -- If we couldn't determine the SRID, default to 4283 (based on error message)
    IF sightings_srid IS NULL THEN
        sightings_srid := 4283;
    END IF;

    RAISE NOTICE 'Using SRID % for sightings geometry', sightings_srid;

    -- Create output table
    v_sql := format('DROP TABLE IF EXISTS %I.r_data_summary', v_schema_name);
    EXECUTE v_sql;
    v_sql := format('CREATE TABLE %I.r_data_summary (
        period text NOT NULL', v_schema_name);

    -- Dynamically add a column per region from public.region_states_simple
    FOR region IN SELECT id, name FROM public.region_states_simple LOOP
        v_sql := v_sql || format(', %I integer DEFAULT 0', region.name);
    END LOOP;
    v_sql := v_sql || ')';
    EXECUTE v_sql;

    -- For each period, count and insert per region
    FOR period_idx IN 1..jsonb_array_length(v_periods) LOOP
        -- Get current period
        period_from := nullif(v_periods -> (period_idx-1) ->> 0, '')::date;
        period_to := (v_periods -> (period_idx-1) ->> 1)::date;

        -- Start building the INSERT statement
        v_sql := format('INSERT INTO %I.r_data_summary (period', v_schema_name);

        -- Add region columns to the INSERT statement
        FOR region IN SELECT id, name FROM public.region_states_simple LOOP
            v_sql := v_sql || format(', %I', region.name);
        END LOOP;

        -- Start the VALUES part of the INSERT
        v_sql := v_sql || format(') VALUES (%L', v_period_names[period_idx]);

        -- For each region, add a subquery to count records for this period using ST_Intersects with SRID handling
        FOR region IN SELECT id, name, geom FROM public.region_states_simple LOOP
            v_sql := v_sql || ', (SELECT count(*) FROM ' || dyn_table_name || ' s WHERE ';

            -- Add date range condition
            IF period_from IS NOT NULL THEN
                v_sql := v_sql || format('s.start_date >= %L AND ', period_from);
            END IF;

            -- Use ST_Intersects with ST_SetSRID to ensure consistent SRID
            v_sql := v_sql || format('s.start_date <= %L AND ST_Intersects(s.geom, ST_SetSRID(%L::geometry, %s)))',
                                    period_to, ST_AsText(region.geom), sightings_srid);
        END LOOP;

        -- Close the VALUES clause
        v_sql := v_sql || ')';

        -- Execute the INSERT statement
        EXECUTE v_sql;
    END LOOP;
END
$$;

CALL r_summarise_data_used(223);

add region to sightings on the fly or add intersectes here???
[42703] ERROR: column "region" does not exist
Hint: Perhaps you meant to reference the column "r_data_summary.period".
Where: PL/pgSQL function r_summarise_data_used(integer) line 77 at EXECUTE


- **Procedure:** `r_summarise_data_used`
- **Input:**
    - `p_sp_id` (species ID)—key for dynamic schema resolution (`v_schema_name := rl_p_sp_id`)

- **Output Table:** In schema `rl_{p_sp_id}`, named appropriately
- **Calculates:**
    - Count of records in sightings (named `rl_{p_sp_id}.sightings`) as `n_data_points`
    - Stratifies count: by `taxon_ir_r`, `class`, `br_class` within `processed_hulls`
    - By region (using `public.region_states_simple` and `public.abi_simple`)
    - By time periods:
        - 2020-2024
        - 2015-2020
        - 2010-2015
        - 1999-2010
        - 1990-1999
        - Pre-1999

- **Result:**
    - Table in `rl_{p_sp_id}` schema, periods as rows (descending), region (descriptions) as columns

### 3. **Example Output Table Structure**
The procedure creates (per species) something like:
**Table:** `rl_1234.r_data_summary`

| period | North Region | Central Region | South Region |
| --- | --- | --- | --- |
| 2020-2024 | 10 | 7 | 3 |
| 2015-2020 | 12 | 9 | 0 |
| ... | ... | ... | ... |
| pre-1999 | 2 | 1 | 0 |
## **Key Points/Notes**
- You can extend the `SELECT` subqueries per cell to include further stratification by `taxon_ir_r`, `class`, `br_class`, or regions from `public.abi_simple` by modifying the above loop(s).
- The query assumes a `date` and `region` field on the `sightings` table in each `rl_{p_sp_id}` schema.
- If you want all regions or abis as columns, repeat the `FOR region IN ...` logic for them.
- The summary table can be enhanced to contain other count splits as needed.

If you want the procedure to summarize other fields or want to handle sightings stratification (e.g. class, br_class), let me know the full expected output schema and details, and I'll adapt the dynamic SQL accordingly!

