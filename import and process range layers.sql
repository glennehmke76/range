  -- range layers
  -- good, clean layers initially
    -- merge range layers
    -- check validity
    -- create table as QGIS import
    processing.run("native:importintopostgis", {'INPUT':'/Volumes/Data/range_merge.gpkg','DATABASE':'birdata','SCHEMA':'public','TABLENAME':'range','PRIMARY_KEY':'id','GEOMETRY_COLUMN':'geom','ENCODING':'UTF-8','OVERWRITE':False,'CREATEINDEX':False,'LOWERCASE_NAMES':True,'DROP_STRING_LENGTH':True,'FORCE_SINGLEPART':False})

    -- process imported table
    ALTER TABLE range
    DROP COLUMN fid,
    DROP COLUMN layer,
    DROP COLUMN path;
    ALTER TABLE IF EXISTS public.range DROP COLUMN IF EXISTS objectid;
    ALTER TABLE IF EXISTS public.range DROP COLUMN IF EXISTS shape_leng;
    ALTER TABLE IF EXISTS public.range DROP COLUMN IF EXISTS shape_area;
    ALTER TABLE range
    RENAME COLUMN taxonid TO taxon_id_r;
    ALTER TABLE range
    RENAME COLUMN spno TO sp_id;
    ALTER TABLE range
    RENAME COLUMN brrnge TO br_rnge;
    CREATE INDEX idx_range_geom_GDA94 ON range USING gist (geom);
    CREATE INDEX idx_range_geom_GALCC ON range USING gist (ST_transform(geom, 3112));
    CREATE INDEX IF NOT EXISTS idx_range_taxon_id
    ON range (taxon_id_r);

    -- make sp_id int
    ALTER TABLE range
    ALTER TABLE range
      RENAME COLUMN sp_id TO sp_id_t;

    ALTER TABLE IF EXISTS public.range
        ADD COLUMN sp_id integer;
    CREATE INDEX IF NOT EXISTS idx_range_sp_id
    ON range (sp_id);

    UPDATE range
    SET
      sp_id = (sp_id_t::integer);

    ALTER TABLE IF EXISTS public.range DROP COLUMN IF EXISTS sp_id_t;

    -- make rnge/br_rnge int
    ALTER TABLE range
      RENAME COLUMN rnge TO rnge_t;
    ALTER TABLE range
      RENAME COLUMN br_rnge TO br_rnge_t;

    ALTER TABLE IF EXISTS public.range
        ADD COLUMN rnge integer,
        ADD COLUMN br_rnge integer;

    UPDATE range
    SET
      rnge = (rnge_t::integer),
      br_rnge = (br_rnge_t::integer);

    ALTER TABLE IF EXISTS public.range DROP COLUMN IF EXISTS rnge_t;
    ALTER TABLE IF EXISTS public.range DROP COLUMN IF EXISTS br_rnge_t;

    UPDATE range
    SET rnge = 0
    WHERE rnge IS NULL;
    UPDATE range
    SET br_rnge = 0
    WHERE br_rnge IS NULL;

    -- return invalid geometries
    ALTER TABLE IF EXISTS public.range
        ADD COLUMN geom_valid boolean;
    ALTER TABLE IF EXISTS public.range
        ADD COLUMN geom_invalid_detail character varying;

    UPDATE range
    SET
      geom_valid = sub1.ST_IsValid,
      geom_invalid_detail = sub1.ST_IsValidDetail
    FROM
      (SELECT id, ST_IsValid (geom), ST_IsValidDetail (geom)
      FROM range
      ) sub1
    WHERE range.id = sub1.id;

    -- return number of vertices
      -- 2,368 rows affected in 2 m 28 s 165 ms
    ALTER TABLE IF EXISTS public.range
        ADD COLUMN num_vertices integer;
    UPDATE range
    SET
      num_vertices = sub1.num_vertices
    FROM
    (SELECT
       id,
       SUM(ST_NPoints(geom)) AS num_vertices
    FROM range
    GROUP BY
      id
    ) sub1
    WHERE range.id = sub1.id;

  -- do better to do num vertices as a % or area? INCOMPLETE
    SELECT
      sp_id,
      percentile_disc(0.05) WITHIN GROUP (ORDER BY range.num_vertices)
    FROM
      (SELECT
        sp_id,
--         percentile_disc(0.05) WITHIN GROUP (ORDER BY range.num_vertices)
        SUM(num_vertices) AS num_vertices
      FROM range
      GROUP BY
        sp_id
      ORDER BY
        num_vertices DESC
      LIMIT 10
      )range
    GROUP BY
      sp_id
    ;

-- import range lookup tables
DROP TABLE IF EXISTS lut_rnge;
CREATE TABLE lut_rnge (
  rnge int NOT NULL,
  description varchar DEFAULT NULL,
  notes varchar DEFAULT NULL,
  PRIMARY KEY (rnge)
);
copy lut_rnge FROM '/Users/glennehmke/MEGAsync/RangeLayers/lut_rnge.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS lut_br_rnge;
CREATE TABLE lut_br_rnge (
  br_rnge int NOT NULL,
  description varchar DEFAULT NULL,
  PRIMARY KEY (br_rnge)
);
copy lut_br_rnge FROM '/Users/glennehmke/MEGAsync/RangeLayers/lut_brRnge.csv' DELIMITER ',' CSV HEADER;

-- checks
    -- should be no blank sp_id rows
    SELECT * FROM range WHERE sp_id IS NULL

    -- check sp_id component of taxon_id matches sp_id
    ALTER TABLE IF EXISTS range
        ADD COLUMN taxon_id_real character varying;

    -- does range layer exist

      -- join wlab to wlab range - do we have range layers for taxa



      --   sub-query wlab_range to range
      -- SELECT DISTINCT
      --   wlab.taxon_sort,
      --   sub.taxon_id AS wlab_taxon_id,
      --   wlab.taxon_name,
      --   wlab.taxon_scientific_name,
      --   wlab.population,
      --   wlab_range.taxon_id AS range_taxon_id
      -- FROM
      --   (SELECT DISTINCT
      --     wlab_range.taxon_id
      --   FROM range
      --   LEFT JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r

      --   )

-- return non-matching records from 2 tables

or update into wlab or wlab clone with 2 directionl joins into n new fields

SELECT DISTINCT
  wlab_range.sp_id AS wlab_range_sp_id,
  wlab_range.taxon_id_r AS wlab_range_taxon_id_r,
  range.sp_id AS range_sp_id,
  range.taxon_id_r AS range_taxon_id_r
FROM range
FULL OUTER JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
WHERE
  wlab_range.taxon_id_r IS NULL
  OR range.taxon_id_r  IS NULL






      -- add is_hybrid
        WHERE taxon_id LIKE '%.%'



      UPDATE range
      SET taxon_id_real = taxon_id;

      UPDATE range
      SET taxon_id_real = REPLACE (taxon_id_real,	'a',	'');


-- duplicte table as record of pre-changes
  CREATE TABLE range_old AS
  (SELECT * FROM range);

  -- get below using
    SELECT indexdef FROM pg_indexes WHERE tablename='range';

  CREATE UNIQUE INDEX range_old_pkey1 ON public.range_old USING btree (id);
  CREATE INDEX idx_range_old_geom_gda94 ON public.range_old USING gist (geom);
  CREATE INDEX idx_range_old_taxon_id ON public.range_old USING btree (taxon_id_r);
  CREATE INDEX IF NOT EXISTS idx_range_old_sp_id ON range_old (sp_id);

-- add a range layer
  -- merge layers if needed
  -- import to pgSQL
  processing.run("native:importintopostgis", {'INPUT':'/Volumes/Data/range_merge.gpkg','DATABASE':'birdata','SCHEMA':'public','TABLENAME':'range_addition','GEOMETRY_COLUMN':'geom','ENCODING':'UTF-8','OVERWRITE':False,'CREATEINDEX':False,'LOWERCASE_NAMES':True,'DROP_STRING_LENGTH':True,'FORCE_SINGLEPART':False})

    -- process imported table
    ALTER TABLE IF EXISTS range_addition DROP CONSTRAINT IF EXISTS range_addition_pkey;
    ALTER TABLE IF EXISTS range_addition DROP COLUMN IF EXISTS fid;
    ALTER TABLE IF EXISTS range_addition DROP COLUMN IF EXISTS objectid;
    ALTER TABLE IF EXISTS range_addition DROP COLUMN IF EXISTS shape_leng;
    ALTER TABLE IF EXISTS range_addition DROP COLUMN IF EXISTS shape_area;
    ALTER TABLE IF EXISTS range_addition DROP COLUMN IF EXISTS layer;
    ALTER TABLE IF EXISTS range_addition DROP COLUMN IF EXISTS path;

    ALTER TABLE range_addition
    RENAME COLUMN taxonid TO taxon_id_r;
    ALTER TABLE range_addition
    RENAME COLUMN spno TO sp_id;
    ALTER TABLE range_addition
    RENAME COLUMN brrnge TO br_rnge;

    -- make sp_id int
    ALTER TABLE range_addition
      RENAME COLUMN sp_id TO sp_id_t;

    ALTER TABLE IF EXISTS public.range_addition
        ADD COLUMN sp_id integer;

    UPDATE range_addition
    SET
      sp_id = (sp_id_t::integer);

    ALTER TABLE IF EXISTS public.range_addition DROP COLUMN IF EXISTS sp_id_t;

    -- make rnge/br_rnge int
    ALTER TABLE range_addition
      RENAME COLUMN rnge TO rnge_t;
    ALTER TABLE range_addition
      RENAME COLUMN br_rnge TO br_rnge_t;

    ALTER TABLE IF EXISTS public.range_addition
        ADD COLUMN rnge integer,
        ADD COLUMN br_rnge integer;

    UPDATE range_addition
    SET
      rnge = (rnge_t::integer),
      br_rnge = (br_rnge_t::integer);

    ALTER TABLE IF EXISTS public.range_addition DROP COLUMN IF EXISTS rnge_t;
    ALTER TABLE IF EXISTS public.range_addition DROP COLUMN IF EXISTS br_rnge_t;

    -- update primary table
    INSERT INTO range (geom, taxon_id_r, sp_id, rnge, br_rnge)
    SELECT
      geom,
      taxon_id_r,
      sp_id,
      rnge,
      br_rnge
    FROM range_addition;
    DROP TABLE IF EXISTS range_addition;


-- add alpha hulls (old versions NOT CLEAN)
  -- process imported table

  ALTER TABLE range_alpha
  RENAME COLUMN taxonid TO taxon_id_r;
  ALTER TABLE range_alpha
  RENAME COLUMN spno TO sp_id;
  ALTER TABLE range_alpha
  RENAME COLUMN brrnge TO br_rnge;
  CREATE INDEX IF NOT EXISTS idx_range_alpha_taxon_id
  ON range_alpha (taxon_id_r);

  -- return invalid geometries
  ALTER TABLE IF EXISTS public.range_alpha
      ADD COLUMN geom_valid boolean;
  ALTER TABLE IF EXISTS public.range_alpha
      ADD COLUMN geom_invalid_detail character varying;

  UPDATE range_alpha
  SET
    geom_valid = sub1.ST_IsValid,
    geom_invalid_detail = sub1.ST_IsValidDetail
  FROM
  (SELECT id, ST_IsValid (geom), ST_IsValidDetail (geom)
  FROM range_alpha
  ) sub1
  WHERE range_alpha.id = sub1.id;

