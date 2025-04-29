-- range poly summaries
-- need to add taxon_id_r AND range via wlab_range

-- old geom
  SELECT
    Sum(ST_Area
      (ST_Transform
        (sub.geom,3112))
      / 1000000) AS clipped_sqkm
  FROM
    (SELECT
      range_old.sp_id,
      ST_Intersection(range_old.geom,region_aus.geom) AS geom
    FROM range_old, region_aus
    WHERE
      range_old.sp_id = 411
    )sub

  SELECT
    Sum(ST_NumGeometries(range_old.geom)) AS num_polys,
    Sum(ST_NPoints(range_old.geom)) AS num_vertices
  FROM range_old
  WHERE
    range_old.sp_id = 411
  ;

-- new geom
  SELECT
    ST_Area
      (ST_Transform
        (sub.geom,3112))
      / 1000000 AS clipped_sqkm
  FROM
    (SELECT
      range.sp_id,
      ST_Intersection(range.geom,region_aus.geom) AS geom
    FROM range, region_aus
    WHERE
      range.sp_id = 411
    )sub

  SELECT
    ST_NumGeometries(range.geom) AS num_polys,
    ST_NPoints(range.geom) AS num_vertices
  FROM range
  WHERE
    range.sp_id = 411
  ;

-- MCP
  SELECT
    ST_Area
      (ST_Transform
        (sub2.geom,3112))
      / 1000000 AS clipped_sqkm
  FROM
    (SELECT
      ST_SetSRID
        (ST_AsText
          (ST_ConvexHull
            (ST_Collect
              (sub.geom))),3112) AS geom
    FROM
        (SELECT
          survey.geom_galcc AS geom
        FROM survey
        JOIN sighting ON survey.id = sighting.survey_id
        JOIN range ON ST_DWithin(ST_Transform(range.geom,3112),survey.geom_galcc,100)
        WHERE
          range.sp_id = 411
          AND sighting.species_id = 411
        )sub
    )sub2

-- have a clipped range layer-set as a view?
too long
index + add galcc geom + index

  CREATE INDEX idx_region_aus_geom_GDA94 ON region_aus USING gist (geom);

  ALTER TABLE IF EXISTS region_aus
    ADD COLUMN geom_galcc geometry(Multipolygon,3112);
  CREATE INDEX idx_region_aus_geom_galcc ON region_aus USING gist (geom_galcc);

  -- Query returned successfully in 6 min 51 secs.
  UPDATE region_aus
  SET geom_galcc = ST_Transform(geom, 3112);


-- alpha layers temp
  SELECT
    ST_Area(411_alpha.geom) / 1000000 AS clipped_sqkm
  FROM 411_alpha;
