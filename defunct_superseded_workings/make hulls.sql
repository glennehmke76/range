-- make sMCP hulls
-- if you don't use HAVING distinct geom > 2 returns error due to poly and point geom generation

-- sMCP_basin_hull
--  sp_id 225 = 57sec
  SELECT
    -- sub.species_id,
    ST_Transform
      (ST_SetSRID
        (sub.hull,4283),3112) as conv_hull
  FROM
    -- (SELECT
    --   -- sub.species_id,
    --   ST_Union etc(sub.hull) AS hull
    -- FROM
      (SELECT
        -- sighting.species_id,
        region_basins.id AS region_id, -- or substitute other regionalisation
        ST_AsText
          (ST_ConvexHull -- or ST_ConcaveHull
            (ST_Collect
              (survey.geom))) AS hull
      FROM
          survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN region_basins ON ST_Intersects(survey.geom, region_basins.geom)
          -- if using subspecies
          -- JOIN range ON ST_Intersects(survey.geom,range.geom)
          -- JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
        WHERE
          sighting.species_id = 225
          -- if using subspecies
          -- wlab_range.taxon_id = 'u728b'
        GROUP BY
          -- sighting.species_id,
          region_basins.id
        -- exclude point geoms from coming through
        HAVING
          Count(DISTINCT survey.geom) >2
        )sub
      -- )sub2
  ;

-- sMCP_ibra_hull
 SELECT
    -- sub.species_id,
    ST_Transform
      (ST_SetSRID
        (sub.hull,4283),3112) as conv_hull
  FROM
    -- (SELECT
    --   -- sub.species_id,
    --   ST_Union etc(sub.hull) AS hull
    -- FROM
      (SELECT
        -- sighting.species_id,
        region_sibra.id AS region_id, -- or substitute other regionalisation
        ST_AsText
          (ST_ConvexHull -- or ST_ConcaveHull
            (ST_Collect
              (survey.geom))) AS hull
      FROM
          survey
          JOIN sighting ON survey.id = sighting.survey_id
          JOIN region_sibra ON ST_Intersects(survey.geom, region_sibra.geom)
          -- if using subspecies
          -- JOIN range ON ST_Intersects(survey.geom,range.geom)
          -- JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
        WHERE
          sighting.species_id = 225
          -- or use a ssp thats becoming a sp
          -- wlab_range.taxon_id = 'u728b'
        GROUP BY
          -- sighting.species_id,
          region_sibra.id
        -- exclude point geoms from coming through
        HAVING
          Count(DISTINCT survey.geom) >2
        )sub
      -- )sub2
  ;

-- can we join points based on min of ST_Distance or some other function?
  -- convert polys to points preserving regional attributes
  -- attribute Min of ST_Distance to other region then re-do MCP?






-- make IUCN MCP hull
  -- need to run DWithin instead of Intersects because hull vertices dont always include the points to which they are snapped dyring digitisation
  -- current outputs in GALCC - add ST_Transform after ST_SetSRID for GDA
  SELECT
    ST_SetSRID
      (ST_AsText
        (ST_ConvexHull
          (ST_Collect
            (sub.geom))),3112) AS hull
  FROM
      (SELECT
        survey.geom_galcc AS geom
      FROM survey
      JOIN sighting ON survey.id = sighting.survey_id
      JOIN range ON ST_DWithin(ST_Transform(range.geom,3112),survey.geom_galcc,100)
      WHERE
        range.sp_id = 225
        AND sighting.species_id = 225
      )sub
  ;

-- clipped range
  SELECT
    ST_Intersection(range.geom,region_aus.geom)
  FROM range, region_aus
  WHERE
    range.sp_id = 225
  ;

-- concave_hull
  SELECT
    -- sighting.species_id,
    ST_SetSRID
      (ST_AsText
        (ST_ConcaveHull
          (ST_Collect
            (survey.geom_galcc),0.8)),3112) AS hull
  FROM
    survey
    JOIN sighting ON survey.id = sighting.survey_id
  WHERE
    sighting.species_id = 411
    AND ST_DWithin(survey.geom_galcc,
          (SELECT
            ST_Transform(range.geom,3112)
          FROM range
          WHERE
            sp_id = 411)
        ,10000)s
  ;
