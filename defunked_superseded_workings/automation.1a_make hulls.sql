-- make base hulls
  -- sMCP_basin_region
  --  sp_id 225 = 57sec
SELECT
  ST_Union
    (ST_SetSRID(hull_sightings.hull,4283)) AS geom
FROM
    (SELECT
      -- sighting.species_id,
      region_ibra.id AS region_id, -- or substitute other regionalisation
      ST_AsText
        (ST_ConvexHull -- or ST_ConcaveHull
          (ST_Collect
            (survey.geom))) AS hull
    FROM
        survey
        JOIN sighting ON survey.id = sighting.survey_id
        JOIN region_ibra ON ST_Intersects(survey.geom, region_ibra.geom)
        -- if using subspecies
        -- JOIN range ON ST_Intersects(survey.geom,range.geom)
        -- JOIN wlab_range ON range.taxon_id_r = wlab_range.taxon_id_r
    WHERE
      sighting.species_id = 621
      -- if using subspecies
      -- wlab_range.taxon_id = 'u728b'
    GROUP BY
      -- sighting.species_id,
      region_ibra.id
    -- exclude point geoms from coming through
    HAVING
      Count(DISTINCT survey.geom) >2
    )hull_sightings
;
