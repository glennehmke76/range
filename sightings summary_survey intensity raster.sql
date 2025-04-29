-- sightings summary
  SELECT
    Count(sighting.id) AS num_sightings,
    -- Count(Distinct survey.db_source_id) AS db_num_sources,
    Count(Distinct survey.program_id) AS num_programs,
    Count(Distinct survey.source_id) AS num_sources,
    Count(Distinct(EXTRACT(YEAR FROM survey.start_date))) AS num_years,
    Min(EXTRACT(YEAR FROM survey.start_date)) AS min_year,
    Max(EXTRACT(YEAR FROM survey.start_date)) AS max_year
  FROM
    survey
    INNER JOIN survey_point ON survey.survey_point_id = survey_point.id
    LEFT JOIN sighting ON survey.id = sighting.survey_id
    -- if other covariates wanted
      -- INNER JOIN source ON survey.source_id = source.id
      -- INNER JOIN survey_type ON survey.survey_type_id = survey_type.id
      -- INNER JOIN breeding_activity ON sighting.breeding_activity_id = breeding_activity.id
    INNER JOIN species ON sighting.species_id = species.id
  WHERE
    species.id = 411
  ;

-- make surveyintensity kde
  -- all below time out
  SELECT
    survey.geom_galcc
  FROM
    survey,
    region_aus
  WHERE
    ST_DWithin(survey.geom_galcc,ST_Transform(region_aus.geom,3112), 10000)
  ;

-- or count then weight
  SELECT
    survey.geom_galcc
  FROM
    survey
  JOIN region_aus ON ST_DWithin(survey.geom_galcc,ST_Transform(region_aus.geom,3112), 10000)
  ;

  SELECT
    survey.geom_galcc
  FROM
    survey
  JOIN region_aus ON ST_DWithin(survey.geom_galcc,region_aus.geom_galcc, 10000)
  ;

-- Successfully run. Total query runtime: 2 min 31 secs.
  -- 2317102 rows affected.
  SELECT
    survey.geom_galcc
  FROM
    survey
  JOIN region_aus ON ST_Intersects (survey.geom_galcc, region_aus.geom_galcc)
  ;

  -- > 20 mins
  SELECT
    survey.geom_galcc
  FROM
    survey
  JOIN region_aus ON ST_Intersects (survey.geom_galcc,
                          ST_Buffer(region_aus.geom_galcc,10000, 'quad_segs=2'))
  ;

  -- ST_Buffer region_aus.geom_galcc then ST_Contains/Intersects?

  conclusion = stupidly slow cf select by in QGIS / python?

  10k
  5k

processing.run("qgis:heatmapkerneldensityestimation", {'INPUT':'postgres://dbname=\'birdata\' host=localhost port=5432 user=\'gehmke\' password=\'pf2x5n\' sslmode=disable key=\'_uid_\' checkPrimaryKeyUnicity=\'1\' table="(SELECT row_number() over () AS _uid_,* FROM (SELECT   survey.geom_galcc FROM   survey JOIN region_aus ON ST_Intersects (survey.geom_galcc, region_aus.geom_galcc) \n) AS _subq_1_\n)" (geom_galcc)','RADIUS':25000,'RADIUS_FIELD':'','PIXEL_SIZE':1000,'WEIGHT_FIELD':'','KERNEL':0,'DECAY':0,'OUTPUT_VALUE':0,'OUTPUT':'/Users/glennehmke/MEGAsync/RangeLayers/survey_intensity_25k.tif'})
