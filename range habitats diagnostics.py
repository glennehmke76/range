# example broad terrestrial bird habitats in range and by sightings

processing.run("native:reprojectlayer", {'INPUT':'postgres://dbname=\'birdata\' host=localhost port=5432 user=\'gehmke\' password=\'pf2x5n\' sslmode=disable key=\'_uid_\' checkPrimaryKeyUnicity=\'1\' table="(SELECT row_number() over () AS _uid_,* FROM (SELECT   ST_Intersection(range.geom,region_aus.geom) FROM range, region_aus WHERE   range.sp_id = 411 \n) AS _subq_1_\n)" (st_intersection)','TARGET_CRS':QgsCoordinateReferenceSystem('EPSG:3112'),'OPERATION':'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=lcc +lat_0=0 +lon_0=134 +lat_1=-18 +lat_2=-36 +x_0=0 +y_0=0 +ellps=GRS80','OUTPUT':'TEMPORARY_OUTPUT'})

processing.run("native:zonalhistogram", {'INPUT_RASTER':'/Volumes/Backup4tb/HabitatRasters/BirdHabitats_mosiac.tif','RASTER_BAND':1,'INPUT_VECTOR':'memory://MultiPolygon?crs=EPSG:3112&field=_uid_:long(-1,0)&uid={8b9fa348-a0ae-4d43-94ea-c8ddfb94fc4e}','COLUMN_PREFIX':'hab_','OUTPUT':'TEMPORARY_OUTPUT'})


Execution completed in 1109.29 seconds (18 minutes 29 seconds)
processing.run("native:zonalhistogram", {'INPUT_RASTER':'/Volumes/Backup4tb/HabitatRasters/BirdHabitats_mosiac.tif','RASTER_BAND':1,'INPUT_VECTOR':'postgres://dbname=\'birdata\' host=localhost port=5432 user=\'gehmke\' password=\'pf2x5n\' sslmode=disable key=\'id\' srid=4283 type=MultiPolygon checkPrimaryKeyUnicity=\'1\' table="public"."range" (geom) sql="sp_id" = 411','COLUMN_PREFIX':'hab_','OUTPUT':'TEMPORARY_OUTPUT'})



#  for points as majority in pseudo-AOO
    # for points
    SELECT DISTINCT
        survey.id,
        ST_Transform(survey_point.geom, 3112)
    FROM
        survey
        JOIN sighting ON survey.id = sighting.survey_id
        JOIN survey_point ON survey.survey_point_id = survey_point.id
    WHERE
        sighting.species_id = 411
        AND EXTRACT(YEAR FROM survey.start_date) >=1990
        AND survey_point.accuracy_in_metres < 2000
    ;

    # for 4km2 buffers
    SELECT DISTINCT
        survey.id,
        ST_Buffer(ST_Transform(survey_point.geom, 3112), 1129)
    FROM
        survey
        JOIN sighting ON survey.id = sighting.survey_id
        JOIN survey_point ON survey.survey_point_id = survey_point.id
    WHERE
        sighting.species_id = 411
        AND EXTRACT(YEAR FROM survey.start_date) >=1990
        AND survey_point.accuracy_in_metres < 2000
    ;

processing.run("native:zonalhistogram", {'INPUT_RASTER':'/Volumes/Backup4tb/HabitatRasters/BirdHabitats_mosiac.tif','RASTER_BAND':1,'INPUT_VECTOR':'postgres://dbname=\'birdata\' host=localhost port=5432 user=\'gehmke\' password=\'pf2x5n\' sslmode=disable key=\'_uid_\' checkPrimaryKeyUnicity=\'1\' table="(SELECT row_number() over () AS _uid_,* FROM (SELECT DISTINCT     survey.id,     ST_Buffer(ST_Transform(survey_point.geom, 3112), 1129) FROM     survey     JOIN sighting ON survey.id = sighting.survey_id     JOIN survey_point ON survey.survey_point_id = survey_point.id WHERE     sighting.species_id = 411     AND EXTRACT(YEAR FROM survey.start_date) >=1990     AND survey_point.accuracy_in_metres < 2000 \n) AS _subq_1_\n)" (st_buffer)','COLUMN_PREFIX':'hab_','OUTPUT':'TEMPORARY_OUTPUT'})

