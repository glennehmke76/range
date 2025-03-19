


-- Query returned successfully in 11 min 10 secs.
  DROP TABLE IF EXISTS sighting_old;
  CREATE TABLE sighting_old (LIKE sighting INCLUDING ALL);
  INSERT INTO public.sighting_old(
    id, survey_id, species_id, individual_count, breeding_activity_id, vetting_status_id, sensitivity_id, entry_order, time_created, last_modified, birdsurveyspeciesid)
  SELECT 
    *
  FROM sighting
  ;

-- examples
  -- if we make extra polygons with rnge = 10 to capture out-of-range sighting they will also get reclassified

-- example Restless / Paperbark Flycatcher split
  -- for Paperbark Flycatcher
  UPDATE sighting
  SET species_id = 722
  WHERE
    ST_Intersects(survey_point.geom, range.geom)
    AND survey_point.id = survey.survey_point_id
    AND survey.id = sighting.survey_id
    AND sighting.sp_id = 728 -- the old species
    AND range.sp_id = 722 -- the new species
  ;

  -- for Restless Flycatcher
  UPDATE sighting
  SET species_id = 728
  WHERE
    ST_Intersects(survey_point.geom, range.geom)
    AND sighting.sp_id = 728 -- the old species
    AND range.sp_id = 728 -- the new species
  ;

    UPDATE survey
    SET geom = sub1.geom
    FROM
    (SELECT id, geom
    FROM survey_point)sub1
    WHERE survey.survey_point_id = sub1.id;



  -- or compound for species
  UPDATE sighting
  SET species_id = CASE
    WHEN 
      sighting.species_id = 728 -- the old species
      AND range.sp_id = 722 -- the new species
      THEN 722
    WHEN 
      sighting.species_id = 728 -- the old species
      AND range.sp_id = 728 -- the new species
      THEN 728
    END
  FROM survey, survey_point, range
  WHERE
    ST_Intersects(survey_point.geom, range.geom)
    AND survey_point.id = survey.survey_point_id
    AND survey.id = sighting.survey_id
  ;


-- check changes




-- does this need to be individual - can we update globally?
-- perhaps if we have
UPDATE sighting
SET sighting.species_id = range.sp_id
WHERE
  ST_Intersects/Contains(survey_point.geom, range.geom)
  AND range.sp_id = sighting.species_id_old
  AND xxxx
;







-- simple update
  -- update sightings independent of range layer
  UPDATE sighting
  SET species_id = 714 -- Norfiolk Island Robin
  FROM survey, survey_point
  WHERE
    sighting.survey_id = survey.id
    AND survey.id = survey_point.id
    AND ST_X(survey_point.geom) >166 -- longitude is well east of Australian mainland
    AND sighting.species_id = 380 -- the old species
  ;

  -- update range layer independent of sightings
  UPDATE range
  SET -- to new taxon (Norfolk Island Robin)
    sp_id = 714,
    taxon_id_r = 'u714'
  WHERE
    sp_id = 380 -- the old species (Scarlet Robin)
    taxon_id_r = 'u380d' -- the old subspecies that is now a species (Norfolk Island Scarlet Robin)
  ;
  -- Alternatively, the range table can be updated manually through QGIS or other PostgreSQL clients.

  -- in this case the remainder would stay as species_id - 380 and not require change. 

need to disable fk below
ERROR:  insert or update on table 'sighting' violates foreign key constraint 'sighting_ibfk_2'
DETAIL:  Key (species_id)=(714) is not present in table 'species'.
SQL state: 23503

