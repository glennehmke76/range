DROP TABLE IF EXISTS atlas;
CREATE TABLE atlas AS
SELECT
  sp_id,
  ST_Envelope
    (ST_Collect
      (COALESCE
        (geom)))
    AS geom_bounding_box
FROM range
GROUP BY
  sp_id
;
alter table atlas
    add constraint atlas_pk
        primary key (sp_id);



-- QGIS filters for symbology
on = "route_id" = attribute( @atlas_feature ,  'route_id')

off = "route_id" <> attribute( @atlas_feature,  'route_id')

-- for labels
"route_id" = attribute( @atlas_feature ,  'route_id') AND surv_year = xxxx


sudo -u glennehmke pg_dump -t sites -t sites_integrated -t source -t source_sharing -t species -t subspecies -t supertaxon -t supertaxon_species -t survey_type -t vetting_classification -t vetting_review -t vetting_status -t wlab -t wlab_covariates -t wlab_range -t wlab_v2 birdata | psql -h birdlife.webgis1.com -p 5432 -U birdlife -d birdlife_birdata
