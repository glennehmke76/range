DROP PROCEDURE IF EXISTS r_import_ala;
CREATE OR REPLACE PROCEDURE r_import_ala(
    p_sp_id integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema_name TEXT;
    v_row_count INTEGER;
    v_deleted_count INTEGER;
    v_column_list TEXT;
    v_sql TEXT;
    v_error_message TEXT;
    v_error_state TEXT;
    v_file_path TEXT;
BEGIN
    -- Input validation
    IF p_sp_id IS NULL THEN
        RAISE EXCEPTION 'Species ID cannot be null';
    END IF;

    -- Set schema name and file path
    v_schema_name := 'rl_' || p_sp_id;
    v_file_path := '/Users/glennehmke/Data/ala/ala_' || p_sp_id || '.csv';

    -- Check if schema exists, if not create it
    PERFORM schema_name
    FROM information_schema.schemata
    WHERE schema_name = v_schema_name;

    IF NOT FOUND THEN
        EXECUTE 'CREATE SCHEMA ' || v_schema_name;
        RAISE NOTICE 'Schema % created', v_schema_name;
    END IF;

    -- Create temporary table for data import
    EXECUTE 'DROP TABLE IF EXISTS temp_ala_import';
    EXECUTE '
        CREATE TEMPORARY TABLE temp_ala_import (
            dataResourceUid                     text,
            images                              text,
            "dcterms:modified"                  text,
            "dcterms:language"                  text,
            "dcterms:license"                   text,
            rightsHolder                        text,
            "dcterms:accessRights"              text,
            "dcterms:bibliographicCitation"     text,
            "references"                        text,
            institutionID                       text,
            collectionID                        text,
            datasetID                           text,
            institutionCode                     text,
            collectionCode                      text,
            datasetName                         text,
            ownerInstitutionCode                text,
            basisOfRecord                       text,
            informationWithheld                 text,
            dataGeneralizations                 text,
            dynamicProperties                   text,
            occurrenceID                        text,
            catalogNumber                       text,
            recordNumber                        text,
            recordedBy                          text,
            individualCount                     text,
            organismQuantity                    text,
            organismQuantityType                text,
            sex                                 text,
            lifeStage                           text,
            reproductiveCondition               text,
            behavior                            text,
            establishmentMeans                  text,
            occurrenceStatus                    text,
            preparations                        text,
            disposition                         text,
            associatedMedia                     text,
            associatedReferences                text,
            associatedSequences                 text,
            associatedTaxa                      text,
            otherCatalogNumbers                 text,
            occurrenceRemarks                   text,
            organismID                          text,
            organismName                        text,
            organismScope                       text,
            associatedOccurrences               text,
            associatedOrganisms                 text,
            previousIdentifications             text,
            organismRemarks                     text,
            materialSampleID                    text,
            eventID                             text,
            parentEventID                       text,
            fieldNumber                         text,
            eventDate                           text,
            eventTime                           text,
            startDayOfYear                      text,
            endDayOfYear                        text,
            year                                text,
            month                               text,
            day                                 text,
            verbatimEventDate                   text,
            habitat                             text,
            samplingProtocol                    text,
            samplingEffort                      text,
            sampleSizeValue                     text,
            sampleSizeUnit                      text,
            fieldNotes                          text,
            eventRemarks                        text,
            locationID                          text,
            higherGeographyID                   text,
            higherGeography                     text,
            continent                           text,
            waterBody                           text,
            islandGroup                         text,
            island                              text,
            country                             text,
            countryCode                         text,
            stateProvince                       text,
            county                              text,
            municipality                        text,
            locality                            text,
            verbatimLocality                    text,
            minimumElevationInMeters            text,
            maximumElevationInMeters            text,
            verbatimElevation                   text,
            minimumDepthInMeters                text,
            maximumDepthInMeters                text,
            verbatimDepth                       text,
            minimumDistanceAboveSurfaceInMeters text,
            maximumDistanceAboveSurfaceInMeters text,
            locationAccordingTo                 text,
            locationRemarks                     text,
            decimalLatitude                     text,
            decimalLongitude                    text,
            geodeticDatum                       text,
            coordinateUncertaintyInMeters       text,
            coordinatePrecision                 text,
            pointRadiusSpatialFit               text,
            verbatimCoordinates                 text,
            verbatimLatitude                    text,
            verbatimLongitude                   text,
            verbatimCoordinateSystem            text,
            verbatimSRS                         text,
            footprintWKT                        text,
            footprintSRS                        text,
            footprintSpatialFit                 text,
            georeferencedBy                     text,
            georeferencedDate                   text,
            georeferenceProtocol                text,
            georeferenceSources                 text,
            georeferenceVerificationStatus      text,
            georeferenceRemarks                 text,
            geologicalContextID                 text,
            earliestEonOrLowestEonothem         text,
            latestEonOrHighestEonothem          text,
            earliestEraOrLowestErathem          text,
            latestEraOrHighestErathem           text,
            earliestPeriodOrLowestSystem        text,
            latestPeriodOrHighestSystem         text,
            earliestEpochOrLowestSeries         text,
            latestEpochOrHighestSeries          text,
            earliestAgeOrLowestStage            text,
            latestAgeOrHighestStage             text,
            lowestBiostratigraphicZone          text,
            highestBiostratigraphicZone         text,
            lithostratigraphicTerms             text,
            "group"                             text,
            formation                           text,
            member                              text,
            bed                                 text,
            identificationID                    text,
            identificationQualifier             text,
            typeStatus                          text,
            identifiedBy                        text,
            dateIdentified                      text,
            identificationReferences            text,
            identificationVerificationStatus    text,
            identificationRemarks               text,
            taxonID                             text,
            scientificNameID                    text,
            acceptedNameUsageID                 text,
            parentNameUsageID                   text,
            originalNameUsageID                 text,
            nameAccordingToID                   text,
            namePublishedInID                   text,
            taxonConceptID                      text,
            raw_scientificName                  text,
            scientificName                      text,
            acceptedNameUsage                   text,
            parentNameUsage                     text,
            originalNameUsage                   text,
            nameAccordingTo                     text,
            namePublishedIn                     text,
            namePublishedInYear                 text,
            higherClassification                text,
            kingdom                             text,
            phylum                              text,
            class                               text,
            "order"                             text,
            family                              text,
            genus                               text,
            subgenus                            text,
            specificEpithet                     text,
            infraspecificEpithet                text,
            taxonRank                           text,
            verbatimTaxonRank                   text,
            scientificNameAuthorship            text,
            vernacularName                      text,
            nomenclaturalCode                   text,
            taxonomicStatus                     text,
            nomenclaturalStatus                 text,
            taxonRemarks                        text,
            measurementDeterminedDate           text,
            recordedByID                        text,
            identifiedByID                      text,
            videos                              text,
            species                             text,
            relationshipAccordingTo             text,
            subfamily                           text,
            resourceRelationshipID              text,
            eventType                           text,
            measurementRemarks                  text,
            measurementValue                    text,
            relationshipRemarks                 text,
            resourceID                          text,
            relationshipEstablishedDate         text,
            superfamily                         text,
            identifierRole                      text,
            relationshipOfResource              text,
            measurementMethod                   text,
            identifier                          text,
            provenance                          text,
            sounds                              text,
            recordID                            text,
            rights                              text,
            source                              text,
            measurementID                       text,
            measurementType                     text,
            measurementUnit                     text,
            measurementDeterminedBy             text,
            measurementAccuracy                 text,
            degreeOfEstablishment               text,
            relatedResourceID                   text,
            images_2                            text,
            "dcterms:type"                      text
        )';

    -- Import CSV data into temporary table
    BEGIN
        EXECUTE format('
            COPY temp_ala_import FROM %L WITH (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER '',''
            )', v_file_path);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_state = RETURNED_SQLSTATE;
            RAISE EXCEPTION 'Error importing CSV: % (SQLSTATE: %)', v_error_message, v_error_state;
    END;

    -- Get count of imported rows
    EXECUTE 'SELECT COUNT(*) FROM temp_ala_import' INTO v_row_count;
    RAISE NOTICE 'Imported % rows from CSV file', v_row_count;

    -- Delete records with null or empty year
    EXECUTE '
        DELETE FROM temp_ala_import
        WHERE year IS NULL OR year = ''''
    ';

    -- Delete records where recordedBy contains 'Birds Australia'
    EXECUTE '
        DELETE FROM temp_ala_import
        WHERE "recordedby" LIKE ''%Birds Australia%''
    ';

    -- Get count of rows after deletion
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % rows', v_deleted_count;

    -- Create the sightings table if it doesn't exist
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.sightings (
            id SERIAL PRIMARY KEY,
            sp_id INTEGER DEFAULT %s,
            data_source INTEGER,
            source_ref TEXT,
            count INTEGER,
            start_date DATE,
            year INTEGER,
            precision NUMERIC,
            geom GEOMETRY(POINT, 4283)
        )', v_schema_name, p_sp_id);

    -- Insert data from temp table to sightings table with proper transformations
    EXECUTE format('
        INSERT INTO %I.sightings (
            sp_id,
            data_source,
            source_ref,
            count,
            start_date,
            year,
            precision,
            geom
        )
        SELECT
            %s AS sp_id,
            3 AS data_source,
            "occurrenceid" AS source_ref,
            NULLIF("individualcount", '''')::INTEGER AS count,
            NULLIF("eventdate", '''')::DATE AS start_date,
            NULLIF("year", '''')::INTEGER AS year,
            NULLIF("coordinateuncertaintyinmeters", '''')::NUMERIC AS precision,
            ST_Transform(
                ST_SetSRID(
                    ST_MakePoint(
                        NULLIF("decimallongitude", '''')::NUMERIC,
                        NULLIF("decimallatitude", '''')::NUMERIC
                    ),
                    4326
                ),
                4283
            ) AS geom
        FROM
            temp_ala_import
        WHERE
            "decimallongitude" IS NOT NULL AND "decimallongitude" <> ''''
            AND "decimallatitude" IS NOT NULL AND "decimallatitude" <> ''''
    ', v_schema_name, p_sp_id);

    -- Get count of inserted rows
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % rows into %.sightings table', v_row_count, v_schema_name;

    -- Create spatial index on the geometry column if it doesn't exist
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_%I_sightings_geom ON %I.sightings USING GIST(geom)
    ', v_schema_name, v_schema_name);

    -- Clean up
    EXECUTE 'DROP TABLE IF EXISTS temp_ala_import';

    RAISE NOTICE 'Successfully imported ALA data for species ID: %', p_sp_id;
END;
$$;

CALL r_import_ala(402); -- where 123 is the species ID