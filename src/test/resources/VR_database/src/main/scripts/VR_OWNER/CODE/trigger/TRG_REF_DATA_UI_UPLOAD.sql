--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_REF_DATA_UI_UPLOAD runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_REF_DATA_UI_UPLOAD
    BEFORE INSERT ON REF_DATA_UI_UPLOAD 
    FOR EACH ROW 
BEGIN
IF :new.uploaded_on is null 
THEN :new.uploaded_on := sysdate;
END IF;
END;
