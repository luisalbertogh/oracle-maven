--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_LOG_JOBS_BEF_I runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_LOG_JOBS_BEF_I 
    BEFORE INSERT ON LOG_JOBS 
    FOR EACH ROW 
BEGIN
  :new.JOB_SK := LOG_JOBS_SEQ.nextval;
  :new.JOB_timestamp  := systimestamp;
END TRG_LOG_JOBS_BEF_I;
