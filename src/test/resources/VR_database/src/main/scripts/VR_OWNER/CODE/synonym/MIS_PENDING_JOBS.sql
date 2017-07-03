--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:SYNONYM_MIS_PENDING_JOBS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE SYNONYM "${vr_owner_user}"."MIS_PENDING_JOBS" FOR "${mis_owner_user_synonym}"."MIS_PENDING_JOBS"
