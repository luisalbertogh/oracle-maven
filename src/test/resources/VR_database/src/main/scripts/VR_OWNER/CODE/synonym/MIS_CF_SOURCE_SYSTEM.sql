--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:SYNONYM_MIS_CF_SOURCE_SYSTEM runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE SYNONYM "${vr_owner_user}"."MIS_CF_SOURCE_SYSTEM" FOR "${mis_owner_user_synonym}"."MIS_CF_SOURCE_SYSTEM"
