--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TYPE_SDATA_AUDIT_HISTORY_TYPE_TABLE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE TYPE "SDATA_AUDIT_HISTORY_TYPE_TABLE" AS TABLE OF SDATA_AUDIT_HISTORY_DATA_TYPE
