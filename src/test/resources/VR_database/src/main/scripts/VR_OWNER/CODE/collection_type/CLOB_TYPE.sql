--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TYPE_CLOB_TYPE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE TYPE "CLOB_TYPE" AS TABLE OF clob;
