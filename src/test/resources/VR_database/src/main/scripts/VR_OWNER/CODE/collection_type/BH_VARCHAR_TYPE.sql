--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TYPE_BH_VARCHAR_TYPE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE TYPE "BH_VARCHAR_TYPE" AS TABLE OF VARCHAR2(4000);
