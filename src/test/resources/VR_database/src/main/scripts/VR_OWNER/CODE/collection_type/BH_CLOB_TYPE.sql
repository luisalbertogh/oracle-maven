--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TYPE_BH_CLOB_TYPE runOnChange:TRUE runAlways:TRUE failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE TYPE "BH_CLOB_TYPE" AS TABLE OF clob;
