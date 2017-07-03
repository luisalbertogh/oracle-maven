--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_STAGING runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_STAGING" as

function f_prepare_aging_tables (a_source_system SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,a_asofdate AGE.ASOFDATE%TYPE) return number;

end pkg_staging;
