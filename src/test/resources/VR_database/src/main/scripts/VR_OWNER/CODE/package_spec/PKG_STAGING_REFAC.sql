--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_STAGING_REFAC runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_STAGING_REFAC" as

function f_prepare_aging_tables_REFAC (a_source_system SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,a_asofdate AGE.ASOFDATE%TYPE) return number;

end PKG_STAGING_REFAC;
