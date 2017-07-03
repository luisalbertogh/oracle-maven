--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_PR_ETL runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_PR_ETL" as

procedure p_prds_init ( pRunId OUT int );
procedure p_prds_etl_exceptions; 
procedure p_prds_etl_load_core_data;
procedure p_prds_etl_insert_staging;
procedure p_prds_etl ( pRunId IN int );

end pkg_pr_etl;
