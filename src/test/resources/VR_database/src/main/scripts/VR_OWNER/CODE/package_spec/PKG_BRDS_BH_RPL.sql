--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BRDS_BH_RPL runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BRDS_BH_RPL" AS  

procedure p_brds_init ( pTableList IN varchar2, pRunId OUT int );
procedure p_brds_etl_build_hierarchy;
procedure p_brds_etl_set_hierarchy;
procedure p_brds_etl_load_core_data;
procedure p_brds_etl_update_core_data;
procedure p_brds_etl_non_vtd;
procedure p_brds_etl_apply_deltas;
procedure p_brds_etl_manual_conflicts;
procedure p_brds_etl_load_rpl ( pMode int DEFAULT 0 );
procedure p_brds_etl ( pRunId IN int, pMode IN int DEFAULT 0 );
procedure p_brds_etl_check_ui_status ( p_result OUT int );
procedure p_brds_etl_recon_alerts ( p_threshold out number, p_book_count_landing out number, p_book_count_out_of_scope out number, p_book_count_presubmission out number );
procedure  p_brds_etl_load_rpl_new_books;
END PKG_BRDS_BH_RPL;
