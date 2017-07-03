--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BRDS_BH_RPL_WF runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BRDS_BH_RPL_WF" AS  

procedure p_brds_etl_val_book_id_length;
procedure p_brds_etl_val_dup_gtbid;
procedure p_brds_etl_val_dups;
procedure p_brds_etl_val_dup_book_id;
procedure p_brds_etl_val_no_hierarchy;
procedure p_brds_etl_val_dup_node_rpl;
procedure p_brds_etl_val_null_vtd;
-- start GBSVR-27301  Remove ETL validations for CRU/CRP/CFBU:
-- end GBSVR- 27301
--start GBSVR-28997
procedure p_brds_etl_init_hierarchy;
--end GBSVR-28997
procedure p_brds_etl_val_dup_node_rpl_nb;
procedure p_brds_etl_val_vtd;
procedure p_brds_etl_val_cru;
procedure p_brds_etl_val_crp;
-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning
procedure p_brds_etl_workflow_items;
--start GBSVR-28877
procedure p_brds_etl_val_non_vtd;
--end GBSVR-28877

END PKG_BRDS_BH_RPL_WF;
