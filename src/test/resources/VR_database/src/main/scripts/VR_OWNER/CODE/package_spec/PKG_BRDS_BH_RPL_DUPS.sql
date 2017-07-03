--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BRDS_BH_RPL_DUPS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BRDS_BH_RPL_DUPS" AS  

procedure p_brds_etl_remove_duplicates;

procedure p_brds_etl_remove_dup_hierarch;
procedure p_brds_etl_remove_dup_cru;
procedure p_brds_etl_remove_dup_crp;
-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning
procedure p_brds_etl_remove_dup_vtd;
procedure p_brds_etl_remove_dup_book;
procedure p_brds_etl_remove_dup_portfol;

procedure p_brds_etl_add_duplicates;
procedure p_brds_etl_clear_duplicates;

END PKG_BRDS_BH_RPL_DUPS;
