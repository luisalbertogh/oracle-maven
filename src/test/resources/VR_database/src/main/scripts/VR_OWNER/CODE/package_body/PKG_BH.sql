--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BH runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BH" 
AS

PROCEDURE p_brds_init ( pTableList IN varchar2, pRunId OUT int )
IS
BEGIN
  PKG_BRDS_BH_RPL.p_brds_init(pTableList, pRunId);
END p_brds_init;


PROCEDURE p_brds_etl ( pRunId INT, pMode INT DEFAULT 0 )
IS
BEGIN
  pkg_brds_bh_rpl.p_brds_etl(pRunId, pMode);
END p_brds_etl;


PROCEDURE p_brds_etl_load_rpl ( pMode int DEFAULT 0 )
IS
BEGIN
  PKG_BRDS_BH_RPL.p_brds_etl_load_rpl( pMode );
END p_brds_etl_load_rpl;


PROCEDURE P_BH_INITIAL_LOADING
IS
BEGIN
  pkg_bh_process.p_bh_initial_loading;
END P_BH_INITIAL_LOADING;





PROCEDURE P_SPLIT_CSV(
    idUpload IN NUMBER)
IS  
BEGIN
  pkg_bh_process.p_split_csv_process(idUpload);
EXCEPTION
  WHEN OTHERS THEN
    RAISE;
END P_SPLIT_CSV;

PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR)
IS
BEGIN
  pkg_bh_process.p_accept_upload(IDUPLOAD, IDUSER, COMMENTS);
END P_ACCEPT_UPLOAD;

PROCEDURE P_REJECT_UPLOAD (P_IDUPLOAD IN NUMBER, P_IDUSER IN VARCHAR, P_COMMENTS IN VARCHAR)
IS
BEGIN 
  pkg_bh_process.p_reject_upload(P_IDUPLOAD, P_IDUSER, P_COMMENTS);
END P_REJECT_UPLOAD;

PROCEDURE P_BH_GET_CONFLICT_DATA(p_conflict_id IN NUMBER, p_result OUT SYS_REFCURSOR)
IS
BEGIN
  pkg_bh_process.p_bh_get_conflict_data(p_conflict_id, p_result);
END P_BH_GET_CONFLICT_DATA;

PROCEDURE P_BH_RESOLVE_CONFLICT(p_conflict_id IN NUMBER, p_selected_choice IN VARCHAR2, p_user_name IN VARCHAR2, p_comments IN VARCHAR2)
IS
BEGIN
  pkg_bh_process.p_bh_resolve_conflict(p_conflict_id, p_selected_choice, p_user_name, p_comments);
END P_BH_RESOLVE_CONFLICT;

PROCEDURE p_brds_etl_check_ui_status ( p_result OUT int )
IS
BEGIN
  pkg_brds_bh_rpl.p_brds_etl_check_ui_status(p_result);
END p_brds_etl_check_ui_status;

--start GBSVR-30036
PROCEDURE P_BRDS_ETL_RECON_ALERTS ( p_threshold OUT NUMBER, p_book_count_landing OUT NUMBER, p_book_count_out_of_scope OUT NUMBER, p_book_count_presubmission OUT NUMBER )
IS
BEGIN
  pkg_brds_bh_rpl.p_brds_etl_recon_alerts( p_threshold, p_book_count_landing, p_book_count_out_of_scope, p_book_count_presubmission);
END P_BRDS_ETL_RECON_ALERTS;
--end GBSVR-30036

END PKG_BH;
