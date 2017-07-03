--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BH runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BH" AS 
  PROCEDURE p_brds_init ( pTableList IN varchar2, pRunId OUT int );
  PROCEDURE p_brds_etl ( pRunId INT, pMode INT DEFAULT 0 );
  PROCEDURE p_brds_etl_load_rpl ( pMode int DEFAULT 0 );  
  PROCEDURE P_BH_INITIAL_LOADING;
  PROCEDURE P_SPLIT_CSV (idUpload IN NUMBER);
  PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR);
  PROCEDURE P_REJECT_UPLOAD (P_IDUPLOAD IN NUMBER, P_IDUSER IN VARCHAR, P_COMMENTS IN VARCHAR);
  PROCEDURE P_BH_GET_CONFLICT_DATA(p_conflict_id IN NUMBER, p_result OUT SYS_REFCURSOR);
  PROCEDURE P_BH_RESOLVE_CONFLICT(p_conflict_id IN NUMBER, p_selected_choice IN VARCHAR2, p_user_name IN VARCHAR2, p_comments IN VARCHAR2);
  PROCEDURE P_BRDS_ETL_CHECK_UI_STATUS ( p_result OUT int );
  --start GBSVR-30036
  PROCEDURE P_BRDS_ETL_RECON_ALERTS ( p_threshold OUT NUMBER, p_book_count_landing OUT NUMBER, p_book_count_out_of_scope OUT NUMBER, p_book_count_presubmission OUT NUMBER );
  --end GBSVR-30036
END PKG_BH;
