--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BH_PROCESS_INTERMED runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BH_PROCESS_INTERMED" AS 

  FUNCTION F_BH_INTERMEDIARY_TO_STAGING(P_ID NUMBER) RETURN BOOLEAN;
  --start GBSVR-30255
  PROCEDURE P_BH_CHECK_RAISING_CONFLICT(p_staging_row IN BH_STAGING%ROWTYPE, p_is_full_match_staging IN BOOLEAN);
  --end GBSVR-30255
  PROCEDURE P_BH_EMERGENCY_LOAD(p_bh_staging_row BH_STAGING%ROWTYPE, p_action_id INTEGER);
  FUNCTION F_BH_INTERMEDIARY_EXPAND(p_intermediary_data BH_INTERMEDIARY%ROWTYPE, v_asofdate DATE, p_is_full_match_staging BOOLEAN) RETURN BH_STAGING%ROWTYPE;  
  PROCEDURE P_LOAD_SINGLE_HIERARCHY(p_bh_intermediary_id IN NUMBER, p_book_in_brds IN BOOLEAN, p_bh_staging_row IN BH_STAGING%ROWTYPE);
  PROCEDURE P_UPDATE_SINGLE_HIERARCHY(p_bh_intermediary_id IN NUMBER, p_bh_staging_row IN OUT BH_STAGING%ROWTYPE);
  --start GBSVR-30032
  PROCEDURE P_SET_LEVEL_FIELDS(P_BH_INTERMEDIARY_ID IN NUMBER, P_LEVEL IN NUMBER, P_GLOBAL_TRADER_BOOK_ID IN NUMBER, P_NODE_TYPE IN VARCHAR2, P_ID OUT VARCHAR2, P_NAME OUT VARCHAR2, P_RPL_CODE OUT VARCHAR2);
  --end GBSVR-30032
  PROCEDURE P_UNLOAD_SINGLE_HIERARCHY(p_bh_intermediary_id IN NUMBER);
END PKG_BH_PROCESS_INTERMED;