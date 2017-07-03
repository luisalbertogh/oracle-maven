--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BH_COMMONS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BH_COMMONS" AS  
  FUNCTION F_GET_DATE_EMERGENCY_FLAG(isEmergency IN BOOLEAN)RETURN DATE;
  FUNCTION F_EXIST_RPL_BOOK(p_bookId IN VARCHAR2,  p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_RPL_BOOK_AND_SS(p_bookId IN VARCHAR2, p_sourceSystem IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_STA_BOOK(p_bookId IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_STA_BOOK_AND_SS(p_bookId IN VARCHAR2, p_sourceSystem IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_BRDS_GTB(p_gtb IN VARCHAR2) RETURN BOOLEAN;
  FUNCTION F_EXIST_BRDS_GTB_BOOK_SS(p_gtb IN VARCHAR2, p_sourceSystem IN VARCHAR2) RETURN BOOLEAN;
  FUNCTION F_EXIST_STA_GTB(p_gtb IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_GET_VALIDATION_MSG(code IN NUMBER) RETURN VARCHAR2;
  FUNCTION F_IS_DATE_OK(p_date VARCHAR2) RETURN CHAR;
  FUNCTION IS_EMERGENCY_FLAG(v_date varchar2) RETURN VARCHAR2;
  FUNCTION f_is_valid_gtb_vtd(global_trader_book_id IN varchar2, volcker_trading_desk IN varchar2, p_process IN int)  RETURN int;
  FUNCTION f_is_valid_book_vtd(book_id IN varchar2, volcker_trading_desk IN varchar2, p_process IN int)  RETURN int;
  FUNCTION f_is_valid_gtb_cru(global_trader_book_id IN varchar2, charge_reporting_unit IN varchar2, p_process IN int)  RETURN int;
  FUNCTION f_is_valid_book_cru(book_id IN varchar2, charge_reporting_unit IN varchar2, p_process IN int)  RETURN int;
  FUNCTION f_is_valid_gtb_crp(global_trader_book_id IN varchar2, charge_reporting_parent IN varchar2, p_process IN int)  RETURN int;
  FUNCTION f_is_valid_book_crp(book_id IN varchar2, charge_reporting_parent IN varchar2, p_process IN int)  RETURN int;
  -- GBSVR-33754: Start 1: Remove redundant functions: f_is_valid_gtb_cfbu, f_is_valid_book_cfbu
  -- GBSVR-33754: End 1:   
  FUNCTION F_EXIST_BRDS_RPLCODE(p_rpl_code IN VARCHAR2) RETURN BOOLEAN;
  FUNCTION F_IS_EQUAL_BRDS_BOOK(p_gtb IN VARCHAR2, p_book_id IN VARCHAR2) RETURN BOOLEAN;
  PROCEDURE P_VALIDATION_LOAD_APPROVE(p_bh_intermediary IN BH_INTERMEDIARY%ROWTYPE,p_process IN int,p_validation_msg OUT varchar2,p_cont_error OUT INTEGER);
  FUNCTION F_IS_BRDS(DATA_SOURCE IN VARCHAR2) RETURN NUMBER;
  FUNCTION F_IS_MANUAL(DATA_SOURCE IN VARCHAR2) RETURN NUMBER;
  FUNCTION F_GET_BRDS_BOOK(p_gtb IN VARCHAR2) RETURN VARCHAR2;
  FUNCTION F_GET_MAX_ASOFDATE_BH_RPL RETURN DATE;
  FUNCTION F_IS_FULL_MATCH_RPL(p_intermediary_data BH_INTERMEDIARY%ROWTYPE) RETURN BOOLEAN;
  FUNCTION F_IS_FULL_MATCH_STAGING(p_intermediary_data BH_INTERMEDIARY%ROWTYPE, p_use_active_flag IN BOOLEAN) RETURN BOOLEAN;
  --start GBSVR-30255
  FUNCTION F_IS_FULL_MATCH_STAGING(p_book_id IN VARCHAR2, p_global_trader_book_id IN VARCHAR2, p_volcker_trading_desk IN VARCHAR2, p_use_active_flag IN BOOLEAN) RETURN BOOLEAN;
  --end GBSVR-30255
  FUNCTION F_CONVERT_REGION_AMER(p_region IN VARCHAR2) RETURN VARCHAR2;    
  FUNCTION F_CONVERT_TO_NULL(p_string_null VARCHAR2) RETURN VARCHAR2;
  FUNCTION F_EXIST_RPL_BOOK_MANUAL(p_bookId IN VARCHAR2,  p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_RPL_BOOK_AND_SS_MANUAL(p_bookId IN VARCHAR2, p_sourceSystem IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_STA_BOOK_MANUAL(p_bookId IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_STA_BOOK_AND_SS_MANUAL(p_bookId IN VARCHAR2, p_sourceSystem IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN;
  FUNCTION F_EXIST_RPL_CODE_AND_IS_UBR(p_rpl_code IN VARCHAR2) RETURN BOOLEAN;
  FUNCTION f_hierarchy_dups_gtb(p_gtb IN VARCHAR2) RETURN int;
  FUNCTION f_hierarchy_dups_book(p_book_id IN VARCHAR2) RETURN int;
  FUNCTION F_IS_BRDS_INTEGRATION_ACTIVE RETURN BOOLEAN;
END PKG_BH_COMMONS;
