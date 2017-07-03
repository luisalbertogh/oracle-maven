--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BOOK_HIERARCHY runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BOOK_HIERARCHY" 
AS
  /******************************************************************************
  NAME:       PKG_BOOK_HIERARCHY
  PURPOSE:    Procedures needed for Book Hierarchy management
  ******************************************************************************/
  FUNCTION F_GET_MAX_ASOFDATE_BOOK(
      A_REQUIRED_DATE BOOK_HIERARCHY.ASOFDATE%TYPE,
      A_ASOFDATE OUT BOOK_HIERARCHY.ASOFDATE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    SELECT MAX(ASOFDATE)
    INTO A_ASOFDATE
    FROM BOOK_HIERARCHY
    WHERE ASOFDATE<=A_REQUIRED_DATE;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
  END F_GET_MAX_ASOFDATE_BOOK;
-----------------------------------------------------------------------------
-- Functionality: Get the complete book hierarchy by asofdate
-- Used: QV
------------------------------------------------------------------------------
  FUNCTION F_GET_BOOK_HIERARCHY_ADD(
      a_cob_date IN REGIONS.COB_DATE%TYPE )
    RETURN type_resultset
  IS
    r_refCursor type_resultset;
  BEGIN
    OPEN r_refCursor FOR SELECT * FROM VW_BOOK_HIERARCHY WHERE ASOFDATE <= a_cob_date;
    RETURN r_refCursor;
  END F_GET_BOOK_HIERARCHY_ADD;
-----------------------------------------------------------------------------
-- Functionality: Insert a book in the book hierarchy
-- Used: com.db.volcker.bookhierarchy
------------------------------------------------------------------------------
  PROCEDURE pr_insert_book(
      a_book_id      IN BOOK_HIERARCHY.BOOK_ID%TYPE,
      a_business     IN BOOK_HIERARCHY.SUB_BUSINESS%TYPE,
      a_sub_business IN BOOK_HIERARCHY.BUSINESS%TYPE,
      a_trading_unit IN BOOK_HIERARCHY.TRADING_UNIT%TYPE )
  IS
  BEGIN
    INSERT
    INTO book_hierarchy
      (
        asofdate,
        book_id,
        business,
        sub_business,
        trading_unit
      )
      VALUES
      (
        TRUNC(sysdate),
        a_book_id,
        a_business,
        a_sub_business,
        a_trading_unit
      );
    --COMMIT;
  END pr_insert_book;
-----------------------------------------------------------------------------
-- Functionality: Insert a book in the book hierarchy
-- Used: com.db.volcker.bookhierarchy
------------------------------------------------------------------------------
  PROCEDURE pr_insert_book_rpl
    (
      a_book_id                      IN BOOK_HIERARCHY_RPL.BOOK_ID%TYPE,
      a_volcker_trading_desk         IN BOOK_HIERARCHY_RPL.VOLCKER_TRADING_DESK%TYPE,
      a_volcker_trading_desk_full    IN BOOK_HIERARCHY_RPL.VOLCKER_TRADING_DESK_FULL%TYPE,
      a_lowest_level_rpl_code        IN BOOK_HIERARCHY_RPL.LOWEST_LEVEL_RPL_CODE%TYPE,
      a_lowest_level_rpl_full_name   IN BOOK_HIERARCHY_RPL.LOWEST_LEVEL_RPL_FULL_NAME%TYPE,
      a_lowest_level_rpl             IN BOOK_HIERARCHY_RPL.LOWEST_LEVEL_RPL%TYPE,
      a_source_system                IN BOOK_HIERARCHY_RPL.SOURCE_SYSTEM%TYPE,
      a_legal_entity                 IN BOOK_HIERARCHY_RPL.LEGAL_ENTITY%TYPE,
      a_global_trader_book_id        IN BOOK_HIERARCHY_RPL.GLOBAL_TRADER_BOOK_ID%TYPE,
      a_profit_center_id             IN BOOK_HIERARCHY_RPL.PROFIT_CENTER_ID%TYPE,
      a_comments                     IN BOOK_HIERARCHY_RPL.COMMENTS%TYPE,
      a_data_source                  IN BOOK_HIERARCHY_RPL.DATA_SOURCE%TYPE,
      a_charge_reporting_unit_code   IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_UNIT_CODE%TYPE,
      a_charge_reporting_unit        IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_UNIT%TYPE,
      a_charge_reporting_parent_code IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE,
      a_charge_reporting_parent      IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT%TYPE,
      a_mi_location                  IN BOOK_HIERARCHY_RPL.MI_LOCATION%TYPE,
      a_ubr_level_1_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_1_ID%TYPE,
      a_ubr_level_1_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_1_NAME%TYPE,
      a_ubr_level_1_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_1_RPL_CODE%TYPE,
      a_ubr_level_2_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_2_ID%TYPE,
      a_ubr_level_2_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_2_NAME%TYPE,
      a_ubr_level_2_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_2_RPL_CODE%TYPE,
      a_ubr_level_3_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_3_ID%TYPE,
      a_ubr_level_3_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_3_NAME%TYPE,
      a_ubr_level_3_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_3_RPL_CODE%TYPE,
      a_ubr_level_4_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_4_ID%TYPE,
      a_ubr_level_4_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_4_NAME%TYPE,
      a_ubr_level_4_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_4_RPL_CODE%TYPE,
      a_ubr_level_5_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_5_ID%TYPE,
      a_ubr_level_5_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_5_NAME%TYPE,
      a_ubr_level_5_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_5_RPL_CODE%TYPE,
      a_ubr_level_6_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_6_ID%TYPE,
      a_ubr_level_6_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_6_NAME%TYPE,
      a_ubr_level_6_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_6_RPL_CODE%TYPE,
      a_ubr_level_7_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_7_ID%TYPE,
      a_ubr_level_7_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_7_NAME%TYPE,
      a_ubr_level_7_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_7_RPL_CODE%TYPE,
      a_ubr_level_8_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_8_ID%TYPE,
      a_ubr_level_8_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_8_NAME%TYPE,
      a_ubr_level_8_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_8_RPL_CODE%TYPE,
      a_ubr_level_9_id               IN BOOK_HIERARCHY_RPL.UBR_LEVEL_9_ID%TYPE,
      a_ubr_level_9_name             IN BOOK_HIERARCHY_RPL.UBR_LEVEL_9_NAME%TYPE,
      a_ubr_level_9_rpl_code         IN BOOK_HIERARCHY_RPL.UBR_LEVEL_9_RPL_CODE%TYPE,
      a_ubr_level_10_id              IN BOOK_HIERARCHY_RPL.UBR_LEVEL_10_ID%TYPE,
      a_ubr_level_10_name            IN BOOK_HIERARCHY_RPL.UBR_LEVEL_10_NAME%TYPE,
      a_ubr_level_10_rpl_code        IN BOOK_HIERARCHY_RPL.UBR_LEVEL_10_RPL_CODE%TYPE,
      a_ubr_level_11_id              IN BOOK_HIERARCHY_RPL.UBR_LEVEL_11_ID%TYPE,
      a_ubr_level_11_name            IN BOOK_HIERARCHY_RPL.UBR_LEVEL_11_NAME%TYPE,
      a_ubr_level_11_rpl_code        IN BOOK_HIERARCHY_RPL.UBR_LEVEL_11_RPL_CODE%TYPE,
      a_ubr_level_12_id              IN BOOK_HIERARCHY_RPL.UBR_LEVEL_12_ID%TYPE,
      a_ubr_level_12_name            IN BOOK_HIERARCHY_RPL.UBR_LEVEL_12_NAME%TYPE,
      a_ubr_level_12_rpl_code        IN BOOK_HIERARCHY_RPL.UBR_LEVEL_12_RPL_CODE%TYPE,
      a_ubr_level_13_id              IN BOOK_HIERARCHY_RPL.UBR_LEVEL_13_ID%TYPE,
      a_ubr_level_13_name            IN BOOK_HIERARCHY_RPL.UBR_LEVEL_13_NAME%TYPE,
      a_ubr_level_13_rpl_code        IN BOOK_HIERARCHY_RPL.UBR_LEVEL_13_RPL_CODE%TYPE,
      a_ubr_level_14_id              IN BOOK_HIERARCHY_RPL.UBR_LEVEL_14_ID%TYPE,
      a_ubr_level_14_name            IN BOOK_HIERARCHY_RPL.UBR_LEVEL_14_NAME%TYPE,
      a_ubr_level_14_rpl_code        IN BOOK_HIERARCHY_RPL.UBR_LEVEL_14_RPL_CODE%TYPE,
      a_desk_level_1_id              IN BOOK_HIERARCHY_RPL.DESK_LEVEL_1_ID%TYPE,
      a_desk_level_1_name            IN BOOK_HIERARCHY_RPL.DESK_LEVEL_1_NAME%TYPE,
      a_desk_level_1_rpl_code        IN BOOK_HIERARCHY_RPL.DESK_LEVEL_1_RPL_CODE%TYPE,
      a_desk_level_2_id              IN BOOK_HIERARCHY_RPL.DESK_LEVEL_2_ID%TYPE,
      a_desk_level_2_name            IN BOOK_HIERARCHY_RPL.DESK_LEVEL_2_NAME%TYPE,
      a_desk_level_2_rpl_code        IN BOOK_HIERARCHY_RPL.DESK_LEVEL_2_RPL_CODE%TYPE,
      a_desk_level_3_id              IN BOOK_HIERARCHY_RPL.DESK_LEVEL_3_ID%TYPE,
      a_desk_level_3_name            IN BOOK_HIERARCHY_RPL.DESK_LEVEL_3_NAME%TYPE,
      a_desk_level_3_rpl_code        IN BOOK_HIERARCHY_RPL.DESK_LEVEL_3_RPL_CODE%TYPE,
      a_desk_level_4_id              IN BOOK_HIERARCHY_RPL.DESK_LEVEL_4_ID%TYPE,
      a_desk_level_4_name            IN BOOK_HIERARCHY_RPL.DESK_LEVEL_4_NAME%TYPE,
      a_desk_level_4_rpl_code        IN BOOK_HIERARCHY_RPL.DESK_LEVEL_4_RPL_CODE%TYPE,
      a_desk_level_5_id              IN BOOK_HIERARCHY_RPL.DESK_LEVEL_5_ID%TYPE,
      a_desk_level_5_name            IN BOOK_HIERARCHY_RPL.DESK_LEVEL_5_NAME%TYPE,
      a_desk_level_5_rpl_code        IN BOOK_HIERARCHY_RPL.DESK_LEVEL_5_RPL_CODE%TYPE,
      a_portfolio_id                 IN BOOK_HIERARCHY_RPL.PORTFOLIO_ID%TYPE,
      a_portfolio_name               IN BOOK_HIERARCHY_RPL.PORTFOLIO_NAME%TYPE,
      a_portfolio_rpl_code           IN BOOK_HIERARCHY_RPL.PORTFOLIO_RPL_CODE%TYPE,
      a_business                     IN BOOK_HIERARCHY_RPL.BUSINESS%TYPE,
      a_sub_business                 IN BOOK_HIERARCHY_RPL.SUB_BUSINESS%TYPE,
      a_date                         IN VARCHAR2,
      a_region                       IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      a_subregion                    IN BOOK_HIERARCHY_RPL.SUBREGION%TYPE
    )
  IS
    v_asofdate DATE;
  BEGIN
    IF (a_date  IS NULL) THEN
      v_asofdate:=TRUNC(sysdate);
    ELSE
      v_asofdate:=to_date(a_date,'yyyyMMdd');
    END IF;
    INSERT
    INTO book_hierarchy_rpl
      (
        asofdate,
        BOOK_ID,
        VOLCKER_TRADING_DESK,
        VOLCKER_TRADING_DESK_FULL,
        LOWEST_LEVEL_RPL_CODE,
        LOWEST_LEVEL_RPL_FULL_NAME,
        LOWEST_LEVEL_RPL,
        SOURCE_SYSTEM,
        LEGAL_ENTITY,
        GLOBAL_TRADER_BOOK_ID,
        PROFIT_CENTER_ID,
        COMMENTS,
        DATA_SOURCE,
        CREATE_DATE,
        LAST_MODIFIED_DATE,
        CHARGE_REPORTING_UNIT_CODE,
        CHARGE_REPORTING_UNIT,
        CHARGE_REPORTING_PARENT_CODE,
        CHARGE_REPORTING_PARENT,
        MI_LOCATION,
        UBR_LEVEL_1_ID,
        UBR_LEVEL_1_NAME,
        UBR_LEVEL_1_RPL_CODE,
        UBR_LEVEL_2_ID,
        UBR_LEVEL_2_NAME,
        UBR_LEVEL_2_RPL_CODE,
        UBR_LEVEL_3_ID,
        UBR_LEVEL_3_NAME,
        UBR_LEVEL_3_RPL_CODE,
        UBR_LEVEL_4_ID,
        UBR_LEVEL_4_NAME,
        UBR_LEVEL_4_RPL_CODE,
        UBR_LEVEL_5_ID,
        UBR_LEVEL_5_NAME,
        UBR_LEVEL_5_RPL_CODE,
        UBR_LEVEL_6_ID,
        UBR_LEVEL_6_NAME,
        UBR_LEVEL_6_RPL_CODE,
        UBR_LEVEL_7_ID,
        UBR_LEVEL_7_NAME,
        UBR_LEVEL_7_RPL_CODE,
        UBR_LEVEL_8_ID,
        UBR_LEVEL_8_NAME,
        UBR_LEVEL_8_RPL_CODE,
        UBR_LEVEL_9_ID,
        UBR_LEVEL_9_NAME,
        UBR_LEVEL_9_RPL_CODE,
        UBR_LEVEL_10_ID,
        UBR_LEVEL_10_NAME,
        UBR_LEVEL_10_RPL_CODE,
        UBR_LEVEL_11_ID,
        UBR_LEVEL_11_NAME,
        UBR_LEVEL_11_RPL_CODE,
        UBR_LEVEL_12_ID,
        UBR_LEVEL_12_NAME,
        UBR_LEVEL_12_RPL_CODE,
        UBR_LEVEL_13_ID,
        UBR_LEVEL_13_NAME,
        UBR_LEVEL_13_RPL_CODE,
        UBR_LEVEL_14_ID,
        UBR_LEVEL_14_NAME,
        UBR_LEVEL_14_RPL_CODE,
        DESK_LEVEL_1_ID,
        DESK_LEVEL_1_NAME,
        DESK_LEVEL_1_RPL_CODE,
        DESK_LEVEL_2_ID,
        DESK_LEVEL_2_NAME,
        DESK_LEVEL_2_RPL_CODE,
        DESK_LEVEL_3_ID,
        DESK_LEVEL_3_NAME,
        DESK_LEVEL_3_RPL_CODE,
        DESK_LEVEL_4_ID,
        DESK_LEVEL_4_NAME,
        DESK_LEVEL_4_RPL_CODE,
        DESK_LEVEL_5_ID,
        DESK_LEVEL_5_NAME,
        DESK_LEVEL_5_RPL_CODE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        PORTFOLIO_RPL_CODE,
        BUSINESS,
        SUB_BUSINESS,
		REGION,
		SUBREGION
      )
      VALUES
      (
        v_asofdate,
        a_book_id,
        a_volcker_trading_desk,
        a_volcker_trading_desk_full,
        a_lowest_level_rpl_code,
        a_lowest_level_rpl_full_name,
        a_lowest_level_rpl,
        a_source_system,
        a_legal_entity,
        a_global_trader_book_id,
        a_profit_center_id,
        a_comments,
        a_data_source,
        TRUNC(sysdate),
        TRUNC(sysdate),
        a_charge_reporting_unit_code,
        a_charge_reporting_unit,
        a_charge_reporting_parent_code,
        a_charge_reporting_parent,
        a_mi_location,
        a_ubr_level_1_id,
        a_ubr_level_1_name,
        a_ubr_level_1_rpl_code,
        a_ubr_level_2_id,
        a_ubr_level_2_name,
        a_ubr_level_2_rpl_code,
        a_ubr_level_3_id,
        a_ubr_level_3_name,
        a_ubr_level_3_rpl_code,
        a_ubr_level_4_id,
        a_ubr_level_4_name,
        a_ubr_level_4_rpl_code,
        a_ubr_level_5_id,
        a_ubr_level_5_name,
        a_ubr_level_5_rpl_code,
        a_ubr_level_6_id,
        a_ubr_level_6_name,
        a_ubr_level_6_rpl_code,
        a_ubr_level_7_id,
        a_ubr_level_7_name,
        a_ubr_level_7_rpl_code,
        a_ubr_level_8_id,
        a_ubr_level_8_name,
        a_ubr_level_8_rpl_code,
        a_ubr_level_9_id,
        a_ubr_level_9_name,
        a_ubr_level_9_rpl_code,
        a_ubr_level_10_id,
        a_ubr_level_10_name,
        a_ubr_level_10_rpl_code,
        a_ubr_level_11_id,
        a_ubr_level_11_name,
        a_ubr_level_11_rpl_code,
        a_ubr_level_12_id,
        a_ubr_level_12_name,
        a_ubr_level_12_rpl_code,
        a_ubr_level_13_id,
        a_ubr_level_13_name,
        a_ubr_level_13_rpl_code,
        a_ubr_level_14_id,
        a_ubr_level_14_name,
        a_ubr_level_14_rpl_code,
        a_desk_level_1_id,
        a_desk_level_1_name,
        a_desk_level_1_rpl_code,
        a_desk_level_2_id,
        a_desk_level_2_name,
        a_desk_level_2_rpl_code,
        a_desk_level_3_id,
        a_desk_level_3_name,
        a_desk_level_3_rpl_code,
        a_desk_level_4_id,
        a_desk_level_4_name,
        a_desk_level_4_rpl_code,
        a_desk_level_5_id,
        a_desk_level_5_name,
        a_desk_level_5_rpl_code,
        a_portfolio_id,
        a_portfolio_name,
        a_portfolio_rpl_code,
        a_business,
        a_sub_business,
		a_region,
		a_subregion
      );
    --COMMIT;
  END pr_insert_book_rpl;
END PKG_BOOK_HIERARCHY;
