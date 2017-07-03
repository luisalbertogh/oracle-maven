--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BH_UPDATER runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BH_UPDATER" 
AS
  /**************************************************************************************************************
  * Autor: SERGIO.REDONDO@DB.COM
  * Date: 24/03/2015
  *
  * Purpose: This package manage the related functions for the generation of a new version of the BH
  ***************************************************************************************************************/
  -----------------------------------------------------------------------------
  -- Functionality: Generate a copy of the newest version of the BH with a new date
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GENERATE_COPY_BH(
      new_date book_hierarchy_rpl.asofdate%TYPE)
    RETURN NUMBER
  IS
    v_number NUMBER(15);
    date_aux DATE;
  BEGIN
    SELECT MAX(asofdate) INTO date_aux FROM book_hierarchy_rpl;
    IF date_aux >= new_date THEN
      RETURN -2;
    END IF;
    INSERT
    INTO book_hierarchy_rpl
      (
        ASOFDATE,
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
    SELECT new_date,
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
    FROM book_hierarchy_rpl
    WHERE asofdate =
      (SELECT MAX(asofdate)
      FROM book_hierarchy_rpl
      WHERE TO_CHAR(asofdate,'YYYYMMDD') < TO_CHAR(new_date,'YYYYMMDD')
      );
    COMMIT;
    SELECT COUNT(*)
    INTO v_number
    FROM book_hierarchy_rpl
    WHERE asofdate = new_date;
    RETURN v_number;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RETURN -1;
    RAISE;
  END F_GENERATE_COPY_BH;
  FUNCTION F_PROCESS_INSERT(
      as_of_date book_hierarchy_rpl.ASOFDATE%TYPE,
      bookid book_hierarchy_rpl.BOOK_ID%TYPE,
      volckertradingdesk book_hierarchy_rpl.VOLCKER_TRADING_DESK%TYPE,
      volckertradingdeskfull book_hierarchy_rpl.VOLCKER_TRADING_DESK_FULL%TYPE,
      lowestlevelrplcode book_hierarchy_rpl.LOWEST_LEVEL_RPL_CODE%TYPE,
      lowestlevelrplfullname book_hierarchy_rpl.LOWEST_LEVEL_RPL_FULL_NAME%TYPE,
      lowestlevelrpl book_hierarchy_rpl.LOWEST_LEVEL_RPL%TYPE,
      sourcesystem book_hierarchy_rpl.SOURCE_SYSTEM%TYPE,
      legalentity book_hierarchy_rpl.LEGAL_ENTITY%TYPE,
      globaltraderbookid book_hierarchy_rpl.GLOBAL_TRADER_BOOK_ID%TYPE,
      profitcenterid book_hierarchy_rpl.PROFIT_CENTER_ID%TYPE,
      comments book_hierarchy_rpl.COMMENTS%TYPE,
      datasource book_hierarchy_rpl.DATA_SOURCE%TYPE,
      chargereportingunitcode book_hierarchy_rpl.CHARGE_REPORTING_UNIT_CODE%TYPE,
      chargereportingunit book_hierarchy_rpl.CHARGE_REPORTING_UNIT%TYPE,
      chargereportingparentcode book_hierarchy_rpl.CHARGE_REPORTING_PARENT_CODE%TYPE,
      chargereportingparent book_hierarchy_rpl.CHARGE_REPORTING_PARENT%TYPE,
      milocation book_hierarchy_rpl.MI_LOCATION%TYPE,
      ubrlevel1id book_hierarchy_rpl.UBR_LEVEL_1_ID%TYPE,
      ubrlevel1name book_hierarchy_rpl.UBR_LEVEL_1_NAME%TYPE,
      ubrlevel1rplcode book_hierarchy_rpl.UBR_LEVEL_1_RPL_CODE%TYPE,
      ubrlevel2id book_hierarchy_rpl.UBR_LEVEL_2_ID%TYPE,
      ubrlevel2name book_hierarchy_rpl.UBR_LEVEL_2_NAME%TYPE,
      ubrlevel2rplcode book_hierarchy_rpl.UBR_LEVEL_2_RPL_CODE%TYPE,
      ubrlevel3id book_hierarchy_rpl.UBR_LEVEL_3_ID%TYPE,
      ubrlevel3name book_hierarchy_rpl.UBR_LEVEL_3_NAME%TYPE,
      ubrlevel3rplcode book_hierarchy_rpl.UBR_LEVEL_3_RPL_CODE%TYPE,
      ubrlevel4id book_hierarchy_rpl.UBR_LEVEL_4_ID%TYPE,
      ubrlevel4name book_hierarchy_rpl.UBR_LEVEL_4_NAME%TYPE,
      ubrlevel4rplcode book_hierarchy_rpl.UBR_LEVEL_4_RPL_CODE%TYPE,
      ubrlevel5id book_hierarchy_rpl.UBR_LEVEL_5_ID%TYPE,
      ubrlevel5name book_hierarchy_rpl.UBR_LEVEL_5_NAME%TYPE,
      ubrlevel5rplcode book_hierarchy_rpl.UBR_LEVEL_5_RPL_CODE%TYPE,
      ubrlevel6id book_hierarchy_rpl.UBR_LEVEL_6_ID%TYPE,
      ubrlevel6name book_hierarchy_rpl.UBR_LEVEL_6_NAME%TYPE,
      ubrlevel6rplcode book_hierarchy_rpl.UBR_LEVEL_6_RPL_CODE%TYPE,
      ubrlevel7id book_hierarchy_rpl.UBR_LEVEL_7_ID%TYPE,
      ubrlevel7name book_hierarchy_rpl.UBR_LEVEL_7_NAME%TYPE,
      ubrlevel7rplcode book_hierarchy_rpl.UBR_LEVEL_7_RPL_CODE%TYPE,
      ubrlevel8id book_hierarchy_rpl.UBR_LEVEL_8_ID%TYPE,
      ubrlevel8name book_hierarchy_rpl.UBR_LEVEL_8_NAME%TYPE,
      ubrlevel8rplcode book_hierarchy_rpl.UBR_LEVEL_8_RPL_CODE%TYPE,
      ubrlevel9id book_hierarchy_rpl.UBR_LEVEL_9_ID%TYPE,
      ubrlevel9name book_hierarchy_rpl.UBR_LEVEL_9_NAME%TYPE,
      ubrlevel9rplcode book_hierarchy_rpl.UBR_LEVEL_9_RPL_CODE%TYPE,
      ubrlevel10id book_hierarchy_rpl.UBR_LEVEL_10_ID%TYPE,
      ubrlevel10name book_hierarchy_rpl.UBR_LEVEL_10_NAME%TYPE,
      ubrlevel10rplcode book_hierarchy_rpl.UBR_LEVEL_10_RPL_CODE%TYPE,
      ubrlevel11id book_hierarchy_rpl.UBR_LEVEL_11_ID%TYPE,
      ubrlevel11name book_hierarchy_rpl.UBR_LEVEL_11_NAME%TYPE,
      ubrlevel11rplcode book_hierarchy_rpl.UBR_LEVEL_11_RPL_CODE%TYPE,
      ubrlevel12id book_hierarchy_rpl.UBR_LEVEL_12_ID%TYPE,
      ubrlevel12name book_hierarchy_rpl.UBR_LEVEL_12_NAME%TYPE,
      ubrlevel12rplcode book_hierarchy_rpl.UBR_LEVEL_12_RPL_CODE%TYPE,
      ubrlevel13id book_hierarchy_rpl.UBR_LEVEL_13_ID%TYPE,
      ubrlevel13name book_hierarchy_rpl.UBR_LEVEL_13_NAME%TYPE,
      ubrlevel13rplcode book_hierarchy_rpl.UBR_LEVEL_13_RPL_CODE%TYPE,
      ubrlevel14id book_hierarchy_rpl.UBR_LEVEL_14_ID%TYPE,
      ubrlevel14name book_hierarchy_rpl.UBR_LEVEL_14_NAME%TYPE,
      ubrlevel14rplcode book_hierarchy_rpl.UBR_LEVEL_14_RPL_CODE%TYPE,
      desklevel1id book_hierarchy_rpl.DESK_LEVEL_1_ID%TYPE,
      desklevel1name book_hierarchy_rpl.DESK_LEVEL_1_NAME%TYPE,
      desklevel1rplcode book_hierarchy_rpl.DESK_LEVEL_1_RPL_CODE%TYPE,
      desklevel2id book_hierarchy_rpl.DESK_LEVEL_2_ID%TYPE,
      desklevel2name book_hierarchy_rpl.DESK_LEVEL_2_NAME%TYPE,
      desklevel2rplcode book_hierarchy_rpl.DESK_LEVEL_2_RPL_CODE%TYPE,
      desklevel3id book_hierarchy_rpl.DESK_LEVEL_3_ID%TYPE,
      desklevel3name book_hierarchy_rpl.DESK_LEVEL_3_NAME%TYPE,
      desklevel3rplcode book_hierarchy_rpl.DESK_LEVEL_3_RPL_CODE%TYPE,
      desklevel4id book_hierarchy_rpl.DESK_LEVEL_4_ID%TYPE,
      desklevel4name book_hierarchy_rpl.DESK_LEVEL_4_NAME%TYPE,
      desklevel4rplcode book_hierarchy_rpl.DESK_LEVEL_4_RPL_CODE%TYPE,
      desklevel5id book_hierarchy_rpl.DESK_LEVEL_5_ID%TYPE,
      desklevel5name book_hierarchy_rpl.DESK_LEVEL_5_NAME%TYPE,
      desklevel5rplcode book_hierarchy_rpl.DESK_LEVEL_5_RPL_CODE%TYPE,
      portfolioid book_hierarchy_rpl.PORTFOLIO_ID%TYPE,
      portfolioname book_hierarchy_rpl.PORTFOLIO_NAME%TYPE,
      portfoliorplcode book_hierarchy_rpl.PORTFOLIO_RPL_CODE%TYPE,
      business book_hierarchy_rpl.BUSINESS%TYPE,
      subbusiness book_hierarchy_rpl.SUB_BUSINESS%TYPE,
      region book_hierarchy_rpl.REGION%TYPE,
      subregion book_hierarchy_rpl.SUBREGION%TYPE)
    RETURN NUMBER
  IS
    sourcesystemfound NUMBER;
    uniqueentry       NUMBER;
    ssnull            NUMBER;
  BEGIN
    --EXECUTE immediate 'SET DEFINE OFF';
    ssnull := 0;
    SELECT COUNT(*)
    INTO sourcesystemfound
    FROM SOURCE_SYSTEM
    WHERE SOURCE_SYSTEM_ID = sourcesystem;
    IF sourcesystem       IS NULL THEN
      dbms_output.put_line('source system is null');
      sourcesystemfound := 1;
      ssnull            := 1;
    END IF;
    IF sourcesystemfound > 0 THEN
      dbms_output.put_line('source system is known');
      SELECT COUNT(*) utotal
      INTO uniqueentry
      FROM book_hierarchy_rpl
      WHERE book_id      = bookid
      AND (source_system = sourcesystem
      OR (ssnull         = 1
      AND source_system IS NULL))
      AND asofdate       = as_of_date;
      IF uniqueentry     = 0 THEN
        dbms_output.put_line('entry is unique... inserting...');
        INSERT
        INTO book_hierarchy_rpl
          (
            ASOFDATE,
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
            as_of_date,
            bookid,
            volckertradingdesk,
            volckertradingdeskfull,
            lowestlevelrplcode,
            lowestlevelrplfullname,
            lowestlevelrpl,
            sourcesystem,
            legalentity,
            globaltraderbookid,
            profitcenterid,
            comments,
            datasource,
            chargereportingunitcode,
            chargereportingunit,
            chargereportingparentcode,
            chargereportingparent,
            milocation,
            ubrlevel1id,
            ubrlevel1name,
            ubrlevel1rplcode,
            ubrlevel2id,
            ubrlevel2name,
            ubrlevel2rplcode,
            ubrlevel3id,
            ubrlevel3name,
            ubrlevel3rplcode,
            ubrlevel4id,
            ubrlevel4name,
            ubrlevel4rplcode,
            ubrlevel5id,
            ubrlevel5name,
            ubrlevel5rplcode,
            ubrlevel6id,
            ubrlevel6name,
            ubrlevel6rplcode,
            ubrlevel7id,
            ubrlevel7name,
            ubrlevel7rplcode,
            ubrlevel8id,
            ubrlevel8name,
            ubrlevel8rplcode,
            ubrlevel9id,
            ubrlevel9name,
            ubrlevel9rplcode,
            ubrlevel10id,
            ubrlevel10name,
            ubrlevel10rplcode,
            ubrlevel11id,
            ubrlevel11name,
            ubrlevel11rplcode,
            ubrlevel12id,
            ubrlevel12name,
            ubrlevel12rplcode,
            ubrlevel13id,
            ubrlevel13name,
            ubrlevel13rplcode,
            ubrlevel14id,
            ubrlevel14name,
            ubrlevel14rplcode,
            desklevel1id,
            desklevel1name,
            desklevel1rplcode,
            desklevel2id,
            desklevel2name,
            desklevel2rplcode,
            desklevel3id,
            desklevel3name,
            desklevel3rplcode,
            desklevel4id,
            desklevel4name,
            desklevel4rplcode,
            desklevel5id,
            desklevel5name,
            desklevel5rplcode,
            portfolioid,
            portfolioname,
            portfoliorplcode,
            business,
            subbusiness,
            region,
            subregion
          );
      ELSE
        dbms_output.put_line('entry is repeated.');
        RETURN -1;
      END IF;
    ELSE
      dbms_output.put_line('source system is unknown');
      SELECT COUNT(*)
      INTO uniqueentry
      FROM book_hierarchy_rpl_pending
      WHERE book_id      = bookid
      AND (source_system = sourcesystem
      OR (ssnull         = 1
      AND source_system IS NULL))
      AND asofdate       = as_of_date;
      IF uniqueentry     = 0 THEN
        dbms_output.put_line('entry is unique... inserting...');
        INSERT
        INTO book_hierarchy_rpl_pending
          (
            ASOFDATE,
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
            as_of_date,
            bookid,
            volckertradingdesk,
            volckertradingdeskfull,
            lowestlevelrplcode,
            lowestlevelrplfullname,
            lowestlevelrpl,
            sourcesystem,
            legalentity,
            globaltraderbookid,
            profitcenterid,
            comments,
            datasource,
            chargereportingunitcode,
            chargereportingunit,
            chargereportingparentcode,
            chargereportingparent,
            milocation,
            ubrlevel1id,
            ubrlevel1name,
            ubrlevel1rplcode,
            ubrlevel2id,
            ubrlevel2name,
            ubrlevel2rplcode,
            ubrlevel3id,
            ubrlevel3name,
            ubrlevel3rplcode,
            ubrlevel4id,
            ubrlevel4name,
            ubrlevel4rplcode,
            ubrlevel5id,
            ubrlevel5name,
            ubrlevel5rplcode,
            ubrlevel6id,
            ubrlevel6name,
            ubrlevel6rplcode,
            ubrlevel7id,
            ubrlevel7name,
            ubrlevel7rplcode,
            ubrlevel8id,
            ubrlevel8name,
            ubrlevel8rplcode,
            ubrlevel9id,
            ubrlevel9name,
            ubrlevel9rplcode,
            ubrlevel10id,
            ubrlevel10name,
            ubrlevel10rplcode,
            ubrlevel11id,
            ubrlevel11name,
            ubrlevel11rplcode,
            ubrlevel12id,
            ubrlevel12name,
            ubrlevel12rplcode,
            ubrlevel13id,
            ubrlevel13name,
            ubrlevel13rplcode,
            ubrlevel14id,
            ubrlevel14name,
            ubrlevel14rplcode,
            desklevel1id,
            desklevel1name,
            desklevel1rplcode,
            desklevel2id,
            desklevel2name,
            desklevel2rplcode,
            desklevel3id,
            desklevel3name,
            desklevel3rplcode,
            desklevel4id,
            desklevel4name,
            desklevel4rplcode,
            desklevel5id,
            desklevel5name,
            desklevel5rplcode,
            portfolioid,
            portfolioname,
            portfoliorplcode,
            business,
            subbusiness,
            region,
            subregion
          );
      ELSE
        dbms_output.put_line('entry is repeated.');
        RETURN -1;
      END IF;
    END IF;
    RETURN 1;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RETURN -2;
    RAISE;
  END F_PROCESS_INSERT;
  FUNCTION F_PROCESS_UPDATE
    (
      volckertradingdeskold book_hierarchy_rpl.VOLCKER_TRADING_DESK%TYPE,
      sourcesystemold book_hierarchy_rpl.SOURCE_SYSTEM%TYPE,
      as_of_date book_hierarchy_rpl.ASOFDATE%TYPE,
      bookid book_hierarchy_rpl.BOOK_ID%TYPE,
      volckertradingdesk book_hierarchy_rpl.VOLCKER_TRADING_DESK%TYPE,
      volckertradingdeskfull book_hierarchy_rpl.VOLCKER_TRADING_DESK_FULL%TYPE,
      lowestlevelrplcode book_hierarchy_rpl.LOWEST_LEVEL_RPL_CODE%TYPE,
      lowestlevelrplfullname book_hierarchy_rpl.LOWEST_LEVEL_RPL_FULL_NAME%TYPE,
      lowestlevelrpl book_hierarchy_rpl.LOWEST_LEVEL_RPL%TYPE,
      sourcesystem book_hierarchy_rpl.SOURCE_SYSTEM%TYPE,
      legalentity book_hierarchy_rpl.LEGAL_ENTITY%TYPE,
      globaltraderbookid book_hierarchy_rpl.GLOBAL_TRADER_BOOK_ID%TYPE,
      profitcenterid book_hierarchy_rpl.PROFIT_CENTER_ID%TYPE,
      comments_ book_hierarchy_rpl.COMMENTS%TYPE,
      datasource book_hierarchy_rpl.DATA_SOURCE%TYPE,
      chargereportingunitcode book_hierarchy_rpl.CHARGE_REPORTING_UNIT_CODE%TYPE,
      chargereportingunit book_hierarchy_rpl.CHARGE_REPORTING_UNIT%TYPE,
      chargereportingparentcode book_hierarchy_rpl.CHARGE_REPORTING_PARENT_CODE%TYPE,
      chargereportingparent book_hierarchy_rpl.CHARGE_REPORTING_PARENT%TYPE,
      milocation book_hierarchy_rpl.MI_LOCATION%TYPE,
      ubrlevel1id book_hierarchy_rpl.UBR_LEVEL_1_ID%TYPE,
      ubrlevel1name book_hierarchy_rpl.UBR_LEVEL_1_NAME%TYPE,
      ubrlevel1rplcode book_hierarchy_rpl.UBR_LEVEL_1_RPL_CODE%TYPE,
      ubrlevel2id book_hierarchy_rpl.UBR_LEVEL_2_ID%TYPE,
      ubrlevel2name book_hierarchy_rpl.UBR_LEVEL_2_NAME%TYPE,
      ubrlevel2rplcode book_hierarchy_rpl.UBR_LEVEL_2_RPL_CODE%TYPE,
      ubrlevel3id book_hierarchy_rpl.UBR_LEVEL_3_ID%TYPE,
      ubrlevel3name book_hierarchy_rpl.UBR_LEVEL_3_NAME%TYPE,
      ubrlevel3rplcode book_hierarchy_rpl.UBR_LEVEL_3_RPL_CODE%TYPE,
      ubrlevel4id book_hierarchy_rpl.UBR_LEVEL_4_ID%TYPE,
      ubrlevel4name book_hierarchy_rpl.UBR_LEVEL_4_NAME%TYPE,
      ubrlevel4rplcode book_hierarchy_rpl.UBR_LEVEL_4_RPL_CODE%TYPE,
      ubrlevel5id book_hierarchy_rpl.UBR_LEVEL_5_ID%TYPE,
      ubrlevel5name book_hierarchy_rpl.UBR_LEVEL_5_NAME%TYPE,
      ubrlevel5rplcode book_hierarchy_rpl.UBR_LEVEL_5_RPL_CODE%TYPE,
      ubrlevel6id book_hierarchy_rpl.UBR_LEVEL_6_ID%TYPE,
      ubrlevel6name book_hierarchy_rpl.UBR_LEVEL_6_NAME%TYPE,
      ubrlevel6rplcode book_hierarchy_rpl.UBR_LEVEL_6_RPL_CODE%TYPE,
      ubrlevel7id book_hierarchy_rpl.UBR_LEVEL_7_ID%TYPE,
      ubrlevel7name book_hierarchy_rpl.UBR_LEVEL_7_NAME%TYPE,
      ubrlevel7rplcode book_hierarchy_rpl.UBR_LEVEL_7_RPL_CODE%TYPE,
      ubrlevel8id book_hierarchy_rpl.UBR_LEVEL_8_ID%TYPE,
      ubrlevel8name book_hierarchy_rpl.UBR_LEVEL_8_NAME%TYPE,
      ubrlevel8rplcode book_hierarchy_rpl.UBR_LEVEL_8_RPL_CODE%TYPE,
      ubrlevel9id book_hierarchy_rpl.UBR_LEVEL_9_ID%TYPE,
      ubrlevel9name book_hierarchy_rpl.UBR_LEVEL_9_NAME%TYPE,
      ubrlevel9rplcode book_hierarchy_rpl.UBR_LEVEL_9_RPL_CODE%TYPE,
      ubrlevel10id book_hierarchy_rpl.UBR_LEVEL_10_ID%TYPE,
      ubrlevel10name book_hierarchy_rpl.UBR_LEVEL_10_NAME%TYPE,
      ubrlevel10rplcode book_hierarchy_rpl.UBR_LEVEL_10_RPL_CODE%TYPE,
      ubrlevel11id book_hierarchy_rpl.UBR_LEVEL_11_ID%TYPE,
      ubrlevel11name book_hierarchy_rpl.UBR_LEVEL_11_NAME%TYPE,
      ubrlevel11rplcode book_hierarchy_rpl.UBR_LEVEL_11_RPL_CODE%TYPE,
      ubrlevel12id book_hierarchy_rpl.UBR_LEVEL_12_ID%TYPE,
      ubrlevel12name book_hierarchy_rpl.UBR_LEVEL_12_NAME%TYPE,
      ubrlevel12rplcode book_hierarchy_rpl.UBR_LEVEL_12_RPL_CODE%TYPE,
      ubrlevel13id book_hierarchy_rpl.UBR_LEVEL_13_ID%TYPE,
      ubrlevel13name book_hierarchy_rpl.UBR_LEVEL_13_NAME%TYPE,
      ubrlevel13rplcode book_hierarchy_rpl.UBR_LEVEL_13_RPL_CODE%TYPE,
      ubrlevel14id book_hierarchy_rpl.UBR_LEVEL_14_ID%TYPE,
      ubrlevel14name book_hierarchy_rpl.UBR_LEVEL_14_NAME%TYPE,
      ubrlevel14rplcode book_hierarchy_rpl.UBR_LEVEL_14_RPL_CODE%TYPE,
      desklevel1id book_hierarchy_rpl.DESK_LEVEL_1_ID%TYPE,
      desklevel1name book_hierarchy_rpl.DESK_LEVEL_1_NAME%TYPE,
      desklevel1rplcode book_hierarchy_rpl.DESK_LEVEL_1_RPL_CODE%TYPE,
      desklevel2id book_hierarchy_rpl.DESK_LEVEL_2_ID%TYPE,
      desklevel2name book_hierarchy_rpl.DESK_LEVEL_2_NAME%TYPE,
      desklevel2rplcode book_hierarchy_rpl.DESK_LEVEL_2_RPL_CODE%TYPE,
      desklevel3id book_hierarchy_rpl.DESK_LEVEL_3_ID%TYPE,
      desklevel3name book_hierarchy_rpl.DESK_LEVEL_3_NAME%TYPE,
      desklevel3rplcode book_hierarchy_rpl.DESK_LEVEL_3_RPL_CODE%TYPE,
      desklevel4id book_hierarchy_rpl.DESK_LEVEL_4_ID%TYPE,
      desklevel4name book_hierarchy_rpl.DESK_LEVEL_4_NAME%TYPE,
      desklevel4rplcode book_hierarchy_rpl.DESK_LEVEL_4_RPL_CODE%TYPE,
      desklevel5id book_hierarchy_rpl.DESK_LEVEL_5_ID%TYPE,
      desklevel5name book_hierarchy_rpl.DESK_LEVEL_5_NAME%TYPE,
      desklevel5rplcode book_hierarchy_rpl.DESK_LEVEL_5_RPL_CODE%TYPE,
      portfolioid book_hierarchy_rpl.PORTFOLIO_ID%TYPE,
      portfolioname book_hierarchy_rpl.PORTFOLIO_NAME%TYPE,
      portfoliorplcode book_hierarchy_rpl.PORTFOLIO_RPL_CODE%TYPE,
      business_ book_hierarchy_rpl.BUSINESS%TYPE,
      subbusiness book_hierarchy_rpl.SUB_BUSINESS%TYPE,
      region_ book_hierarchy_rpl.REGION%TYPE,
      subregion_ book_hierarchy_rpl.SUBREGION%TYPE
    )
    RETURN NUMBER
  IS
    sourcesystemfound NUMBER;
    uniqueentry       NUMBER;
    ssnull            NUMBER;
    total             NUMBER := 0;
  BEGIN
    --EXECUTE immediate 'SET DEFINE OFF';
    ssnull := 0;
    SELECT COUNT(*)
    INTO sourcesystemfound
    FROM SOURCE_SYSTEM
    WHERE SOURCE_SYSTEM_ID = sourcesystem;
    IF sourcesystemold    IS NULL THEN
      dbms_output.put_line('source system is null');
      sourcesystemfound := 1;
      ssnull            := 1;
    END IF;
    IF sourcesystemfound > 0 THEN
      dbms_output.put_line('source system is known');
      dbms_output.put_line('updating...');
      UPDATE book_hierarchy_rpl
      SET ASOFDATE                   = as_of_date,
        BOOK_ID                      = bookid,
        VOLCKER_TRADING_DESK         = volckertradingdesk,
        VOLCKER_TRADING_DESK_FULL    = volckertradingdeskfull,
        LOWEST_LEVEL_RPL_CODE        = lowestlevelrplcode,
        LOWEST_LEVEL_RPL_FULL_NAME   = lowestlevelrplfullname,
        LOWEST_LEVEL_RPL             = lowestlevelrpl,
        SOURCE_SYSTEM                = sourcesystem,
        LEGAL_ENTITY                 = legalentity,
        GLOBAL_TRADER_BOOK_ID        = globaltraderbookid,
        PROFIT_CENTER_ID             = profitcenterid,
        COMMENTS                     = comments_,
        DATA_SOURCE                  = datasource,
        CHARGE_REPORTING_UNIT_CODE   = chargereportingunitcode,
        CHARGE_REPORTING_UNIT        = chargereportingunit,
        CHARGE_REPORTING_PARENT_CODE = chargereportingparentcode,
        CHARGE_REPORTING_PARENT      = chargereportingparent,
        MI_LOCATION                  = milocation,
        UBR_LEVEL_1_ID               = ubrlevel1id,
        UBR_LEVEL_1_NAME             = ubrlevel1name,
        UBR_LEVEL_1_RPL_CODE         = ubrlevel1rplcode,
        UBR_LEVEL_2_ID               = ubrlevel2id,
        UBR_LEVEL_2_NAME             = ubrlevel2name,
        UBR_LEVEL_2_RPL_CODE         = ubrlevel2rplcode,
        UBR_LEVEL_3_ID               = ubrlevel3id,
        UBR_LEVEL_3_NAME             = ubrlevel3name,
        UBR_LEVEL_3_RPL_CODE         = ubrlevel3rplcode,
        UBR_LEVEL_4_ID               = ubrlevel4id,
        UBR_LEVEL_4_NAME             = ubrlevel4name,
        UBR_LEVEL_4_RPL_CODE         = ubrlevel4rplcode,
        UBR_LEVEL_5_ID               = ubrlevel5id,
        UBR_LEVEL_5_NAME             = ubrlevel5name,
        UBR_LEVEL_5_RPL_CODE         = ubrlevel5rplcode,
        UBR_LEVEL_6_ID               = ubrlevel6id,
        UBR_LEVEL_6_NAME             = ubrlevel6name,
        UBR_LEVEL_6_RPL_CODE         = ubrlevel6rplcode,
        UBR_LEVEL_7_ID               = ubrlevel7id,
        UBR_LEVEL_7_NAME             = ubrlevel7name,
        UBR_LEVEL_7_RPL_CODE         = ubrlevel7rplcode,
        UBR_LEVEL_8_ID               = ubrlevel8id,
        UBR_LEVEL_8_NAME             = ubrlevel8name,
        UBR_LEVEL_8_RPL_CODE         = ubrlevel8rplcode,
        UBR_LEVEL_9_ID               = ubrlevel9id,
        UBR_LEVEL_9_NAME             = ubrlevel9name,
        UBR_LEVEL_9_RPL_CODE         = ubrlevel9rplcode,
        UBR_LEVEL_10_ID              = ubrlevel10id,
        UBR_LEVEL_10_NAME            = ubrlevel10name,
        UBR_LEVEL_10_RPL_CODE        = ubrlevel10rplcode,
        UBR_LEVEL_11_ID              = ubrlevel11id,
        UBR_LEVEL_11_NAME            = ubrlevel11name,
        UBR_LEVEL_11_RPL_CODE        = ubrlevel11rplcode,
        UBR_LEVEL_12_ID              = ubrlevel12id,
        UBR_LEVEL_12_NAME            = ubrlevel12name,
        UBR_LEVEL_12_RPL_CODE        = ubrlevel12rplcode,
        UBR_LEVEL_13_ID              = ubrlevel13id,
        UBR_LEVEL_13_NAME            = ubrlevel13name,
        UBR_LEVEL_13_RPL_CODE        = ubrlevel13rplcode,
        UBR_LEVEL_14_ID              = ubrlevel14id,
        UBR_LEVEL_14_NAME            = ubrlevel14name,
        UBR_LEVEL_14_RPL_CODE        = ubrlevel14rplcode,
        DESK_LEVEL_1_ID              = desklevel1id,
        DESK_LEVEL_1_NAME            = desklevel1name,
        DESK_LEVEL_1_RPL_CODE        = desklevel1rplcode,

DESK_LEVEL_2_ID              = desklevel2id,
        DESK_LEVEL_2_NAME            = desklevel2name,
        DESK_LEVEL_2_RPL_CODE        = desklevel2rplcode,
        DESK_LEVEL_3_ID              = desklevel3id,
        DESK_LEVEL_3_NAME            = desklevel3name,
        DESK_LEVEL_3_RPL_CODE        = desklevel3rplcode,
        DESK_LEVEL_4_ID              = desklevel4id,
        DESK_LEVEL_4_NAME            = desklevel4name,
        DESK_LEVEL_4_RPL_CODE        = desklevel4rplcode,
        DESK_LEVEL_5_ID              = desklevel5id,
        DESK_LEVEL_5_NAME            = desklevel5name,
        DESK_LEVEL_5_RPL_CODE        = desklevel5rplcode,
        PORTFOLIO_ID                 = portfolioid,
        PORTFOLIO_NAME               = portfolioname,
        PORTFOLIO_RPL_CODE           = portfoliorplcode,
        BUSINESS                     = business_,
        SUB_BUSINESS                 = subbusiness,
        REGION                       = region_,
        SUBREGION                    = subregion_
      WHERE ASOFDATE                 = as_of_date
      AND BOOK_ID                    = bookid
      AND VOLCKER_TRADING_DESK       = volckertradingdeskold
      AND (source_system             = sourcesystemold
      OR (ssnull                     = 1
      AND source_system             IS NULL));
      total                         := sql%rowcount;
      dbms_output.put_line('updated rows: '||total);
    ELSE
      dbms_output.put_line('source system is unknown');
      dbms_output.put_line('updating...');
      UPDATE book_hierarchy_rpl_pending
      SET ASOFDATE                   = as_of_date,
        BOOK_ID                      = bookid,
        VOLCKER_TRADING_DESK         = volckertradingdesk,
        VOLCKER_TRADING_DESK_FULL    = volckertradingdeskfull,
        LOWEST_LEVEL_RPL_CODE        = lowestlevelrplcode,
        LOWEST_LEVEL_RPL_FULL_NAME   = lowestlevelrplfullname,
        LOWEST_LEVEL_RPL             = lowestlevelrpl,
        SOURCE_SYSTEM                = sourcesystem,
        LEGAL_ENTITY                 = legalentity,
        GLOBAL_TRADER_BOOK_ID        = globaltraderbookid,
        PROFIT_CENTER_ID             = profitcenterid,
        COMMENTS                     = comments_,
        DATA_SOURCE                  = datasource,
        CHARGE_REPORTING_UNIT_CODE   = chargereportingunitcode,
        CHARGE_REPORTING_UNIT        = chargereportingunit,
        CHARGE_REPORTING_PARENT_CODE = chargereportingparentcode,
        CHARGE_REPORTING_PARENT      = chargereportingparent,
        MI_LOCATION                  = milocation,
        UBR_LEVEL_1_ID               = ubrlevel1id,
        UBR_LEVEL_1_NAME             = ubrlevel1name,
        UBR_LEVEL_1_RPL_CODE         = ubrlevel1rplcode,
        UBR_LEVEL_2_ID               = ubrlevel2id,
        UBR_LEVEL_2_NAME             = ubrlevel2name,
        UBR_LEVEL_2_RPL_CODE         = ubrlevel2rplcode,
        UBR_LEVEL_3_ID               = ubrlevel3id,
        UBR_LEVEL_3_NAME             = ubrlevel3name,
        UBR_LEVEL_3_RPL_CODE         = ubrlevel3rplcode,
        UBR_LEVEL_4_ID               = ubrlevel4id,
        UBR_LEVEL_4_NAME             = ubrlevel4name,
        UBR_LEVEL_4_RPL_CODE         = ubrlevel4rplcode,
        UBR_LEVEL_5_ID               = ubrlevel5id,
        UBR_LEVEL_5_NAME             = ubrlevel5name,
        UBR_LEVEL_5_RPL_CODE         = ubrlevel5rplcode,
        UBR_LEVEL_6_ID               = ubrlevel6id,
        UBR_LEVEL_6_NAME             = ubrlevel6name,
        UBR_LEVEL_6_RPL_CODE         = ubrlevel6rplcode,
        UBR_LEVEL_7_ID               = ubrlevel7id,
        UBR_LEVEL_7_NAME             = ubrlevel7name,
        UBR_LEVEL_7_RPL_CODE         = ubrlevel7rplcode,
        UBR_LEVEL_8_ID               = ubrlevel8id,
        UBR_LEVEL_8_NAME             = ubrlevel8name,
        UBR_LEVEL_8_RPL_CODE         = ubrlevel8rplcode,
        UBR_LEVEL_9_ID               = ubrlevel9id,
        UBR_LEVEL_9_NAME             = ubrlevel9name,
        UBR_LEVEL_9_RPL_CODE         = ubrlevel9rplcode,
        UBR_LEVEL_10_ID              = ubrlevel10id,
        UBR_LEVEL_10_NAME            = ubrlevel10name,
        UBR_LEVEL_10_RPL_CODE        = ubrlevel10rplcode,
        UBR_LEVEL_11_ID              = ubrlevel11id,
        UBR_LEVEL_11_NAME            = ubrlevel11name,
        UBR_LEVEL_11_RPL_CODE        = ubrlevel11rplcode,
        UBR_LEVEL_12_ID              = ubrlevel12id,
        UBR_LEVEL_12_NAME            = ubrlevel12name,
        UBR_LEVEL_12_RPL_CODE        = ubrlevel12rplcode,
        UBR_LEVEL_13_ID              = ubrlevel13id,
        UBR_LEVEL_13_NAME            = ubrlevel13name,
        UBR_LEVEL_13_RPL_CODE        = ubrlevel13rplcode,
        UBR_LEVEL_14_ID              = ubrlevel14id,
        UBR_LEVEL_14_NAME            = ubrlevel14name,
        UBR_LEVEL_14_RPL_CODE        = ubrlevel14rplcode,
        DESK_LEVEL_1_ID              = desklevel1id,
        DESK_LEVEL_1_NAME            = desklevel1name,
        DESK_LEVEL_1_RPL_CODE        = desklevel1rplcode,
        DESK_LEVEL_2_ID              = desklevel2id,
        DESK_LEVEL_2_NAME            = desklevel2name,
        DESK_LEVEL_2_RPL_CODE        = desklevel2rplcode,
        DESK_LEVEL_3_ID              = desklevel3id,
        DESK_LEVEL_3_NAME            = desklevel3name,
        DESK_LEVEL_3_RPL_CODE        = desklevel3rplcode,
        DESK_LEVEL_4_ID              = desklevel4id,
        DESK_LEVEL_4_NAME            = desklevel4name,
        DESK_LEVEL_4_RPL_CODE        = desklevel4rplcode,
        DESK_LEVEL_5_ID              = desklevel5id,
        DESK_LEVEL_5_NAME            = desklevel5name,
        DESK_LEVEL_5_RPL_CODE        = desklevel5rplcode,
        PORTFOLIO_ID                 = portfolioid,
        PORTFOLIO_NAME               = portfolioname,
        PORTFOLIO_RPL_CODE           = portfoliorplcode,
        BUSINESS                     = business_,
        SUB_BUSINESS                 = subbusiness,
        REGION                       = region_,
        SUBREGION                    = subregion_
      WHERE ASOFDATE                 = as_of_date
      AND BOOK_ID                    = bookid
      AND VOLCKER_TRADING_DESK       = volckertradingdeskold
      AND (source_system             = sourcesystemold
      OR (ssnull                     = 1
      AND source_system             IS NULL));
      total                         := sql%rowcount;
      dbms_output.put_line('updated rows: '||total);
    END IF;
    RETURN total;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RETURN -2;
    RAISE;
  END F_PROCESS_UPDATE;
  FUNCTION F_PROCESS_DELETE(
      volckertradingdeskold book_hierarchy_rpl.VOLCKER_TRADING_DESK%TYPE,
      sourcesystemold book_hierarchy_rpl.SOURCE_SYSTEM%TYPE,
      as_of_date book_hierarchy_rpl.ASOFDATE%TYPE,
      bookid book_hierarchy_rpl.BOOK_ID%TYPE)
    RETURN NUMBER
  IS
    sourcesystemfound NUMBER;
    uniqueentry       NUMBER;
    ssnull            NUMBER;
    total             NUMBER := 0;
  BEGIN
    --EXECUTE immediate 'SET DEFINE OFF';
    ssnull := 0;
    SELECT COUNT(*)
    INTO sourcesystemfound
    FROM SOURCE_SYSTEM
    WHERE SOURCE_SYSTEM_ID = sourcesystemold;
    IF sourcesystemold    IS NULL THEN
      dbms_output.put_line('source system is null');
      sourcesystemfound := 1;
      ssnull            := 1;
    END IF;
    IF sourcesystemfound > 0 THEN
      dbms_output.put_line('source system is known');
      dbms_output.put_line('deleting...');
      DELETE
      FROM book_hierarchy_rpl
      WHERE ASOFDATE           = as_of_date
      AND BOOK_ID              = bookid
      AND VOLCKER_TRADING_DESK = volckertradingdeskold
      AND SOURCE_SYSTEM        = sourcesystemold;
      total                   := sql%rowcount;
      dbms_output.put_line('deleted rows: '||total);
    ELSE
      dbms_output.put_line('source system is unknown');
      dbms_output.put_line('deleting...');
      DELETE
      FROM book_hierarchy_rpl_pending
      WHERE ASOFDATE           = as_of_date
      AND BOOK_ID              = bookid
      AND VOLCKER_TRADING_DESK = volckertradingdeskold
      AND SOURCE_SYSTEM        = sourcesystemold;
      total                   := sql%rowcount;
      dbms_output.put_line('deleted rows: '||total);
    END IF;
    RETURN total;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RETURN -2;
    RAISE;
  END F_PROCESS_DELETE;
  
  function CLEAR_UNFINISHED_BH_RELOADS(DESTINY IN VARCHAR2) return number is
	cursor c_hive is 
		SELECT 
			* 
		FROM 
			book_hierarchy_rpl_reload 
		WHERE 
			reload_hive_start IS NOT NULL AND 
			reload_hive_end IS NULL and 
			reload_hive_start < (sysdate-1)
	;
	
	cursor c_qv is 
		SELECT 
			* 
		FROM 
			book_hierarchy_rpl_reload 
		WHERE 
			reload_qv_start IS NOT NULL AND 
			reload_qv_end IS NULL and 
			reload_qv_start < (sysdate-1)
	;
	
	v_bh_rpl_reload book_hierarchy_rpl_reload%ROWTYPE;
  begin 
	
	if lower(destiny) = 'hive' then 
	
		for v_bh_rpl_reload in c_hive loop
			-- set previous executions as done
			update book_hierarchy_rpl_reload 
			set reload_hive_end = systimestamp 
			where id = v_bh_rpl_reload.id
			;
			pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'BH_RELOAD',current_date,'PKG_BH_UPDATER.CLEAR_UNFINISHED_BH_RELOADS','LOGGING', 'INFO', 'Set hive uncompleted reload as done', 'Id of hive reload '|| v_bh_rpl_reload.id || ' for asofdate ' || v_bh_rpl_reload.asofdate, 'BH_RELOAD');
			-- insert new reload record for the pending execution to be done, only for the reload type aka destiny received as a parameter to this function 
			INSERT INTO BOOK_HIERARCHY_RPL_RELOAD 
				(ID, ASOFDATE, REQUEST_TIMESTAMP, RELOAD_HIVE_START, RELOAD_HIVE_END, RELOAD_QV_START, RELOAD_QV_END)
			VALUES (
				SEQ_BOOK_HIERARCHY_RPL_RELOAD.NEXTVAL,
				v_bh_rpl_reload.asofdate,
				systimestamp,
				null, null, 
				systimestamp, systimestamp
				)
			;
		end loop;
	
	elsif lower(destiny) = 'qv' then
	
		for v_bh_rpl_reload in c_qv loop
			-- set previous executions as done
			update book_hierarchy_rpl_reload 
			set reload_qv_end = systimestamp 
			where id = v_bh_rpl_reload.id
			;
			pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'BH_RELOAD',current_date,'PKG_BH_UPDATER.CLEAR_UNFINISHED_BH_RELOADS','LOGGING', 'INFO', 'Set QV uncompleted reload as done', 'Id of QV reload '|| v_bh_rpl_reload.id || ' for asofdate ' || v_bh_rpl_reload.asofdate, 'BH_RELOAD');
			-- insert new reload record for the pending execution to be done, only for the reload type aka destiny received as a parameter to this function 
			INSERT INTO BOOK_HIERARCHY_RPL_RELOAD 
				(ID, ASOFDATE, REQUEST_TIMESTAMP, RELOAD_HIVE_START, RELOAD_HIVE_END, RELOAD_QV_START, RELOAD_QV_END)
			VALUES (
				SEQ_BOOK_HIERARCHY_RPL_RELOAD.NEXTVAL,
				v_bh_rpl_reload.asofdate,
				systimestamp,
				systimestamp, systimestamp,
				null, null
				)
			;
		end loop;
	
	end if;
	
	-- commit changes
	commit;
	-- return success
	return 0;
  exception
	when others then
		pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'BH_RELOAD',current_date,'PKG_BH_UPDATER.CLEAR_UNFINISHED_BH_RELOADS','ERROR', 'FATAL', 'Error: '||TO_CHAR(SQLCODE), SUBSTR(SQLERRM, 1, 2500), 'BH_RELOAD');
		rollback;
		return -1;
  end CLEAR_UNFINISHED_BH_RELOADS;

END PKG_BH_UPDATER;
