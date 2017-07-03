--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_REPORTS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_REPORTS" 
AS
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for CRP
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_CRP(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_CRP    IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for CRP in thousands
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_CRPT(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_CRP    IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for CRP in millions
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_CRPM(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_CRP    IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for VTD
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_VTD(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for VTD
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_VTD(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_REGION IN FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for VTD in thousands
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDT(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for VTD in thousands
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDT(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_REGION IN FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for VTD in million
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDM(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for VTD in million
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDM(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_REGION IN FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER ;
  -----------------------------------------------------------------------------
  -- Functionality: Get currency for CRP
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_CURRENCY_CRP(
      A_CRP IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN VARCHAR2 ;
  -----------------------------------------------------------------------------
  -- Functionality: Get currency for VTD
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_CURRENCY_VTD
    RETURN VARCHAR2 ;
  -----------------------------------------------------------------------------
  -- Functionality: Get asofdate
  -- Used:
  -- DEPRECATED
  ------------------------------------------------------------------------------
  FUNCTION PR_GET_ASOFDATE
    RETURN VARCHAR2;
  -----------------------------------------------------------------------------
  -- Functionality: Get asofdate
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_MIN_HADOOP_LASTDATE(
      A_METRIC_ID IN SOURCE_SYSTEM_METRIC.METRIC_ID%TYPE)
    RETURN DATE;
  -----------------------------------------------------------------------------
  -- Functionality: Get date monthly
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_COMPARISON_MONTH(
      A_ASOFDATE      IN DATE,
      A_COMP          IN NUMBER,
      A_NUMBER_MONTHS IN NUMBER)
    RETURN DATE;
  -----------------------------------------------------------------------------
  -- Functionality: Get fx_rates CONVERSION
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_FX_RATE(
      A_FROM_CCY IN FX_RATES.CURRENCY_ID%TYPE,
      A_TO_CCY   IN FX_RATES.CURRENCY_ID%TYPE,
      A_REGION   IN FX_RATES.REGION_ID%TYPE,
      A_ASOFDATE IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER;
  -----------------------------------------------------------------------------
  -- Functionality: Get region for CRP
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_REGION_CRP(
      A_CRP IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT%TYPE)
    RETURN VARCHAR2 ;
  -----------------------------------------------------------------------------
  -- Functionality: Get region for VTD
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_REGION_VTD
    RETURN VARCHAR2 ;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: Get fx_rates CONVERSION and load the values
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PR_GET_FXRATE_COMP_CASH;
  -----------------------------------------------------------------------------
  -- Functionality: Return currency for regions
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_CURRENCY_REG(
      A_REGION IN VARCHAR2 )
    RETURN VARCHAR2;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for Regions
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_REG(
      A_NUMBER   IN NUMBER,
      A_DATE     IN FX_RATES.ASOFDATE%TYPE,
      A_REGIONSS IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      A_REGION   IN SOURCE_SYSTEM.REGION_ID%TYPE)
    RETURN NUMBER;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for REGION in thousands
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_REGT(
      A_NUMBER   IN NUMBER,
      A_DATE     IN FX_RATES.ASOFDATE%TYPE,
      A_REGIONSS IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      A_REGION   IN SOURCE_SYSTEM.REGION_ID%TYPE)
    RETURN NUMBER;
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for REGION in millions
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_C_RATE_REGM(
      A_NUMBER   IN NUMBER,
      A_DATE     IN FX_RATES.ASOFDATE%TYPE,
      A_REGIONSS IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      A_REGION   IN SOURCE_SYSTEM.REGION_ID%TYPE)
    RETURN NUMBER;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: Get fx_rates CONVERSION and load the values
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PR_GET_FXRATE_COMP_DER;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: Get fx_rates rate and update age
  --
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PR_GET_FX_RATE(
      p_source_system_id IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE);
  ------------------------------------------------------------
  ------ GEt old dates for comparison reports ----------------
  ------------------------------------------------------------
  FUNCTION F_GET_COMPARISON_MONTH_R(
      A_ASOFDATE      IN DATE,
      A_NUMBER_MONTHS IN NUMBER)
    RETURN DATE;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: Load one year of month-end book hierarchies expanded into BOOK_HIERARCHY_RPL_EXPANDED
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_YEAR_BH_RPL_EXPANDED(
      IDATE DATE);
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: Call REFRESH_BH_RPL_EXPANDED for the given date and for the previous 11 month-end dates
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_BH_RPL_EXPANDED(
      IDATE DATE);
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: refresh one date of data in BOOK_HIERARCHY_RPL_EXPANDED with data from BOOK_HIERARCHY_RPL
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE REFRESH_BH_RPL_EXPANDED(
      IDATE DATE,
      IMONTHEND01 DATE,
      IMONTHEND02 DATE,
      IMONTHEND03 DATE,
      IMONTHEND04 DATE,
      IMONTHEND05 DATE,
      IMONTHEND06 DATE,
      IMONTHEND07 DATE,
      IMONTHEND08 DATE,
      IMONTHEND09 DATE,
      IMONTHEND10 DATE,
      IMONTHEND11 DATE);
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: refresh one date of data in BOOK_HIERARCHY_RPL_QV_EXPANDED with data from BOOK_HIERARCHY_RPL
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE REFRESH_BH_RPL_QV_EXPANDED(IDATE DATE);
END PKG_REPORTS;
