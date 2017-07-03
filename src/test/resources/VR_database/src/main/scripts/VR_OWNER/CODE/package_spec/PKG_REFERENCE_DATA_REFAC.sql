--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_REFERENCE_DATA_REFAC runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_REFERENCE_DATA_REFAC" as

 /**************************************************************************************************************
  ||
  || Date: 16/03/2015
  ||
  || This package manage all the related functions or procedures of reference data
  ||  F_SET_ROLL_DATE              --> Funtion to roll the date to next business date, update PREV_DATE,COB_DATE and NEXT_DATE columns of REGIONS table
  ||  F_GET_COB_DATE               --> Function to get the maximum COB_DATE from a specific REGION_ID into REGIONS table
  ||  F_GET_PREV_DATE              --> Function to get the maximum PREV_DATE from a specific REGION_ID into REGIONS table
  ||  F_GET_MAX_ASOFDATE           --> Function to get the maximum ASOFDATE from a specific REQUIRED_DATE into BOOK_HIERARCHY table
  ||  P_GET_BOOK_HIERARCHY         --> Procedure to get the data of BOOK_HIERARCHY table for a specific REGION_ID
  ||  P_INSERT_BOOK                --> Procedure to insert book into the BOKK_HIERARCHY
  || F_GET_FX_RATES                --> Function to get the values of all the currencies in the fx_rates for an specific ASOFDATE and REGION_ID
  || F_GET_LAST_FX_RATES           --> Function to get the values of all the currencies in the fx_rates for the previous max date of an specific ASOFDATE and REGION_ID
  || F_INSERT_FX_RATES             --> Function to insert the SPOT of a currency in the fx_rates for an specific ASOFDATE and REGION_ID and CURRENCY_ID
  || F_GET_REPORT_TRADING_DESK     --> Function to get the reporting book.
   ***************************************************************************************************************/

 TYPE TYPE_RESULTSET IS REF CURSOR;
  TYPE TYPE_RESULSET_UNIX IS REF CURSOR;

  FUNCTION F_GET_COB_DATE_REFAC (A_REGION_ID IN REGIONS.REGION_ID%TYPE,A_COB_DATE OUT REGIONS.COB_DATE%TYPE) RETURN NUMBER;
  FUNCTION F_GET_PREV_DATE_REFAC (A_REGION_ID IN REGIONS.REGION_ID%TYPE,A_PREV_DATE OUT REGIONS.PREV_DATE%TYPE) RETURN NUMBER;
  FUNCTION F_GET_PREPREV_DATE_REFAC (A_REGION_ID IN REGIONS.REGION_ID%TYPE,A_PREPREV_DATE OUT REGIONS.PREPREV_DATE%TYPE) RETURN NUMBER;

  FUNCTION F_GET_FX_RATES_REFAC (A_region_id REGIONS.REGION_ID%TYPE,A_RATES_RESULTSET OUT TYPE_RESULTSET) RETURN NUMBER ;
  FUNCTION F_GET_LAST_FX_RATES_REFAC (A_region_id REGIONS.REGION_ID%TYPE,A_cobdate FX_RATES.ASOFDATE%TYPE,A_RATES_RESULTSET OUT TYPE_RESULTSET) RETURN NUMBER ;
  FUNCTION F_GET_ASOFDATE_FX_RATES_REFAC (A_source_system_id SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,A_cobdate FX_RATES.ASOFDATE%TYPE,A_RATES_RESULTSET OUT TYPE_RESULTSET) RETURN NUMBER ;

  FUNCTION F_GET_FX_CURRENCIES_REFAC (A_REGION_ID REGIONS.REGION_ID%TYPE,A_ASOFDATE AGE.ASOFDATE%TYPE,A_CURRENCIES_RESULTSET OUT TYPE_RESULTSET) RETURN NUMBER ;

  FUNCTION F_INSERT_FX_RATES_REFAC (A_asofdate FX_RATES.ASOFDATE%TYPE,A_region_id REGIONS.REGION_ID%TYPE,A_CURRENCY_ID FX_RATES.CURRENCY_ID%TYPE,A_SPOT FX_RATES.SPOT%TYPE,A_fx_date FX_RATES.FX_DATE%TYPE) RETURN NUMBER ;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Checks the last load and next load dates by the region/source system given and return 1 if the data load is allowed and 0 if not.
-- Used: QlickView
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION F_GET_ALLOW_QV_LOAD_REFAC (
        p_source_system_id    IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
        p_region_id                IN SOURCE_SYSTEM.REGION_ID%TYPE,
        p_metric_id              IN VARCHAR2 DEFAULT 'IA'
  ) RETURN NUMBER;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Updated the load dates for QV by region/source_system with the given dates.
-- Used: QlickView
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PR_SET_QV_LOAD_DATES_REFAC (
    p_source_system_id    IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
    p_region_id                IN SOURCE_SYSTEM.REGION_ID%TYPE,
    p_last_load                IN SOURCE_SYSTEM_METRIC.AGE_QV_LASTLOAD%TYPE,
    p_next_load                IN SOURCE_SYSTEM_METRIC.AGE_QV_NEXTLOAD%TYPE,
    p_metric                    IN VARCHAR2 DEFAULT 'IA'
  );

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Returns the next load date for the given region/source system
-- Used: IA Hadoop processes
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION F_GET_HADOOP_LOAD_DATE_REFAC (
        p_source_system_id       IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
        p_region_id              IN SOURCE_SYSTEM.REGION_ID%TYPE,
        p_metric                  IN SOURCE_SYSTEM_METRIC.METRIC_ID%TYPE DEFAULT 'IA'
  )  RETURN TYPE_RESULSET_UNIX;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Returns the next load date for the given region/source system
-- Used: IA Hadoop processes
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION F_SET_HADOOP_LOAD_DATES_REFAC (
        p_source_system_id    IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
        p_region_id                IN SOURCE_SYSTEM.REGION_ID%TYPE,
        p_metric_id                IN VARCHAR2 DEFAULT 'IA'/*,
        p_last_load                IN SOURCE_SYSTEM.AGE_HADOOP_LASTLOAD%TYPE*/
  ) RETURN NUMBER;

  FUNCTION F_CALC_CHARGE_THRESHOLD_REFAC(p_source_system_id SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE, p_asofdate NEW_AGE.ASOFDATE%TYPE, p_region_id FX_RATES.REGION_ID%TYPE) RETURN NUMBER;
  
  FUNCTION F_GET_REPORT_TRADING_DESK_RF(p_metric_id  IN SOURCE_SYSTEM_METRIC.METRIC_ID%TYPE DEFAULT 'IA', p_asofdateIni IN AGE.ASOFDATE%TYPE, p_asofdateEnd IN AGE.ASOFDATE%TYPE) RETURN TYPE_RESULSET_UNIX;

END PKG_REFERENCE_DATA_REFAC;
