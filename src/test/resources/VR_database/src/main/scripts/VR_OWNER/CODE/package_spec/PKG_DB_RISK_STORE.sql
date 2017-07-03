--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_DB_RISK_STORE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_DB_RISK_STORE" AS

/**************************************************************************************************************
  * Autor: MARINO.SERNA-SANNAMED@DB.COM
  * Date: 30/10/2015
  *
  * Purpose: This package manage all the related functions or procedures directly related to DB Risk Store reruns of the process/jobs

  FUNCTION F_GET_PARAMS_FOR_SS_FACTOR			--> Function to get the the params by source system and factor
 
  ***************************************************************************************************************/

TYPE TYPE_RESULSET IS REF CURSOR;
DB_RISK_STORE_STRING varchar2(15) := 'DBRISK_STORE';
DB_RISK_STORE_ERROR_STRING varchar2(20) := 'DBRISK_STORE ERROR';
ALL_SS_STRING varchar2(5) := 'ALL';

-----------------------------------------------------------------------------
-- Functionality: Get the params values by factor
-- Used: dbRisk Store
------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS_FOR_FACTOR ( 
	p_factor_name DB_RISK_STORE_CONF.FACTOR_NAME%TYPE
	) RETURN TYPE_RESULSET;
    
	-----------------------------------------------------------------------------
-- Functionality: Get the params values
-- Used: dbRisk Store
------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS RETURN TYPE_RESULSET;
  
-----------------------------------------------------------------------------
-- Functionality: Insert a riskFactor in the DB_RISK_STORE_CONF table
-- Used: com.db.volcker.dbrisk.configPublisher
------------------------------------------------------------------------------
   PROCEDURE PR_INSERT_RISKFACTORS (      
      p_factor_name DB_RISK_STORE_CONF.FACTOR_NAME%TYPE,
      p_factor_position DB_RISK_STORE_CONF.FACTOR_POSITION%TYPE,
      p_context DB_RISK_STORE_CONF.CONTEXT%TYPE,
      p_exposure DB_RISK_STORE_CONF.EXPOSURE%TYPE,
      p_data_type DB_RISK_STORE_CONF.DATA_TYPE%TYPE,
      p_scenario_type DB_RISK_STORE_CONF.SCENARIO_TYPE%TYPE,
      p_sensitivity DB_RISK_STORE_CONF.SENSITIVITY%TYPE,
      p_sensitivity_category DB_RISK_STORE_CONF.SENSITIVITY_CATEGORY%TYPE,
      p_sensitivity_subcategory DB_RISK_STORE_CONF.SENSITIVITY_SUBCATEGORY%TYPE,
      p_definition_id_filtering DB_RISK_STORE_CONF.DEFINITION_ID_FILTERING%TYPE,
      p_report_name DB_RISK_STORE_CONF.REPORT_NAME%TYPE,
      p_other_parameters DB_RISK_STORE_CONF.OTHER_PARAMETERS%TYPE,
      p_cobdate VARCHAR2          
   );  
   
-----------------------------------------------------------------------------
-- Functionality: Get the params values by isofinterest and max cobdate
-- Used: dbRisk Store
------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS_FOR_DATE_INTERES RETURN TYPE_RESULSET; 
  
 
 -----------------------------------------------------------------------------
  -- Functionality: Get the params with isofinterest='YES' for max cobdate with overriden values
  -- Used: dbRisk Store
  ------------------------------------------------------------------------------
 FUNCTION F_GET_PARAMS_OVERRIDE RETURN TYPE_RESULSET;
 
 	-----------------------------------------------------------------------------
-- Functionality: Get the desk param values
-- Used: dbRisk TradeDetails
------------------------------------------------------------------------------
  FUNCTION F_GET_DESK_PARAMS (p_cobdate VARCHAR2) RETURN TYPE_RESULSET;
 
  END PKG_DB_RISK_STORE;

