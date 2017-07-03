--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_ORCHESTRATOR runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_ORCHESTRATOR" AS 

  /**************************************************************************************************************
  * Author: SERGIO.COUTO@DB.COM, IGNACIO.SALES@DB.COM
  * Date: 10/17/2016
  * 
  * Purpose: This package manages all the functions or procedures directly related to the Orchestrator component
  * 
  ***************************************************************************************************************/  
  
  type TYPE_RESULSET is ref cursor;

  PENDING_STATUS CONF_STATUS.STATUS_CODE%type := 0;
  RUNNING_STATUS CONF_STATUS.STATUS_CODE%type := 1;
  SUCCESS_STATUS CONF_STATUS.STATUS_CODE%type := 2;
  ERROR_STATUS CONF_STATUS.STATUS_CODE%type := 3;
  IGNORE_STATUS CONF_STATUS.STATUS_CODE%TYPE := 4;
  RERUN_STATUS CONF_STATUS.STATUS_CODE%TYPE := 5;
  MANUAL_EXECUTION_STATUS CONF_STATUS.STATUS_CODE%TYPE := 6;

-----------------------------------------------------------------------------
-- Functionality: Get the steps needed by step
------------------------------------------------------------------------------
  FUNCTION F_GET_STEPS_BY_PROCESS(P_PROCESS_ID CONF_STEPS.PROCESS_ID%TYPE)  RETURN TYPE_RESULSET;
 
-----------------------------------------------------------------------------
-- Functionality: Get datalaoders to run
------------------------------------------------------------------------------
 function F_GET_MAX_EXECUTED_SCHEMA(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type)  return TYPE_RESULSET; 
  
-----------------------------------------------------------------------------
-- Functionality: Get execution schedule
------------------------------------------------------------------------------
  function F_GET_EXEC_SCHEDULE   return  TYPE_RESULSET;
  
  
-------------------------------------------------------------------------------
---- Functionality: insert workItem 
--------------------------------------------------------------------------------
  function F_INSERT_DATE_STATUS(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_STATUS_CODE CONTROL_ORCHESTRATOR.STATUS_CODE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type) return number ;
  
-----------------------------------------------------------------------------
-- Functionality: Get execution schedule
------------------------------------------------------------------------------
  function F_INSERT_PENDING_DATE (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%TYPE, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%TYPE)  return number ;
-----------------------------------------------------------------------------
-- Functionality: Get execution schedule
------------------------------------------------------------------------------
 function F_UPDATE_ERROR_DATES (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type)  return number; 

-----------------------------------------------------------------------------
-- Functionality: Get execution schedule
------------------------------------------------------------------------------
 function F_GET_RUNNABLE_DATES (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type)  return TYPE_RESULSET; 

-----------------------------------------------------------------------------
-- Functionality: set status
------------------------------------------------------------------------------
  function F_SET_STATUS (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, p_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, p_schema_type CONTROL_ORCHESTRATOR.SCHEMA_TYPE%TYPE, p_data_version CONTROL_ORCHESTRATOR.DATA_VERSION%TYPE, p_status CONTROL_ORCHESTRATOR.STATUS_CODE%TYPE)  return NUMBER;
  
-----------------------------------------------------------------------------
-- Functionality: set status
------------------------------------------------------------------------------
  function F_SET_STATUS_RUNNING (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number;

-----------------------------------------------------------------------------
-- Functionality: set status
------------------------------------------------------------------------------
  function F_SET_STATUS_ERROR (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number;
  -----------------------------------------------------------------------------
-- Functionality: set status
------------------------------------------------------------------------------
 function F_SET_STATUS_SUCCESS (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number;
  
-----------------------------------------------------------------------------
-- Functionality: check preconditions
------------------------------------------------------------------------------
 function F_CHECK_PRECONDITION (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type,  P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type)  return number;
  
----------------------------------------------------------------------------- 
 -- Functionality: Get region folder by SS
------------------------------------------------------------------------------
 function F_GET_FOLDERS (P_PROCESS_ID CONF_FOLDERS.PROCESS_ID%type)  return TYPE_RESULSET; 
 
 -----------------------------------------------------------------------------
-- Functionality: set status
------------------------------------------------------------------------------
  function F_SET_NOT_ACTIVE (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number;

-----------------------------------------------------------------------------
-- Functionality: insert reruns
------------------------------------------------------------------------------

function F_INSERT_RERUN_RECORDS(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_INCLUDE_DOWNSTREAM NUMBER) return number;

-----------------------------------------------------------------------------
-- Functionality: insert reruns
------------------------------------------------------------------------------
  function F_INSERT_RERUN(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE_FROM CONTROL_ORCHESTRATOR.COBDATE%type, P_COBDATE_TO CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_INCLUDE_DOWNSTREAM NUMBER) return number;
  
-----------------------------------------------------------------------------
-- Functionality: get the lineage for a data set
------------------------------------------------------------------------------
  procedure P_GET_LINEAGE(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type);
  
               
END PKG_ORCHESTRATOR;
