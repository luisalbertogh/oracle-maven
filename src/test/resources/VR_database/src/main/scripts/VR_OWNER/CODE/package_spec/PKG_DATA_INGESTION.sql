--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_DATA_INGESTION runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_DATA_INGESTION" AS 

  /**************************************************************************************************************
  * Autor: SERGIO.COUTO@DB.COM
  * Date: 07/01/2016
  * 
  * Purpose: This package manage all the related functions or procedures directly related to data ingestion process
  * 
  * 
  * FUNCTION F_GET_CONFIGURATION_POSITIONS			--> Function to retrieve positions configuration
  * FUNCTION F_GET_CONFIGURATION_TRADES			    --> Function to retrieve trades configuration
  * FUNCTION F_GET_CONFIGURATION_ADJUST		--> Function to retrieve adjustments configuration
  * FUNCTION F_GET_CONFIGURATION_OTHERS			 	--> Function to retrieve other data type configuration

  * 
  ***************************************************************************************************************/  
  
  TYPE TYPE_RESULSET IS REF CURSOR;
  TRADES_STRING RAW_INGESTION_CONFIG.INPUT_FILE_TYPE%TYPE := 'TRADES';
  POSITIONS_STRING RAW_INGESTION_CONFIG.INPUT_FILE_TYPE%TYPE := 'POSITIONS';
  ADJUSTMENTS_STRING RAW_INGESTION_CONFIG.INPUT_FILE_TYPE%TYPE := 'ADJUSTMENTS';
  DATA_INGESTION_STRING VARCHAR2(50) := 'DATA_INGESTION';



-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR POSITIONS
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_POSITIONS(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE)  RETURN TYPE_RESULSET;
  
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR TRADES
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_TRADES(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE)  RETURN TYPE_RESULSET;
  
  -----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR ADJUSTMENTS
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_ADJUST(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE)  RETURN TYPE_RESULSET;
  
  -----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR OTHER DATA TYPE
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_OTHERS(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE, p_type RAW_INGESTION_CONFIG.INPUT_FILE_TYPE%TYPE)  RETURN TYPE_RESULSET;
  
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR COMPONENTS
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_COMPONENTS (p_component_id COMPONENT_CONFIG.COMPONENT_ID%TYPE, p_exec_date COMPONENT_CONFIG.INIT_DATE%TYPE) RETURN TYPE_RESULSET;
  
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR COMPONENTS LISTS
------------------------------------------------------------------------------
 FUNCTION F_GET_CONFIG_COMPONENT_LIST (p_component_id COMPONENT_CONFIG.COMPONENT_ID%TYPE, p_exec_date COMPONENT_CONFIG.INIT_DATE%TYPE, p_property COMPONENT_CONFIG_LIST.PROPERTY_VALUE%TYPE) RETURN TYPE_RESULSET;

               
END PKG_DATA_INGESTION;
