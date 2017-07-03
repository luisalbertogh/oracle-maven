--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_DATA_INGESTION runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_DATA_INGESTION" AS

 /**************************************************************************************************************
  * Autor: SERGIO.COUTO@DB.COM
  * Date: 07/01/2016
  * 
  * Purpose: This package manage all the related functions or procedures directly related to data ingestion process
  ***************************************************************************************************************/ 

    
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR POSITIONS
------------------------------------------------------------------------------
 FUNCTION F_GET_CONFIGURATION_POSITIONS (p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE) RETURN TYPE_RESULSET IS
  
    r_output   TYPE_RESULSET;
    v_number number(15);
  
   BEGIN
   
        
        OPEN r_output FOR
            SELECT  AUD_MOD_DATE
                    ,SOURCE_SYSTEM_ID
                    ,INPUT_FILE_TYPE
                    ,INPUT_FIELD_NAME
                    ,INPUT_FIELD_NUMBER
                    --,INPUT_FIELD_DESC
                    ,DEST_FILE_TYPE
                    ,DEST_FIELD
                    ,INPUT_DEFAULT_VALUE
                    ,OUTPUT_FIELD_NAME
                    ,OUTPUT_FILE_TYPE
                    ,OUTPUT_DEFAULT_VALUE
            FROM    RAW_INGESTION_CONFIG 
            WHERE SOURCE_SYSTEM_ID = p_source_system_id   
            AND INPUT_FILE_TYPE = POSITIONS_STRING
            AND INPUT_FIELD_NUMBER >= 0
            ORDER BY INPUT_FIELD_NUMBER ASC;
            
        RETURN r_output;

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            RETURN NULL;
            RAISE;
 END F_GET_CONFIGURATION_POSITIONS;
     
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR TRADES
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_TRADES(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE)  RETURN TYPE_RESULSET IS
  
    r_output   TYPE_RESULSET;
    v_number number(15);
  
   BEGIN
   
        
        OPEN r_output FOR
            SELECT  AUD_MOD_DATE
                    ,SOURCE_SYSTEM_ID
                    ,INPUT_FILE_TYPE
                    ,INPUT_FIELD_NAME
                    ,INPUT_FIELD_NUMBER
                    --,INPUT_FIELD_DESC
                    ,DEST_FILE_TYPE
                    ,DEST_FIELD
                    ,INPUT_DEFAULT_VALUE
                    ,OUTPUT_FIELD_NAME
                    ,OUTPUT_FILE_TYPE
                    ,OUTPUT_DEFAULT_VALUE
            FROM    RAW_INGESTION_CONFIG 
            WHERE SOURCE_SYSTEM_ID = p_source_system_id   
            AND INPUT_FILE_TYPE = TRADES_STRING
            AND INPUT_FIELD_NUMBER >= 0
            ORDER BY INPUT_FIELD_NUMBER ASC;
            
        RETURN r_output;

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            RETURN NULL;
            RAISE;
 END F_GET_CONFIGURATION_TRADES;
 
 -----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR ADJUSTMENTS
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_ADJUST(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE)  RETURN TYPE_RESULSET IS
  
    r_output   TYPE_RESULSET;
    v_number number(15);
  
   BEGIN
   
        
        OPEN r_output FOR
            SELECT  AUD_MOD_DATE
                    ,SOURCE_SYSTEM_ID
                    ,INPUT_FILE_TYPE
                    ,INPUT_FIELD_NAME
                    ,INPUT_FIELD_NUMBER
                    --,INPUT_FIELD_DESC
                    ,DEST_FILE_TYPE
                    ,DEST_FIELD
                    ,INPUT_DEFAULT_VALUE
                    ,OUTPUT_FIELD_NAME
                    ,OUTPUT_FILE_TYPE
                    ,OUTPUT_DEFAULT_VALUE
            FROM    RAW_INGESTION_CONFIG 
            WHERE SOURCE_SYSTEM_ID = p_source_system_id   
            AND INPUT_FILE_TYPE = ADJUSTMENTS_STRING
            AND INPUT_FIELD_NUMBER >= 0
            ORDER BY INPUT_FIELD_NUMBER ASC;
            
        RETURN r_output;

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            RETURN NULL;
            RAISE;
 END F_GET_CONFIGURATION_ADJUST;
 
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR OTHER DATA TYPE
------------------------------------------------------------------------------
  FUNCTION F_GET_CONFIGURATION_OTHERS(p_source_system_id RAW_INGESTION_CONFIG.SOURCE_SYSTEM_ID%TYPE, p_type RAW_INGESTION_CONFIG.INPUT_FILE_TYPE%TYPE)  RETURN TYPE_RESULSET IS
  
    r_output   TYPE_RESULSET;
    v_number number(15);
  
   BEGIN
   
        
        OPEN r_output FOR
            SELECT  AUD_MOD_DATE
                    ,SOURCE_SYSTEM_ID
                    ,INPUT_FILE_TYPE
                    ,INPUT_FIELD_NAME
                    ,INPUT_FIELD_NUMBER
                    --,INPUT_FIELD_DESC
                    ,DEST_FILE_TYPE
                    ,DEST_FIELD
                    ,INPUT_DEFAULT_VALUE
                    ,OUTPUT_FIELD_NAME
                    ,OUTPUT_FILE_TYPE
                    ,OUTPUT_DEFAULT_VALUE
            FROM    RAW_INGESTION_CONFIG 
            WHERE SOURCE_SYSTEM_ID = p_source_system_id   
            AND INPUT_FILE_TYPE = p_type
            AND INPUT_FIELD_NUMBER >= 0
            ORDER BY INPUT_FIELD_NUMBER ASC;
            
        RETURN r_output;

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            RETURN NULL;
            RAISE;
 END F_GET_CONFIGURATION_OTHERS;
 
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR COMPONENTS
------------------------------------------------------------------------------
 FUNCTION F_GET_CONFIGURATION_COMPONENTS (p_component_id COMPONENT_CONFIG.COMPONENT_ID%TYPE, p_exec_date COMPONENT_CONFIG.INIT_DATE%TYPE) RETURN TYPE_RESULSET IS
  
    r_output   TYPE_RESULSET;
    v_number number(15);
  
   BEGIN
   
        OPEN r_output FOR
                select PROPERTY_KEY, PROPERTY_VALUE from component_config
                where COMPONENT_ID = p_component_id
                and INIT_DATE <= p_exec_date 
                and (END_DATE is NULL OR END_DATE >= p_exec_date) 
                and IS_LIST = 0;
            
        RETURN r_output;

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(p_component_id,null,p_component_id,sysdate,'F_GET_CONFIGURATION_COMPONENTS','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            RETURN NULL;
            RAISE;
 END F_GET_CONFIGURATION_COMPONENTS;
  
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR COMPONENTS
------------------------------------------------------------------------------
 FUNCTION F_GET_CONFIG_COMPONENT_LIST (p_component_id COMPONENT_CONFIG.COMPONENT_ID%TYPE, p_exec_date COMPONENT_CONFIG.INIT_DATE%TYPE, p_property COMPONENT_CONFIG_LIST.PROPERTY_VALUE%TYPE) RETURN TYPE_RESULSET IS
  
    r_output   TYPE_RESULSET;
    v_number number(15);
  
   BEGIN
   
        OPEN r_output FOR
               select clist.PROPERTY_VALUE from component_config_List clist 
                INNER JOIN component_config cc ON cc.COMPONENT_ID = clist.COMPONENT_ID AND cc.PROPERTY_VALUE = clist.ID_LIST  
                where cc.INIT_DATE <= p_exec_date
                and (cc.END_DATE IS NULL OR cc.END_DATE >= p_exec_date)
                and clist.INIT_DATE <= p_exec_date
                and (clist.END_DATE IS NULL OR clist.END_DATE >= p_exec_date)
                and cc.IS_LIST = 1
                and cc.COMPONENT_ID = p_component_id
                and cc.PROPERTY_VALUE = p_property;
            
        RETURN r_output;

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(p_component_id,null,p_property,sysdate,'F_GET_CONFIGURATION_COMPONENTS','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            RETURN NULL;
            RAISE;
 END F_GET_CONFIG_COMPONENT_LIST;
 
END PKG_DATA_INGESTION;
