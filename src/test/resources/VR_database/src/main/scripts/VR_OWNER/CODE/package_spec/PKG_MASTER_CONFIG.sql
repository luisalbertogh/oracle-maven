--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_MASTER_CONFIG runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


CREATE OR REPLACE PACKAGE PKG_MASTER_CONFIG as

 /**************************************************************************************************************
  ||
  || Date: 27/08/2013
  || 
  || This package manage all the related functions or procedures of Parameters Master
  ||  F_GET_PARAM_VALUE             --> Funtion to get tha PARAM_VALUE of the MASTER table with PARAM_KEY as an argument
   ***************************************************************************************************************/
 
  TYPE TYPE_RESULTSET IS REF CURSOR;
  TYPE TYPE_RESULSET_OOZIE IS REF CURSOR;
  
  FUNCTION F_GET_PARAM_VALUE (A_PARAM_KEY IN MASTER_PARAM.PARAM_KEY%TYPE,A_PARAM_VALUE OUT MASTER_PARAM.PARAM_VALUE%TYPE, A_METRIC_ID IN VARCHAR2 DEFAULT 'IA') RETURN NUMBER;
  
  FUNCTION F_GET_OOZIE_CONSTANTS (p_source_system_id SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE, A_METRIC_ID IN VARCHAR2 DEFAULT 'IA') RETURN TYPE_RESULSET_OOZIE;

END PKG_MASTER_CONFIG;
