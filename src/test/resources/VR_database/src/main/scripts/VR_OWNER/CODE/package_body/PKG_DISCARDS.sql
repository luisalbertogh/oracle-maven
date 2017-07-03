--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_DISCARDS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_DISCARDS" 
AS 

  /**************************************************************************************************************
  * Autor: MIGUEL.FABRIQUE@DB.COM
  * Date: 27/09/2016
  *
  * Purpose: This package manage all the related functions or procedures directly related to discards
  ***************************************************************************************************************/
  
-----------------------------------------------------------------------------
-- Functionality: Get the indexes from database
------------------------------------------------------------------------------
FUNCTION F_GET_INDEXES_ENRICHER(
    p_source_system_id CONF_DQ_ENRICHER.SOURCE_SYSTEM_ID%TYPE)
  RETURN TYPE_RESULSET
IS
  r_output TYPE_RESULSET;
  v_number NUMBER(15);
BEGIN
  OPEN r_output FOR SELECT PRIM_INDEXES, SEC_INDEXES, INDEXES_INFO FROM CONF_DQ_ENRICHER WHERE SOURCE_SYSTEM_ID = p_source_system_id;
  RETURN r_output;
EXCEPTION
WHEN OTHERS THEN
  --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(p_source_system_id,null,p_source_system_id,sysdate,'F_GET_INDEXES_ENRICHER','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
  RETURN NULL;
  RAISE;
END F_GET_INDEXES_ENRICHER;
END PKG_DISCARDS;
