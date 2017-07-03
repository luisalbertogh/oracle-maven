--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_DISCARDS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_DISCARDS" AS 
  /**************************************************************************************************************
  * Autor: MIGUEL.FABRIQUE@DB.COM
  * Date: 27/09/2016
  * 
  * Purpose: This package manage all the related functions or procedures directly related to data ingestion process
  * 
  * 
  * FUNCTION F_GET_INDEXES_ENRICHER            --> Function to retrieve indexes values
  * 
  ***************************************************************************************************************/  
  
  TYPE TYPE_RESULSET IS REF CURSOR;
  
-----------------------------------------------------------------------------
-- Functionality: Get the indexes from database
------------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Functionality: Get the CONFIGURATION FOR COMPONENTS
------------------------------------------------------------------------------
  FUNCTION F_GET_INDEXES_ENRICHER (p_source_system_id CONF_DQ_ENRICHER.SOURCE_SYSTEM_ID%TYPE) RETURN TYPE_RESULSET;

END PKG_DISCARDS;
