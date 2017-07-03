--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_MONITORING runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_MONITORING" AS

 /**************************************************************************************************************
  ||
  || Autor: BELLJOR
  || Date: 25/07/2013
  || 
  || This package manage all the related functions or procedures directly related to monitoring of the process/jobs
  ||  F_INSERT_LOG_JOBS    --> Funtion to inert data into LOG_JOBS table
  
 ***************************************************************************************************************/
  
    FUNCTION F_INSERT_LOG_JOBS (A_JOB_ID JOBS.JOB_ID%TYPE,A_REGION_ID REGIONS.REGION_ID%TYPE,A_SOURCE_SYSTEM_ID SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,A_COB_DATE LOG_JOBS.COB_DATE%TYPE,A_COMPONENT_NAME LOG_JOBS.COMPONENT_NAME%TYPE,A_STATUS LOG_JOBS.STATUS%TYPE,A_SEVERITY LOG_JOBS.SEVERITY%TYPE,A_MESSAGE LOG_JOBS.MESSAGE%TYPE,A_COMMENTS LOG_JOBS.COMMENTS%TYPE, A_METRIC_ID VARCHAR2 DEFAULT 'IA') RETURN NUMBER;
    
    PROCEDURE PR_INSERT_LOG_JOBS_QV (A_JOB_ID JOBS.JOB_ID%TYPE,A_REGION_ID REGIONS.REGION_ID%TYPE,A_SOURCE_SYSTEM_ID SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,A_COB_DATE LOG_JOBS.COB_DATE%TYPE,A_COMPONENT_NAME LOG_JOBS.COMPONENT_NAME%TYPE,A_STATUS LOG_JOBS.STATUS%TYPE,A_SEVERITY LOG_JOBS.SEVERITY%TYPE,A_MESSAGE LOG_JOBS.MESSAGE%TYPE,A_COMMENTS LOG_JOBS.COMMENTS%TYPE, A_METRIC_ID VARCHAR2 DEFAULT 'IA');

end PKG_MONITORING;
