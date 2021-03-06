--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_MONITORING runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_MONITORING" AS

    FUNCTION F_INSERT_LOG_JOBS (A_JOB_ID JOBS.JOB_ID%TYPE,A_REGION_ID REGIONS.REGION_ID%TYPE,A_SOURCE_SYSTEM_ID SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,A_COB_DATE LOG_JOBS.COB_DATE%TYPE,A_COMPONENT_NAME LOG_JOBS.COMPONENT_NAME%TYPE,A_STATUS LOG_JOBS.STATUS%TYPE,A_SEVERITY LOG_JOBS.SEVERITY%TYPE,A_MESSAGE LOG_JOBS.MESSAGE%TYPE,A_COMMENTS LOG_JOBS.COMMENTS%TYPE, A_METRIC_ID VARCHAR2 DEFAULT 'IA') RETURN NUMBER IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
            INSERT INTO LOG_JOBS(METRIC_ID, JOB_ID, SOURCE_SYSTEM_ID, COB_DATE, COMPONENT_NAME, STATUS, SEVERITY, MESSAGE, COMMENTS)
            VALUES (UPPER(A_METRIC_ID), A_JOB_ID, A_SOURCE_SYSTEM_ID, A_COB_DATE, A_COMPONENT_NAME, A_STATUS, A_SEVERITY, A_MESSAGE, A_COMMENTS);
        
        COMMIT;
        
        RETURN 0; 
        
    EXCEPTION
        WHEN OTHERS THEN 
            dbms_output.put_line(SQLCODE);
            RETURN 1;
    END;
    
    
    PROCEDURE PR_INSERT_LOG_JOBS_QV (A_JOB_ID JOBS.JOB_ID%TYPE,A_REGION_ID REGIONS.REGION_ID%TYPE,A_SOURCE_SYSTEM_ID SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,A_COB_DATE LOG_JOBS.COB_DATE%TYPE,A_COMPONENT_NAME LOG_JOBS.COMPONENT_NAME%TYPE,A_STATUS LOG_JOBS.STATUS%TYPE,A_SEVERITY LOG_JOBS.SEVERITY%TYPE,A_MESSAGE LOG_JOBS.MESSAGE%TYPE,A_COMMENTS LOG_JOBS.COMMENTS%TYPE, A_METRIC_ID VARCHAR2 DEFAULT 'IA')
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN        
         
            INSERT INTO LOG_JOBS(METRIC_ID, JOB_ID, SOURCE_SYSTEM_ID, COB_DATE, COMPONENT_NAME, STATUS, SEVERITY, MESSAGE, COMMENTS)
            VALUES (UPPER(A_METRIC_ID), A_JOB_ID, A_SOURCE_SYSTEM_ID, A_COB_DATE, A_COMPONENT_NAME, A_STATUS, A_SEVERITY, A_MESSAGE, A_COMMENTS);
        
        COMMIT;                       
    EXCEPTION
        WHEN OTHERS THEN 
		dbms_output.put_line(SQLCODE);
            RAISE;
    END PR_INSERT_LOG_JOBS_QV;
 

END PKG_MONITORING;
