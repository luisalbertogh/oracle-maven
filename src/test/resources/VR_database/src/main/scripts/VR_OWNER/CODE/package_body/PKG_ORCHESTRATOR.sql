--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_ORCHESTRATOR runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_ORCHESTRATOR" AS

 /**************************************************************************************************************
  * Author: SERGIO.COUTO@DB.COM, IGNACIO.SALES@DB.COM
  * Date: 10/17/2016
  * 
  * Purpose: This package manages all the functions or procedures directly related to the Orchestrator component
  ***************************************************************************************************************/ 

    
-----------------------------------------------------------------------------
-- Functionality: Get the steps needed by step
------------------------------------------------------------------------------
  function F_GET_STEPS_BY_PROCESS(P_PROCESS_ID CONF_STEPS.PROCESS_ID%TYPE)  return TYPE_RESULSET is
	r_output   TYPE_RESULSET;
    V_NUMBER number(15);
    
	
  BEGIN
  
      open R_OUTPUT for
    select CF.PROCESS_ID, CF.STEP_ID, CF.STEP_INDEX, CF.STEP_SCRIPT, cf.step_params, cf.ENV_FILE, cf.ALLOWS_LIST_COBDATES, cf.ALLOWS_LIST_SOURCESYSTEMS, cf.schema_Type_code, cs.source_system_id
      from CONF_STEPS CF join CONF_PROCESSES_BY_SS CS
      on CF.PROCESS_ID = CS.PROCESS_ID and CF.STEP_ID = CS.STEP_ID
      where CF.PROCESS_ID = P_PROCESS_ID
      and CS.ACTIVE_FLG =1;
      RETURN r_output;
  
    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return null;
            RAISE;
	END F_GET_STEPS_BY_PROCESS;


-------------------------------------------------------------------------------
---- Functionality: Get datalaoders to run
--------------------------------------------------------------------------------
 function F_GET_MAX_EXECUTED_SCHEMA(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type)  return TYPE_RESULSET is
  
    r_output   TYPE_RESULSET;
  begin
  open R_OUTPUT for
      select PROCESS_ID, SOURCE_SYSTEM_ID, COBDATE, DATA_VERSION, SCHEMA_TYPE, SCHEMA_VERSION, STATUS_CODE, ACTIVE_FROM, ACTIVE_TO
      from (
      select ROW_NUMBER() over (partition by PROCESS_ID, SOURCE_SYSTEM_ID, SCHEMA_TYPE order by PROCESS_ID, schema_type, COBDATE DESC, DATA_VERSION DESC) as row_number,PROCESS_ID,  SOURCE_SYSTEM_ID, COBDATE, DATA_VERSION, SCHEMA_TYPE, SCHEMA_VERSION, STATUS_CODE, ACTIVE_FROM, ACTIVE_TO
      from CONTROL_ORCHESTRATOR) T
      where ROW_NUMBER = 1
      and PROCESS_ID = P_PROCESS_ID;
    return r_output;
  
   EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return null;
            RAISE;
  
  
  END F_GET_MAX_EXECUTED_SCHEMA;

--------------------------------------------------------------------------------
---- Functionality: Get execution schedule
--------------------------------------------------------------------------------
  function F_GET_EXEC_SCHEDULE return TYPE_RESULSET is
  r_output   TYPE_RESULSET;
  begin
    open R_OUTPUT for
    select source_system_id, day_delay, ACTIVE_FLG
    from CONF_EXECUTION_SCHEDULE;
    
    
    return r_output;
  

  
  EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return null;
            RAISE;
            
 end F_GET_EXEC_SCHEDULE;

-------------------------------------------------------------------------------
---- Functionality: insert workItem 
--------------------------------------------------------------------------------
function F_INSERT_DATE_STATUS(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_STATUS_CODE CONTROL_ORCHESTRATOR.STATUS_CODE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type) return number is
  schema_version number;
begin

     select max(SCHEMA_VERSION)
     into SCHEMA_VERSION
      from CONF_SCHEMA_VERSION
      where SCHEMA_TYPE_CODE = P_SCHEMA_TYPE;
      
    insert into CONTROL_ORCHESTRATOR (PROCESS_ID, SOURCE_SYSTEM_ID, COBDATE, DATA_VERSION, SCHEMA_TYPE, SCHEMA_VERSION, STATUS_CODE, ACTIVE_FROM) values (P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_COBDATE, P_DATA_VERSION, P_SCHEMA_TYPE, SCHEMA_VERSION, P_STATUS_CODE, sysdate);
      
    return 0;
 

 EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 1;
            RAISE;

end F_INSERT_DATE_STATUS;

-------------------------------------------------------------------------------
---- Functionality: insert workItem with pending status
--------------------------------------------------------------------------------
-- 
  function F_INSERT_PENDING_DATE (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%TYPE, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%TYPE)  return number IS

    V_NUMBER number(15);
   
    
  begin
      V_NUMBER:=F_INSERT_DATE_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_SCHEMA_TYPE, P_COBDATE, PENDING_STATUS ,1 );
      return V_NUMBER;
      
 
  
    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 2;
            RAISE;

      end F_INSERT_PENDING_DATE;
       
-------------------------------------------------------------------------------
---- Functionality: Get execution schedule
--------------------------------------------------------------------------------
     function F_UPDATE_ERROR_DATES (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type)   return NUMBER IS 
    
     V_NUMBER number(15);
    
    BEGIN
    
        update CONTROL_ORCHESTRATOR
        set STATUS_CODE = PENDING_STATUS,
        SCHEMA_VERSION =  (select max(SV.SCHEMA_VERSION)
                              from CONF_SCHEMA_VERSION SV
                               WHERE SV.SCHEMA_TYPE_CODE = SCHEMA_TYPE)
        where PROCESS_ID = P_PROCESS_ID
        and STATUS_CODE = ERROR_STATUS;
      
      if sql%ROWCOUNT < 1 then
        rollback;
        return 1;
      else
            RETURN 0;
      END IF;
      
     EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 1;
            RAISE;
    
    END F_UPDATE_ERROR_DATES;
--   
-------------------------------------------------------------------------------
---- Functionality: Get execution schedule
--------------------------------------------------------------------------------
--
   function F_GET_RUNNABLE_DATES (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type) return TYPE_RESULSET is
    
    r_output   TYPE_RESULSET;
    begin
   
      open R_OUTPUT for
      select PROCESS_ID, SOURCE_SYSTEM_ID, COBDATE, DATA_VERSION, SCHEMA_TYPE, SCHEMA_VERSION, STATUS_CODE, ACTIVE_FROM, ACTIVE_TO
      from CONTROL_ORCHESTRATOR
      where PROCESS_ID = P_PROCESS_ID 
      and (STATUS_CODE = PENDING_STATUS or STATUS_CODE = RERUN_STATUS);
    
    
      return r_output;
    
     
     EXCEPTION
        WHEN OTHERS THEN 
           --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return NULL;
            RAISE;
   
    end F_GET_RUNNABLE_DATES;
--      
-------------------------------------------------------------------------------
---- Functionality: set status
-------------------------------------------------------------------------------- 
--    
      function F_SET_STATUS (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type, P_STATUS CONTROL_ORCHESTRATOR.STATUS_CODE%type)  return number is
      V_NUMBER number(15);
      
      begin
     
      
       update CONTROL_ORCHESTRATOR
        set STATUS_CODE = P_STATUS,
            active_from = sysdate
        where SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
        and COBDATE = P_COBDATE
        and SCHEMA_TYPE = P_SCHEMA_TYPE
        AND PROCESS_ID = P_PROCESS_ID
        and data_version = p_data_version;
        
      
      IF SQL%ROWCOUNT = 1 THEN
        RETURN 0;
      ELSE
        ROLLBACK;
        return 1;
      END IF;
      
         EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 0;
            RAISE;
      end F_SET_STATUS;

--  -----------------------------------------------------------------------------
---- Functionality: set status
--------------------------------------------------------------------------------
  function F_SET_STATUS_RUNNING (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number IS
  V_NUMBER number(15);
  BEGIN
  
      V_NUMBER:=F_SET_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_COBDATE, P_SCHEMA_TYPE, P_DATA_VERSION, RUNNING_STATUS);
      return V_NUMBER;
  
   EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 0;
            RAISE;
  end F_SET_STATUS_RUNNING;
--  -----------------------------------------------------------------------------
---- Functionality: set status
--------------------------------------------------------------------------------
  function F_SET_STATUS_ERROR (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number IS
  V_NUMBER number(15);
  BEGIN
  
      V_NUMBER:=F_SET_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_COBDATE, P_SCHEMA_TYPE, P_DATA_VERSION, ERROR_STATUS);
      return V_NUMBER;
  
   EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 0;
            RAISE;
  end F_SET_STATUS_ERROR;
--  -----------------------------------------------------------------------------
---- Functionality: set status
--------------------------------------------------------------------------------
  function F_SET_STATUS_SUCCESS (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number IS
  V_NUMBER number(15);
  BEGIN
  
      V_NUMBER:=F_SET_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_COBDATE, P_SCHEMA_TYPE, P_DATA_VERSION, SUCCESS_STATUS);
      return V_NUMBER;
  
   EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 0;
            RAISE;
  end F_SET_STATUS_SUCCESS;
  
  -----------------------------------------------------------------------------
-- Functionality: check preconditions
------------------------------------------------------------------------------
 function F_CHECK_PRECONDITION (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type,  P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type)  return number is
    
      PREV_STATUS number(15);
      r_output number(15);
      V_DAILYWEEK  number(15);
      P_DATE CONTROL_ORCHESTRATOR.COBDATE%type;
 
      cursor PRECONDITIONS is
      select *
      from CONF_PRECONDITIONS
      where PROCESS_ID = P_PROCESS_ID
      and SCHEMA_TYPE = P_SCHEMA_TYPE
      and Source_system_id = P_SOURCE_SYSTEM_ID;
 
 begin
 
      r_output:=0;
      for PRECONDITION in PRECONDITIONS
      LOOP
      
        IF PRECONDITION.PREV_PROCESS_ID = P_PROCESS_ID And PRECONDITION.PREV_SCHEMA_TYPE = P_SCHEMA_TYPE THEN
            DBMS_OUTPUT.PUT_LINE('SUMMIT dependency with previous day');
            Select (To_Char(TO_DATE(P_COBDATE)-1,'D')) into V_DAILYWEEK From dual;      -- Checking if the date is part of weekend
            IF (V_DAILYWEEK = 7) THEN               -- If not we will try to insert a new record
                DBMS_OUTPUT.PUT_LINE('COBDATE-3. Friday');
                P_DATE := TO_DATE(P_COBDATE)-3;     -- If P_COBDATE is Monday we set Friday
            ELSE 
                DBMS_OUTPUT.PUT_LINE('COBDATE-1. Prevoius day');
                P_DATE := TO_DATE(P_COBDATE)-1;     -- Else we set the previous day
            END IF;

            select STATUS_CODE
            into PREV_STATUS
            from (select status_code, data_version
                  from CONTROL_ORCHESTRATOR
                  where PROCESS_ID = PRECONDITION.PREV_PROCESS_ID
                  and SCHEMA_TYPE = PRECONDITION.PREV_SCHEMA_TYPE
                  and SOURCE_SYSTEM_ID = PRECONDITION.PREV_SOURCE_SYSTEM_ID
                  and COBDATE = P_DATE
                  order by DATA_VERSION desc)  
            where ROWNUM =1;   
            DBMS_OUTPUT.PUT_LINE('The status code for previous day is ' || PREV_STATUS);
        ELSE
            
            select STATUS_CODE
            into PREV_STATUS
            from (select status_code, data_version
                  from CONTROL_ORCHESTRATOR
                  where PROCESS_ID = PRECONDITION.PREV_PROCESS_ID
                  and SCHEMA_TYPE = PRECONDITION.PREV_SCHEMA_TYPE
                  and SOURCE_SYSTEM_ID = PRECONDITION.PREV_SOURCE_SYSTEM_ID
                  and COBDATE = P_COBDATE
                  order by DATA_VERSION desc)  
            where ROWNUM =1;   
            DBMS_OUTPUT.PUT_LINE('Normal dependency with status: ' || PREV_STATUS);
        END IF;
  
        
           if (PREV_STATUS <> SUCCESS_STATUS) AND (PREV_STATUS <> MANUAL_EXECUTION_STATUS)  then
              return 1;
          end if;
      end LOOP;
      
      
      return 0;
 
    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 1;
            RAISE;
 
 end F_CHECK_PRECONDITION;
 
----------------------------------------------------------------------------- 
 -- Functionality: Get region folder by SS
------------------------------------------------------------------------------
 function F_GET_FOLDERS (P_PROCESS_ID CONF_FOLDERS.PROCESS_ID%type)  return TYPE_RESULSET is
 r_output   TYPE_RESULSET;
 begin
   open r_output for
      select *
    from CONF_FOLDERS;
    
    
    return R_OUTPUT;
    
     EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return null;
            RAISE;
    
 END F_GET_FOLDERS;    
 
-------------------------------------------------------------------------------
---- Functionality: set activeto
-------------------------------------------------------------------------------- 
--    
      function F_SET_NOT_ACTIVE (P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  return number is
      V_NUMBER number(15);
      
      begin
     
      
       update CONTROL_ORCHESTRATOR
        set active_to = sysdate
        where SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
        and COBDATE = P_COBDATE
        and SCHEMA_TYPE = P_SCHEMA_TYPE
        AND PROCESS_ID = P_PROCESS_ID
        and data_version = p_data_version;
        
      
      IF SQL%ROWCOUNT = 1 THEN
        RETURN 0;
      ELSE
        ROLLBACK;
        return 1;
      END IF;
      
         EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 0;
            RAISE;
      end F_SET_NOT_ACTIVE;



function F_INSERT_RERUN_RECORDS(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_INCLUDE_DOWNSTREAM NUMBER) return number is

      V_NUMBER  number(15);
      DATAVERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type;
      STATUSCODE CONTROL_ORCHESTRATOR.STATUS_CODE%type;

      cursor POSTCONDITIONS is
      select *
      from CONF_PRECONDITIONS
      where PREV_PROCESS_ID = P_PROCESS_ID
      and PREV_SCHEMA_TYPE = P_SCHEMA_TYPE
      and PREV_Source_system_id = P_SOURCE_SYSTEM_ID;

begin
      select nvl((
          select DATA_VERSION
          from ( select DATA_VERSION
                 from CONTROL_ORCHESTRATOR
                 where  PROCESS_ID = P_PROCESS_ID
                 and SCHEMA_TYPE = P_SCHEMA_TYPE
                 and SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
                 and COBDATE = P_COBDATE
                 order by DATA_VERSION  desc)
           where rownum =1),0) DATA_VERSION,
           
           NVL((
          select STATUS_CODE
          from ( select STATUS_CODE
                 from CONTROL_ORCHESTRATOR
                 where  PROCESS_ID = P_PROCESS_ID
                 and SCHEMA_TYPE = P_SCHEMA_TYPE
                 and SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
                 and COBDATE = P_COBDATE
                 order by DATA_VERSION  desc)
           where rownum =1),0) STATUS_CODE
           
           
      into DATAVERSION , STATUSCODE
      from dual;
        
        IF DATAVERSION=0 then
         -- INSERT VERSION 1 as pending
         DBMS_OUTPUT.PUT_LINE('Dataversion is 0. Inserting');
          V_NUMBER:=F_INSERT_DATE_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_SCHEMA_TYPE, P_COBDATE,PENDING_STATUS, 1 );
        else
        
        
            IF STATUSCODE=SUCCESS_STATUS  OR STATUSCODE=MANUAL_EXECUTION_STATUS THEN
                  -- INSERT VERSION ++ as rerun status
                  DBMS_OUTPUT.PUT_LINE('Dataversion is NOT 0. Status is success. Inserting');
                  V_NUMBER:=F_INSERT_DATE_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_SCHEMA_TYPE, P_COBDATE, RERUN_STATUS ,DATAVERSION+1 );
            ELSE
            
                  IF STATUSCODE=PENDING_STATUS then
                      -- Do Nothing - can not re-run something that is pending
                      -- V_NUMBER:=F_INSERT_DATE_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_SCHEMA_TYPE, P_COBDATE, RERUN_STATUS ,DATAVERSION+1 );
                     DBMS_OUTPUT.PUT_LINE('Nothing to do for existing pending row');
                     V_NUMBER:=0;
                  else
                    if STATUSCODE=ERROR_STATUS or STATUSCODE=IGNORE_STATUS then
                      -- UPDATE FROM ERROR OR IGNORE TO PENDING
                      DBMS_OUTPUT.PUT_LINE('Dataversion is NOT 0. Status is error or ignore. Updating');
                      V_NUMBER:=F_SET_STATUS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_COBDATE, P_SCHEMA_TYPE, DATAVERSION, PENDING_STATUS);
                    END IF;
                  END IF;
            end if;
        END IF;
		
		 IF 1 = P_INCLUDE_DOWNSTREAM  then
		  FOR POSTCONDITION in POSTCONDITIONS
		  LOOP
			  V_NUMBER:=F_INSERT_RERUN(POSTCONDITION.PROCESS_ID, POSTCONDITION.SOURCE_SYSTEM_ID, P_COBDATE, null, POSTCONDITION.SCHEMA_TYPE, P_INCLUDE_DOWNSTREAM);
		  END LOOP; 
		 END IF;
        
      return V_NUMBER;
      
end F_INSERT_RERUN_RECORDS;

-----------------------------------------------------------------------------
-- Functionality: insert reruns
------------------------------------------------------------------------------
function F_INSERT_RERUN(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE_FROM CONTROL_ORCHESTRATOR.COBDATE%type, P_COBDATE_TO CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_INCLUDE_DOWNSTREAM NUMBER) return number is
      V_NUMBER  number(15);
      V_DAILYWEEK  number(15);
      P_DATE CONTROL_ORCHESTRATOR.COBDATE%type;

begin
      IF (P_COBDATE_FROM is not null) THEN
          --Check if we have and end date
          IF (P_COBDATE_TO is null) THEN
                V_NUMBER := F_INSERT_RERUN_RECORDS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_COBDATE_FROM, P_SCHEMA_TYPE, P_INCLUDE_DOWNSTREAM);
          ELSE
              IF (P_COBDATE_FROM <= P_COBDATE_TO) THEN
                  P_DATE := P_COBDATE_FROM;
                  LOOP
                      Select (To_Char(P_DATE,'D')) into V_DAILYWEEK From dual;      -- Checking if the date is part of weekend
                      IF (V_DAILYWEEK <> 7 and V_DAILYWEEK <> 6) THEN               -- If not we will try to insert a new record
                          DBMS_OUTPUT.PUT_LINE('Calling insert rerun records');
                          V_NUMBER := F_INSERT_RERUN_RECORDS(P_PROCESS_ID, P_SOURCE_SYSTEM_ID, P_DATE, P_SCHEMA_TYPE, P_INCLUDE_DOWNSTREAM);
                      END IF;
                      P_DATE := P_DATE + 1;
                      EXIT WHEN (P_DATE > P_COBDATE_TO);
                  END LOOP;
              ELSE
                  DBMS_OUTPUT.PUT_LINE('Cobdate_from higher than cobdate_to.');
                  V_NUMBER := 1;
              END IF;
          END IF;
      ELSE
          DBMS_OUTPUT.PUT_LINE('Cobdate_from is null.');
          V_NUMBER := 1;
      END IF;
      
      return V_NUMBER;
      
 EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            return 1;
            RAISE;
end F_INSERT_RERUN;

-----------------------------------------------------------------------------
-- Functionality: get the lineage for a data set
------------------------------------------------------------------------------
  procedure P_GET_LINEAGE(P_PROCESS_ID CONTROL_ORCHESTRATOR.PROCESS_ID%type, P_SOURCE_SYSTEM_ID CONTROL_ORCHESTRATOR.SOURCE_SYSTEM_ID%type, P_COBDATE CONTROL_ORCHESTRATOR.COBDATE%type, P_SCHEMA_TYPE CONTROL_ORCHESTRATOR.SCHEMA_TYPE%type, P_DATA_VERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type)  is
   R_OUTPUT   TYPE_RESULSET;
   ACTIVEFROM CONTROL_ORCHESTRATOR.ACTIVE_FROM%type;
   ACTIVETO CONTROL_ORCHESTRATOR.ACTIVE_TO%type;
   DATAVERSION CONTROL_ORCHESTRATOR.DATA_VERSION%type;
   SCHEMAVERSION CONTROL_ORCHESTRATOR.SCHEMA_VERSION%TYPE;


    ACTIVEFROMPREV CONTROL_ORCHESTRATOR.ACTIVE_FROM%type;
    ACTIVETOPREV CONTROL_ORCHESTRATOR.ACTIVE_TO%type;
    DATAVERSIONPREV CONTROL_ORCHESTRATOR.DATA_VERSION%type;
   SCHEMAVERSIONPREV CONTROL_ORCHESTRATOR.SCHEMA_VERSION%TYPE;
    
    
    cursor PRECONDITIONS is
    select *
    from CONF_PRECONDITIONS
    where PROCESS_ID = P_PROCESS_ID
    and SCHEMA_TYPE = P_SCHEMA_TYPE
    and SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID;
    
  
  
  begin 
  
      --SYS.DBMS_OUTPUT.PUT_LINE(' NEW call =>P_PROCESS_ID: ' ||P_PROCESS_ID ||'| P_SOURCE_SYSTEM_ID: ' ||P_SOURCE_SYSTEM_ID ||'| P_COBDATE: ' ||P_COBDATE||'| DATAVERSION: ' ||P_DATA_VERSION||'| P_SCHEMA_TYPE: ' ||P_SCHEMA_TYPE);
      select  SCHEMA_VERSION, ACTIVE_FROM, ACTIVE_TO
      INTO SCHEMAVERSION, ACTIVEFROM, ACTIVETO
      from CONTROL_ORCHESTRATOR
      where PROCESS_ID = P_PROCESS_ID
      and SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
      and COBDATE = P_COBDATE
      and SCHEMA_TYPE = P_SCHEMA_TYPE
      and DATA_VERSION = P_DATA_VERSION;
    
     
    
      FOR PRECONDITION IN PRECONDITIONS
      LOOP
   
        
        
        --SYS.DBMS_OUTPUT.PUT_LINE('P_PROCESS_ID: ' ||PRECONDITION.PREV_PROCESS_ID ||'| P_SOURCE_SYSTEM_ID: ' ||PRECONDITION.PREV_SOURCE_SYSTEM_ID ||'| P_COBDATE: ' ||P_COBDATE || 'PRECONDITION.PREV_SCHEMA_TYPE: ' || PRECONDITION.PREV_SCHEMA_TYPE || 'ACTIVEFROM: ' || ACTIVEFROM || 'ACTIVEto: ' || ACTIVETO);
        select DATA_VERSION, SCHEMA_VERSION, ACTIVE_FROM, ACTIVE_TO
        INTO DATAVERSIONPREV, SCHEMAVERSIONPREV, ACTIVEFROMPREV, ACTIVETOPREV
        from CONTROL_ORCHESTRATOR
        where PROCESS_ID = PRECONDITION.PREV_PROCESS_ID
        and SOURCE_SYSTEM_ID = PRECONDITION.PREV_SOURCE_SYSTEM_ID
        and COBDATE = P_COBDATE
        and SCHEMA_TYPE = PRECONDITION.PREV_SCHEMA_TYPE
        and ACTIVE_FROM <= ACTIVEFROM 
        and (ACTIVE_TO is null or (ACTIVE_TO >= ACTIVEFROM   AND (ACTIVETO is null or active_to <= ACTIVETO)));
        
        
        SYS.DBMS_OUTPUT.PUT_LINE('P_PROCESS_ID: ' ||P_PROCESS_ID ||'| P_SOURCE_SYSTEM_ID: ' ||P_SOURCE_SYSTEM_ID ||'| P_COBDATE: ' ||P_COBDATE||'| DATAVERSION: ' ||P_DATA_VERSION||'| P_SCHEMA_TYPE: ' ||P_SCHEMA_TYPE||'| SCHEMAVERSION: ' ||SCHEMAVERSION||'| ACTIVEFROM: ' ||ACTIVEFROM ||'| ACTIVETO: ' ||ACTIVETO ||'| PRECONDITION.PREV_PROCESS_ID: ' ||PRECONDITION.PREV_PROCESS_ID ||'| PRECONDITION.PREV_SOURCE_SYSTEM_ID: ' ||PRECONDITION.PREV_SOURCE_SYSTEM_ID ||' P_COBDATE : ' ||P_COBDATE||'| DATAVERSIONPREV: ' ||DATAVERSIONPREV||'| PRECONDITION.PREV_SCHEMA_TYPE: ' ||PRECONDITION.PREV_SCHEMA_TYPE||'| SCHEMAVERSIONPREV: ' ||SCHEMAVERSIONPREV||'| ACTIVEFROMPREV: ' ||ACTIVEFROMPREV || '| ACTIVETOPREV: ' || ACTIVETOPREV);
      
         P_GET_LINEAGE(PRECONDITION.PREV_PROCESS_ID, PRECONDITION.PREV_SOURCE_SYSTEM_ID, P_COBDATE,  PRECONDITION.PREV_SCHEMA_TYPE, DATAVERSIONPREV);
      END LOOP;
  
  
  
  
  
  
    open r_output for
    select * from
    control_orchestrator;
  

    EXCEPTION
        WHEN OTHERS THEN 
            --v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DATA_INGESTION_STRING,null,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK, NULL);
            SYS.DBMS_OUTPUT.PUT_LINE('PROCESS END NO MORE DATA AVAILABLE');
            --RAISE;
  end P_GET_LINEAGE;
  

    
END PKG_ORCHESTRATOR;

--GRANT DEBUG ON PKG_ORCHESTRATOR TO ${vr_read_role};
--GRANT EXECUTE ON PKG_ORCHESTRATOR TO ${vr_update_role};
