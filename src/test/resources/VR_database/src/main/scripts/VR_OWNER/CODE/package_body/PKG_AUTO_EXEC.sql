--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_AUTO_EXEC runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_AUTO_EXEC" 
AS
  /**************************************************************************************************************
  * Autor: SERGIO.COUTO@DB.COM
  * Date: 14/10/2014
  *
  * Purpose: This package manage all the related functions or procedures directly related to automatic reruns of the process/jobs
  ***************************************************************************************************************/
  -----------------------------------------------------------------------------
  -- Functionality: Get the executions needs by metric and source system
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_GET_AUTO_EXEC_PARAM_PENDING
    RETURN TYPE_RESULSET
  IS
    r_autorun TYPE_RESULSET;
    v_number NUMBER(15);
  BEGIN
    OPEN r_autorun FOR SELECT * FROM auto_exec_param aep WHERE aep.enabled_flg = PENDING_STRING ORDER BY aep.source_system_id;
    UPDATE auto_exec_param aep
    SET aep.enabled_flg   = PROGRESS_STRING
    WHERE aep.enabled_flg = PENDING_STRING;
    RETURN r_autorun;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,NULL,sysdate,'F_GET_AUTO_EXEC_PARAM_PENDING','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,RERUN_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_AUTO_EXEC_PARAM_PENDING;
-----------------------------------------------------------------------------
-- Functionality: Get the detailed executions needs by source system and metric
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_SYSTEM_COMPONENTS_PEND(
      p_metricid system_components.metric_id%TYPE ,
      p_source_system_id system_components.source_system_id%TYPE)
    RETURN TYPE_RESULSET
  IS
    r_autorun TYPE_RESULSET;
    v_number NUMBER(15);
  BEGIN
    OPEN r_autorun FOR SELECT * FROM SYSTEM_COMPONENTS SC WHERE sc.source_system_id = p_source_system_id AND sc.metric_id = p_metricid AND SC.ENABLED_FLG = PENDING_STRING ORDER BY sc.execution_order ASC;
    UPDATE SYSTEM_COMPONENTS SC
    SET SC.enabled_flg        = PROGRESS_STRING
    WHERE sc.source_system_id = p_source_system_id
    AND sc.metric_id          = p_metricid
    AND SC.enabled_flg        = PENDING_STRING;
    RETURN r_autorun;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_GET_SYSTEM_COMPONENTS_PEND','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK, p_metricid);
    RETURN NULL;
    RAISE;
  END F_GET_SYSTEM_COMPONENTS_PEND;
-----------------------------------------------------------------------------
-- Functionality: Get the map between component and script
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPONENT_SCRIPT(
      p_component component_script.component_id%TYPE)
    RETURN TYPE_RESULSET
  IS
    r_autorun TYPE_RESULSET;
  BEGIN
    OPEN r_autorun FOR SELECT * FROM component_script cs WHERE cs.component_id = p_component;
    RETURN r_autorun;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
    RAISE;
  end F_GET_COMPONENT_SCRIPT;
  
-----------------------------------------------------------------------------
-- Functionality: Get the map between each script and their parameters
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPONENT_FOLDERS(
      p_component component_params.component_id%TYPE)
    RETURN TYPE_RESULSET
  IS
    r_autorun TYPE_RESULSET;
  begin
    OPEN r_autorun FOR SELECT * FROM component_folders CP WHERE cp.component_id = p_component;
    RETURN r_autorun;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
    RAISE;
  END F_GET_COMPONENT_FOLDERS;
  
-----------------------------------------------------------------------------
-- Functionality: Get the map between each script and their parameters
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPONENT_PARAMS(
      p_component component_params.component_id%TYPE)
    RETURN TYPE_RESULSET
  IS
    r_autorun TYPE_RESULSET;
  BEGIN
    OPEN r_autorun FOR SELECT * FROM component_params CP WHERE cp.component_id = p_component ORDER BY cp.param_order ASC;
    RETURN r_autorun;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
    RAISE;
  END F_GET_COMPONENT_PARAMS;
-----------------------------------------------------------------------------
-- Functionality: Get the params by source system
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_SS_PARAMS(
      p_source_system source_system.source_system_id%TYPE)
    RETURN TYPE_RESULSET
  IS
    r_autorun TYPE_RESULSET;
    V_NUMBER number(15);
  BEGIN
    OPEN r_autorun FOR SELECT ss.region_folder,
    ss.region_id,
    ss.source_system_id,
    ss.system_folder,
    ss.system_folder_aux FROM source_system ss WHERE ss.source_system_id = p_source_system;
    RETURN r_autorun;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system,sysdate,'F_GET_SS_PARAMS','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,RERUN_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_SS_PARAMS;

-----------------------------------------------------------------------------
-- Functionality: Update status in table auto_exec_param
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_AEP_DONE(
      p_metricid auto_exec_param.metric_id%TYPE ,
      p_source_system_id auto_exec_param.source_system_id%TYPE)
    RETURN NUMBER
  IS
    v_number NUMBER(15);
  BEGIN
    UPDATE auto_exec_param aep
    SET aep.enabled_flg      = DONE_STRING
    WHERE aep.metric_id      = p_metricid
    AND aep.source_system_id = p_source_system_id;
    IF SQL%ROWCOUNT          = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_SET_STATUS_AEP_DONE','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,p_metricid);
    ROLLBACK;
    RETURN 1;
    RAISE;
  END F_SET_STATUS_AEP_DONE;
-----------------------------------------------------------------------------
-- Functionality: Update status in table auto_exec_param
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_AEP_FAILED(
      p_metricid auto_exec_param.metric_id%TYPE ,
      p_source_system_id auto_exec_param.source_system_id%TYPE)
    RETURN NUMBER
  IS
    v_number NUMBER(15);
  BEGIN
    UPDATE auto_exec_param aep
    SET aep.enabled_flg      = FAILED_STRING
    WHERE aep.metric_id      = p_metricid
    AND aep.source_system_id = p_source_system_id;
    IF SQL%ROWCOUNT          = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_SET_STATUS_AEP_FAILED','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,p_metricid);
    ROLLBACK;
    RETURN 1;
    RAISE;
  END F_SET_STATUS_AEP_FAILED;

-----------------------------------------------------------------------------
-- Functionality: Update status in table system_components
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_SYSCOM_DONE(
      p_metricid system_components.metric_id%TYPE ,
      p_source_system_id system_components.source_system_id%TYPE,
      p_component system_components.component_id%TYPE)
    RETURN NUMBER
  IS
    v_number NUMBER(15);
  BEGIN
    UPDATE system_components sc
    SET sc.enabled_flg      = DONE_STRING
    WHERE sc.metric_id      = p_metricid
    AND sc.source_system_id = p_source_system_id
    AND sc.component_id     = p_component;
    IF SQL%ROWCOUNT         = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_SET_STATUS_SYSCOM_DONE','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,p_metricid);
    ROLLBACK;
    RETURN 1;
    RAISE;
  END F_SET_STATUS_SYSCOM_DONE;

-----------------------------------------------------------------------------
-- Functionality: Update status in table system_components
-- previous components to DONE
-- current component to FAILED
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_SYSCOM_FAILED(
      p_metricid system_components.metric_id%TYPE ,
      p_source_system_id system_components.source_system_id%TYPE,
      p_component system_components.component_id%TYPE)
    RETURN NUMBER
  IS
    res      NUMBER;
    v_number NUMBER(15);
  BEGIN
    SELECT sc.execution_order
    INTO res
    FROM system_components sc
    WHERE sc.metric_id      = p_metricid
    AND sc.source_system_id = p_source_system_id
    AND sc.component_id     = p_component;
    UPDATE system_components sc
    SET sc.enabled_flg      = DONE_STRING
    WHERE sc.metric_id      = p_metricid
    AND sc.source_system_id = p_source_system_id
    AND sc.execution_order  < res;
    UPDATE system_components sc
    SET sc.enabled_flg      = FAILED_STRING
    WHERE sc.metric_id      = p_metricid
    AND sc.source_system_id = p_source_system_id
    AND sc.component_id     = p_component;
    IF SQL%ROWCOUNT         > 0 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_SET_STATUS_SYSCOM_FAILED','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,p_metricid);
    ROLLBACK;
    RETURN 1;
    RAISE;
  END F_SET_STATUS_SYSCOM_FAILED;

-----------------------------------------------------------------------------
-- Functionality: Update table source_system
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_SET_AEP_DATE_SS(
      p_lastloaddate source_system.AGE_HADOOP_LASTLOAD%TYPE ,
      p_nextloaddate source_system.AGE_HADOOP_NEXTLOAD%TYPE,
      p_source_system_id source_system.source_system_id%TYPE)
    RETURN NUMBER
  AS
    v_number NUMBER(15);
  BEGIN
    UPDATE SOURCE_SYSTEM SS
    SET SS.AGE_HADOOP_LASTLOAD = p_lastloaddate,
      SS.AGE_HADOOP_NEXTLOAD   = p_nextloaddate
    WHERE SS.SOURCE_SYSTEM_ID  = p_source_system_id;
    IF SQL%ROWCOUNT            = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_SET_AEP_DATE_SS','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,RERUN_STRING);
    RETURN 1;
    RAISE;
  END F_SET_AEP_DATE_SS;
-----------------------------------------------------------------------------
-- Functionality: Update table source_system_metrics
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_SET_AEP_DATE_SSM(
      p_lastloaddate source_system.AGE_HADOOP_LASTLOAD%TYPE ,
      p_nextloaddate source_system.AGE_HADOOP_NEXTLOAD%TYPE,
      p_metricid source_system_metric.metric_id%TYPE ,
      p_source_system_id source_system_metric.source_system_id%TYPE)
    RETURN NUMBER
  AS
    v_number NUMBER(15);
  BEGIN
    UPDATE SOURCE_SYSTEM_METRIC SSM
    SET SSM.AGE_HADOOP_LASTLOAD = p_lastloaddate,
      SSM.AGE_HADOOP_NEXTLOAD   = p_nextloaddate
    WHERE SSM.METRIC_ID         = p_metricid
    AND SSM.SOURCE_SYSTEM_ID    = p_source_system_id;
    IF SQL%ROWCOUNT             = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,NULL,p_source_system_id,sysdate,'F_SET_AEP_DATE_SSM','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,p_metricid);
    RETURN 1;
    RAISE;
  END F_SET_AEP_DATE_SSM;
  
  
  
  -----------------------------------------------------------------------------
-- Functionality: Get the bootstrap flag from source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  function F_GET_BOOTSTRAP_FLAG_COMMON (P_SOURCE_SYSTEM_ID SYSTEM_COMPONENTS.SOURCE_SYSTEM_ID%type) 
  return number
  as
  res number(15);
   begin
    select SS.BTS_FLG
    into RES
    from SOURCE_SYSTEM SS
    where SS.SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID;
  
  RETURN res;
   EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
    RAISE;
   
   END F_GET_BOOTSTRAP_FLAG_COMMON;
  -----------------------------------------------------------------------------
-- Functionality: Get the bootstrap flag from source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_BOOTSTRAP_FLAG_METRIC (p_source_system_id system_components.source_system_id%TYPE, p_metricid system_components.metric_id%TYPE) 
  return number
  as
  res number(15);
   begin
    select SSM.BTS_FLG
    into RES
    from SOURCE_SYSTEM_METRIC SSM
    where SSM.SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
    and SSM.METRIC_id = p_metricid;
  
  RETURN res;
   EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
    RAISE;
   
   END F_GET_BOOTSTRAP_FLAG_METRIC;
  
-----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag free from source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_FREE_BOOTSTRAP_FLAG_COMMON (p_source_system_id system_components.source_system_id%TYPE)
  return number
  as
   begin
    UPDATE SOURCE_SYSTEM SS
    SET SS.BTS_FLG = 0
    where SS.SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID;
    
    IF SQL%ROWCOUNT = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    end if;
    
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    return 1;
    RAISE;
    end F_FREE_BOOTSTRAP_FLAG_COMMON;
  
  -----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag free from source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_FREE_BOOTSTRAP_FLAG_METRIC (p_source_system_id system_components.source_system_id%TYPE, p_metricid system_components.metric_id%TYPE) 
  return number
  as
  v_number NUMBER(15);
   begin
    UPDATE SOURCE_SYSTEM_METRIC SSM
    SET SSM.BTS_FLG = 0
    where SSM.SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
    and SSM.METRIC_ID = p_metricid;
    
    IF SQL%ROWCOUNT = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    end if;
    
  EXCEPTION
  WHEN OTHERS THEN
    V_NUMBER:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,null,P_SOURCE_SYSTEM_ID,sysdate,'F_SET_STATUS_AEP_PENDING','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,P_METRICID);
    ROLLBACK;
    return 1;
    RAISE;
    end F_FREE_BOOTSTRAP_FLAG_METRIC;
  
  
-----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag AS LOCK from source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_LOCK_BOOTSTRAP_FLAG_COMMON (p_source_system_id system_components.source_system_id%TYPE)
  return number
  as
   begin
    UPDATE SOURCE_SYSTEM SS
    SET SS.BTS_FLG = 1
    where SS.SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID;
    
    IF SQL%ROWCOUNT = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    end if;
    
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    return 1;
    RAISE;
    end F_LOCK_BOOTSTRAP_FLAG_COMMON;
  
-----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag AS LOCK from source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_LOCK_BOOTSTRAP_FLAG_METRIC (p_source_system_id system_components.source_system_id%TYPE, p_metricid system_components.metric_id%TYPE)
   return number
  as
  v_number NUMBER(15);
   begin
    UPDATE SOURCE_SYSTEM_METRIC SSM
    SET SSM.BTS_FLG = 1
    where SSM.SOURCE_SYSTEM_ID = P_SOURCE_SYSTEM_ID
    and SSM.METRIC_ID = p_metricid;
    
    IF SQL%ROWCOUNT = 1 THEN
      COMMIT;
      RETURN 0;
    ELSE
      ROLLBACK;
      RETURN 1;
    end if;
    
  EXCEPTION
  WHEN OTHERS THEN
    V_NUMBER:=PKG_MONITORING.F_INSERT_LOG_JOBS(AUTORERUN_STRING,null,P_SOURCE_SYSTEM_ID,sysdate,'F_SET_STATUS_AEP_PENDING','ERROR', 'FATAL', 'AUTO_RERUN ERROR', DBMS_UTILITY.FORMAT_ERROR_STACK,P_METRICID);
    ROLLBACK;
    return 1;
    RAISE;
    end F_LOCK_BOOTSTRAP_FLAG_METRIC;
  
  
END PKG_AUTO_EXEC;
