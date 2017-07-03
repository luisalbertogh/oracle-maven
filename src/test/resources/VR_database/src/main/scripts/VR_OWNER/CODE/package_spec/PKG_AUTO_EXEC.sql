--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_AUTO_EXEC runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_AUTO_EXEC" AS

  /**************************************************************************************************************
  * Autor: SERGIO.COUTO@DB.COM
  * Date: 14/10/2014
  *
  * Purpose: This package manage all the related functions or procedures directly related to automatic reruns of the process/jobs

  FUNCTION F_GET_AUTO_EXEC_PARAM_PENDING			--> Function to get the pending metrics and source_systems
  FUNCTION F_GET_SYSTEM_COMPONENTS_PEND 			--> Function to get the components for each metric and source_systems
  FUNCTION F_GET_COMPONENT_SCRIPT 					-->	Function to get the script of each component
  FUNCTION F_GET_COMPONENT_PARAMS 					--> Function to get the parameters of each script
  FUNCTION F_GET_COMPONENT_FOLDERS 					--> Function to get the folders to delete by each script
  FUNCTION F_GET_SS_PARAMS 							--> Function to get the parameters values by source_systems
  FUNCTION F_SET_AEP_DATE_SS 						--> Function to pretend the rolldate execution
  FUNCTION F_SET_AEP_DATE_SSM 						-->  Function to pretend the rolldate execution
  FUNCTION F_SET_STATUS_AEP_DONE 					--> Set status in auto_exec_param to done by metric and source system
  FUNCTION F_SET_STATUS_AEP_FAILED				    --> Set status in auto_exec_param to done by metric and source system
  FUNCTION F_SET_STATUS_SYSCOM_DONE           		--> Set status in system_component to done by metric and source system and component
  FUNCTION F_SET_STATUS_SYSCOM_FAILED         		--> Set status in system_component to Failed for all pending components in a metric and source system
  FUNCTION F_GET_BOOTSTRAP_FLAG_COMMON 				--> Get bootstrap Flag from Source System table
  FUNCTION F_GET_BOOTSTRAP_FLAG_METRIC    			--> Get bootstrap Flag from Source System metric table
  FUNCTION F_FREE_BOOTSTRAP_FLAG_COMMON        		--> Free bootstrap flag from Source System table
  FUNCTION F_FREE_BOOTSTRAP_FLAG_METRIC           	--> Free bootstrap flag from Source System metric table
  FUNCTION F_LOCK_BOOTSTRAP_FLAG_COMMON           	--> Lock bootstrap flag from Source System table
  FUNCTION F_LOCK_BOOTSTRAP_FLAG_METRIC           	--> Lock bootstrap flag from Source System metric table
  ***************************************************************************************************************/

  TYPE TYPE_RESULSET IS REF CURSOR;
  PENDING_STRING varchar2(15) := 'PENDING';
  PROGRESS_STRING varchar2(15) := 'IN PROGRESS';
  DONE_STRING varchar2(15) := 'DONE';
  FAILED_STRING varchar2(15) := 'FAILED';
  AUTORERUN_STRING varchar2(10) := 'AUTO_RERUN';
  RERUN_STRING varchar2(5) := 'RERUN';


-----------------------------------------------------------------------------
-- Functionality: Get the dated executions needs by metric and source system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_AUTO_EXEC_PARAM_PENDING  RETURN TYPE_RESULSET;

-----------------------------------------------------------------------------
-- Functionality: Get the detailed executions needs by source system and metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_SYSTEM_COMPONENTS_PEND (p_metricid system_components.metric_id%TYPE ,
							 p_source_system_id system_components.source_system_id%TYPE) RETURN TYPE_RESULSET;

-----------------------------------------------------------------------------
-- Functionality: Get the map between component and script
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPONENT_SCRIPT (p_component component_script.component_id%TYPE) RETURN TYPE_RESULSET;

-----------------------------------------------------------------------------
-- Functionality: Get the map between each script and their parameters
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPONENT_PARAMS (p_component component_params.component_id%TYPE) RETURN TYPE_RESULSET;
-----------------------------------------------------------------------------
-- Functionality: Get the map between each script and their folders to delete them
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPONENT_FOLDERS (p_component component_params.component_id%TYPE) RETURN TYPE_RESULSET;
-----------------------------------------------------------------------------
-- Functionality: Get the params values by source system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_SS_PARAMS ( p_source_system source_system.source_system_id%TYPE) RETURN TYPE_RESULSET;


-----------------------------------------------------------------------------
-- Functionality: Update table source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_SET_AEP_DATE_SS (p_lastloaddate source_system.AGE_HADOOP_LASTLOAD%TYPE ,
               p_nextloaddate source_system.AGE_HADOOP_NEXTLOAD%TYPE,
							 p_source_system_id source_system.source_system_id%TYPE) RETURN NUMBER;

-----------------------------------------------------------------------------
-- Functionality: Update table source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_SET_AEP_DATE_SSM (p_lastloaddate source_system.AGE_HADOOP_LASTLOAD%TYPE ,
               p_nextloaddate source_system.AGE_HADOOP_NEXTLOAD%TYPE,
							 p_metricid source_system_metric.metric_id%TYPE ,
							 p_source_system_id source_system_metric.source_system_id%TYPE) RETURN NUMBER;

-----------------------------------------------------------------------------
-- Functionality: Update status in table auto_exec_param
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_AEP_DONE (p_metricid auto_exec_param.metric_id%TYPE ,
							 p_source_system_id auto_exec_param.source_system_id%TYPE) RETURN NUMBER;

-----------------------------------------------------------------------------
-- Functionality: Update status in table auto_exec_param
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_AEP_FAILED (p_metricid auto_exec_param.metric_id%TYPE ,
							 p_source_system_id auto_exec_param.source_system_id%TYPE) RETURN NUMBER;


-----------------------------------------------------------------------------
-- Functionality: Update status in table system_components
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_SYSCOM_DONE (p_metricid system_components.metric_id%TYPE ,
							 p_source_system_id system_components.source_system_id%TYPE,
               p_component system_components.component_id%TYPE) RETURN NUMBER;


-----------------------------------------------------------------------------
-- Functionality: Update status in table system_components to failed
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_SET_STATUS_SYSCOM_FAILED (p_metricid system_components.metric_id%TYPE ,
							 p_source_system_id system_components.source_system_id%TYPE,
               p_component system_components.component_id%TYPE) RETURN NUMBER;
               
-----------------------------------------------------------------------------
-- Functionality: Get the bootstrap flag from source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_BOOTSTRAP_FLAG_COMMON (p_source_system_id system_components.source_system_id%TYPE) RETURN NUMBER;
  
  -----------------------------------------------------------------------------
-- Functionality: Get the bootstrap flag from source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_GET_BOOTSTRAP_FLAG_METRIC (p_source_system_id system_components.source_system_id%TYPE, p_metricid system_components.metric_id%TYPE) RETURN NUMBER;
  
-----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag free from source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_FREE_BOOTSTRAP_FLAG_COMMON (p_source_system_id system_components.source_system_id%TYPE) RETURN NUMBER;
  
  -----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag as free from source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_FREE_BOOTSTRAP_FLAG_METRIC (p_source_system_id system_components.source_system_id%TYPE, p_metricid system_components.metric_id%TYPE) RETURN NUMBER;
  
  -----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag as lock from source_system
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_LOCK_BOOTSTRAP_FLAG_COMMON (p_source_system_id system_components.source_system_id%TYPE) RETURN NUMBER;
  
  -----------------------------------------------------------------------------
-- Functionality: Set the bootstrap flag as lock from source_system_metric
-- Used: Automatic Rerun process
------------------------------------------------------------------------------
  FUNCTION F_LOCK_BOOTSTRAP_FLAG_METRIC (p_source_system_id system_components.source_system_id%TYPE, p_metricid system_components.metric_id%TYPE) RETURN NUMBER;

END PKG_AUTO_EXEC;
