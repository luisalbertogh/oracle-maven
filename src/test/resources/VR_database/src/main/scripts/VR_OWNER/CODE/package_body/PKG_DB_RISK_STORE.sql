--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_DB_RISK_STORE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_DB_RISK_STORE" 

AS
  -----------------------------------------------------------------------------
  -- Functionality: Get the params by factor
  -- Used: dbRisk Store
  ------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS_FOR_FACTOR(
      p_factor_name DB_RISK_STORE_CONF.FACTOR_NAME%TYPE )
    RETURN TYPE_RESULSET
  IS
    r_factor TYPE_RESULSET;
    V_NUMBER NUMBER(15);
  BEGIN
    OPEN r_factor FOR SELECT dbRisk.FACTOR_NAME, dbRisk.FACTOR_POSITION, dbRisk.CONTEXT, dbRisk.EXPOSURE, dbRisk.DATA_TYPE, dbRisk.SCENARIO_TYPE, dbRisk.SENSITIVITY, dbRisk.SENSITIVITY_CATEGORY, dbRisk.SENSITIVITY_SUBCATEGORY, dbRisk.OTHER_PARAMETERS, dbRisk.DEFINITION_ID_FILTERING FROM DB_RISK_STORE_CONF dbRisk WHERE dbRisk.FACTOR_NAME = p_factor_name;
    RETURN r_factor;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DB_RISK_STORE_STRING,NULL,sysdate,'F_GET_PARAMS_FOR_SS_FACTOR','ERROR', 'FATAL', DB_RISK_STORE_ERROR_STRING, DBMS_UTILITY.FORMAT_ERROR_STACK,DB_RISK_STORE_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_PARAMS_FOR_FACTOR;
-----------------------------------------------------------------------------
-- Functionality: Get the params values
-- Used: dbRisk Store
------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS
    RETURN TYPE_RESULSET
  IS
    r_factor TYPE_RESULSET;
    V_NUMBER NUMBER(15);
    v_asofdate DATE;
  BEGIN
    select max(cobdate) into v_asofdate from db_risk_store_conf;
    IF (v_asofdate IS NULL) THEN
      v_asofdate:=TRUNC(sysdate);
    END IF;
    OPEN r_factor FOR SELECT dbRisk.FACTOR_NAME, dbRisk.FACTOR_POSITION, dbRisk.CONTEXT, dbRisk.EXPOSURE, dbRisk.DATA_TYPE, dbRisk.SCENARIO_TYPE, dbRisk.SENSITIVITY, dbRisk.SENSITIVITY_CATEGORY, dbRisk.SENSITIVITY_SUBCATEGORY, dbRisk.OTHER_PARAMETERS, dbRisk.DEFINITION_ID_FILTERING, dbRisk.REPORT_NAME  FROM DB_RISK_STORE_CONF dbRisk WHERE dbRisk.COBDATE = v_asofdate order by dbRisk.FACTOR_POSITION;
    RETURN r_factor;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DB_RISK_STORE_STRING,NULL,ALL_SS_STRING,sysdate,'F_GET_PARAMS','ERROR', 'FATAL', DB_RISK_STORE_ERROR_STRING, DBMS_UTILITY.FORMAT_ERROR_STACK,DB_RISK_STORE_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_PARAMS;
  
-----------------------------------------------------------------------------
-- Functionality: Insert a riskFactor in the DB_RISK_STORE_CONF table
-- Used: com.db.volcker.dbrisk.configPublisher
------------------------------------------------------------------------------
  PROCEDURE PR_INSERT_RISKFACTORS
    (
      p_factor_name DB_RISK_STORE_CONF.FACTOR_NAME%TYPE,
      p_factor_position DB_RISK_STORE_CONF.FACTOR_POSITION%TYPE,
      p_context DB_RISK_STORE_CONF.CONTEXT%TYPE,
      p_exposure DB_RISK_STORE_CONF.EXPOSURE%TYPE,
      p_data_type DB_RISK_STORE_CONF.DATA_TYPE%TYPE,
      p_scenario_type DB_RISK_STORE_CONF.SCENARIO_TYPE%TYPE,
      p_sensitivity DB_RISK_STORE_CONF.SENSITIVITY%TYPE,
      p_sensitivity_category DB_RISK_STORE_CONF.SENSITIVITY_CATEGORY%TYPE,
      p_sensitivity_subcategory DB_RISK_STORE_CONF.SENSITIVITY_SUBCATEGORY%TYPE,
      p_definition_id_filtering DB_RISK_STORE_CONF.DEFINITION_ID_FILTERING%TYPE,
      p_report_name DB_RISK_STORE_CONF.REPORT_NAME%TYPE,
      p_other_parameters DB_RISK_STORE_CONF.OTHER_PARAMETERS%TYPE,
      p_cobdate VARCHAR2
    )
  IS
    v_asofdate DATE;
	V_NUMBER NUMBER(15);
  BEGIN
    IF (p_cobdate  IS NULL) THEN
      v_asofdate:=TRUNC(sysdate);
    ELSE
      v_asofdate:=to_date(p_cobdate,'yyyyMMdd');
    END IF;
    IF (p_sensitivity IS NOT NULL AND p_sensitivity_category is NOT NULL AND  p_sensitivity_subcategory IS NOT NULL) THEN
      UPDATE DB_RISK_STORE_CONF 
      SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
      WHERE   EXPOSURE=p_exposure AND
        DATA_TYPE=p_data_type AND
        SCENARIO_TYPE=p_scenario_type AND
        SENSITIVITY=p_sensitivity AND
        SENSITIVITY_CATEGORY=p_sensitivity_category AND
        SENSITIVITY_SUBCATEGORY=p_sensitivity_subcategory AND
        REPORT_NAME=p_report_name AND
        COBDATE=v_asofdate;
   ELSIF (p_sensitivity IS NULL AND p_sensitivity_category IS NULL AND p_sensitivity_subcategory IS NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY IS NULL AND
          SENSITIVITY_CATEGORY IS NULL AND
          SENSITIVITY_SUBCATEGORY IS NULL AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;     
   ELSIF (p_sensitivity IS NULL AND p_sensitivity_category IS NOT NULL AND p_sensitivity_subcategory IS NOT NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY IS NULL AND
          SENSITIVITY_CATEGORY=p_sensitivity_category AND
          SENSITIVITY_SUBCATEGORY=p_sensitivity_subcategory AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;
   ELSIF (p_sensitivity IS NULL AND p_sensitivity_category IS NULL AND p_sensitivity_subcategory IS NOT NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY IS NULL AND
          SENSITIVITY_CATEGORY IS NULL AND
          SENSITIVITY_SUBCATEGORY=p_sensitivity_subcategory AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;  
   ELSIF (p_sensitivity IS NULL AND p_sensitivity_category IS NOT NULL AND p_sensitivity_subcategory IS NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY IS NULL AND
          SENSITIVITY_CATEGORY=p_sensitivity_category AND
          SENSITIVITY_SUBCATEGORY IS NULL AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;        
   ELSIF (p_sensitivity IS NOT NULL AND p_sensitivity_category IS NULL AND p_sensitivity_subcategory IS NOT NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY=p_sensitivity AND
          SENSITIVITY_CATEGORY IS NULL AND
          SENSITIVITY_SUBCATEGORY=p_sensitivity_subcategory AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;          
   ELSIF (p_sensitivity IS NOT NULL AND p_sensitivity_category IS NULL AND p_sensitivity_subcategory IS NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY=p_sensitivity AND
          SENSITIVITY_CATEGORY IS NULL AND
          SENSITIVITY_SUBCATEGORY IS NULL AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;
   ELSIF (p_sensitivity IS NOT NULL AND p_sensitivity_category IS NOT NULL AND p_sensitivity_subcategory IS NULL) THEN
        UPDATE DB_RISK_STORE_CONF 
        SET DEFINITION_ID_FILTERING=p_definition_id_filtering 
        WHERE   EXPOSURE=p_exposure AND
          DATA_TYPE=p_data_type AND
          SCENARIO_TYPE=p_scenario_type AND
          SENSITIVITY=p_sensitivity AND
          SENSITIVITY_CATEGORY=p_sensitivity_category AND
          SENSITIVITY_SUBCATEGORY IS NULL AND
          REPORT_NAME=p_report_name AND
          COBDATE=v_asofdate;          
   END IF; 
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DB_RISK_STORE_STRING,NULL,sysdate,'PR_INSERT_RISKFACTORS','ERROR', 'FATAL', DB_RISK_STORE_ERROR_STRING, DBMS_UTILITY.FORMAT_ERROR_STACK,DB_RISK_STORE_STRING);
    RAISE;	  
  END PR_INSERT_RISKFACTORS;  
  
 -----------------------------------------------------------------------------
  -- Functionality: Get the params by isofinterest and maxcobdate
  -- Used: dbRisk Store
  ------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS_FOR_DATE_INTERES
    RETURN TYPE_RESULSET
  IS
    r_factor TYPE_RESULSET;
    V_NUMBER NUMBER(15);
    v_asofdate DATE;
  BEGIN
  
    --remove previous execution 
	delete from DB_RISK_STORE_CONF where COBDATE=(select TRUNC(sysdate) from dual);
    
    select max(cobdate) into v_asofdate from db_risk_store_conf;
    IF (v_asofdate IS NULL) THEN
      v_asofdate:=TRUNC(sysdate);
    END IF;
	
	
	
	--Copy data in DB_RISK_STORE_CONF for sysdate
	INSERT INTO DB_RISK_STORE_CONF (FACTOR_NAME, FACTOR_POSITION, CONTEXT, EXPOSURE, DATA_TYPE, SCENARIO_TYPE, SENSITIVITY, SENSITIVITY_CATEGORY, SENSITIVITY_SUBCATEGORY, OTHER_PARAMETERS, DEFINITION_ID_FILTERING, REPORT_NAME, COBDATE)
		SELECT dbRisk.FACTOR_NAME, dbRisk.FACTOR_POSITION, dbRisk.CONTEXT, dbRisk.EXPOSURE, dbRisk.DATA_TYPE, dbRisk.SCENARIO_TYPE, dbRisk.SENSITIVITY, dbRisk.SENSITIVITY_CATEGORY, dbRisk.SENSITIVITY_SUBCATEGORY, dbRisk.OTHER_PARAMETERS, dbRisk.DEFINITION_ID_FILTERING,dbrisk.REPORT_NAME,(select TRUNC(sysdate) from dual) FROM DB_RISK_STORE_CONF dbRisk
		WHERE dbRisk.COBDATE = v_asofdate;		
    
    OPEN r_factor FOR SELECT dbRisk.FACTOR_NAME, dbRisk.FACTOR_POSITION, dbRisk.CONTEXT, dbRisk.EXPOSURE, dbRisk.DATA_TYPE, dbRisk.SCENARIO_TYPE, dbRisk.SENSITIVITY, dbRisk.SENSITIVITY_CATEGORY, dbRisk.SENSITIVITY_SUBCATEGORY, dbRisk.OTHER_PARAMETERS, dbRisk.DEFINITION_ID_FILTERING,dbrisk.REPORT_NAME FROM DB_RISK_STORE_CONF dbRisk WHERE dbRisk.COBDATE = v_asofdate AND dbrisk.REPORT_NAME is not null;
    RETURN r_factor;
    
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DB_RISK_STORE_STRING,NULL,sysdate,'F_GET_PARAMS_FOR_DATE_INTERES','ERROR', 'FATAL', DB_RISK_STORE_ERROR_STRING, DBMS_UTILITY.FORMAT_ERROR_STACK,DB_RISK_STORE_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_PARAMS_FOR_DATE_INTERES;    
  
  -----------------------------------------------------------------------------
  -- Functionality: Get the params with isofinterest='YES' for cobdate with overriden values
  -- Used: dbRisk Store
  ------------------------------------------------------------------------------
  FUNCTION F_GET_PARAMS_OVERRIDE
    RETURN TYPE_RESULSET
  IS
    r_factor TYPE_RESULSET;
    V_NUMBER NUMBER(15);
    v_asofdate DATE;
  BEGIN
    select max(cobdate) into v_asofdate from db_risk_store_conf;
    IF (v_asofdate IS NULL) THEN
      v_asofdate:=TRUNC(sysdate);
    END IF;
    OPEN r_factor FOR SELECT dbRiskOver.FACTOR_NAME, dbRiskOver.FACTOR_POSITION, dbRiskOver.CONTEXT, dbRiskOver.EXPOSURE, dbRiskOver.DATA_TYPE, dbRiskOver.SCENARIO_TYPE, dbRiskOver.SENSITIVITY, dbRiskOver.SENSITIVITY_CATEGORY, dbRiskOver.SENSITIVITY_SUBCATEGORY, dbRiskOver.OTHER_PARAMETERS, dbRiskOver.DEFINITION_ID_FILTERING, dbRiskOver.REPORT_NAME
    FROM DB_RISK_STORE_CONF_OVERRIDE dbRiskOver
    JOIN
    DB_RISK_STORE_CONF dbRisk ON dbRiskOver.FACTOR_NAME = dbRisk.FACTOR_NAME
    JOIN 
    (SELECT FACTOR_NAME, MAX(COBDATE) AS COBDATE_MAX FROM DB_RISK_STORE_CONF_OVERRIDE GROUP BY FACTOR_NAME) max
    ON dbRiskOver.FACTOR_NAME = max.FACTOR_NAME AND dbRiskOver.COBDATE = max.COBDATE_MAX
    UNION
    SELECT dbRisk.FACTOR_NAME, dbRisk.FACTOR_POSITION, dbRisk.CONTEXT, dbRisk.EXPOSURE, dbRisk.DATA_TYPE, dbRisk.SCENARIO_TYPE, dbRisk.SENSITIVITY, dbRisk.SENSITIVITY_CATEGORY, dbRisk.SENSITIVITY_SUBCATEGORY, dbRisk.OTHER_PARAMETERS, dbRisk.DEFINITION_ID_FILTERING, dbRisk.REPORT_NAME 
    FROM DB_RISK_STORE_CONF dbRisk WHERE dbRisk.FACTOR_NAME NOT IN (SELECT FACTOR_NAME FROM DB_RISK_STORE_CONF_OVERRIDE dbRiskOver) AND dbRisk.COBDATE = v_asofdate ORDER BY FACTOR_POSITION;
  RETURN r_factor;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DB_RISK_STORE_STRING,NULL,sysdate,'F_GET_PARAMS_OVERRIDE','ERROR', 'FATAL', DB_RISK_STORE_ERROR_STRING, DBMS_UTILITY.FORMAT_ERROR_STACK,DB_RISK_STORE_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_PARAMS_OVERRIDE;  
  
  
   
 	-----------------------------------------------------------------------------
-- Functionality: Get the desk param values
-- Used: dbRisk TradeDetails
------------------------------------------------------------------------------
    FUNCTION F_GET_DESK_PARAMS (
      p_cobdate varchar2 )
    RETURN TYPE_RESULSET
  IS
    r_factor TYPE_RESULSET;
    V_NUMBER NUMBER(15);
    v_asofdate DATE;
  BEGIN
    select max(asofdate) into v_asofdate from book_hierarchy_rpl where asofdate<=to_date(p_cobdate,'yyyyMMdd') ;
    IF (v_asofdate IS NULL) THEN
      select max(asofdate) into v_asofdate from book_hierarchy_rpl;
    END IF;
    OPEN r_factor FOR SELECT distinct rpl.VOLCKER_TRADING_DESK_FULL FROM BOOK_HIERARCHY_RPL rpl WHERE rpl.asofdate = v_asofdate and rpl.VOLCKER_TRADING_DESK_FULL is not null order by rpl.VOLCKER_TRADING_DESK_FULL;
    RETURN r_factor;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS(DB_RISK_STORE_STRING,NULL,ALL_SS_STRING,sysdate,'F_GET_DESK_PARAMS','ERROR', 'FATAL', DB_RISK_STORE_ERROR_STRING, DBMS_UTILITY.FORMAT_ERROR_STACK,DB_RISK_STORE_STRING);
    RETURN NULL;
    RAISE;
  END F_GET_DESK_PARAMS;

  
END PKG_DB_RISK_STORE;

