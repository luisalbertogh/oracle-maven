--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_BH_RPL_PENDING_INSERT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE trigger TRG_BH_RPL_PENDING_INSERT
    BEFORE INSERT ON BOOK_HIERARCHY_RPL_PENDING
    FOR EACH ROW 
DECLARE
   v_username varchar2(20);  
   uniqueentry NUMBER;
   BH_PK_VIOLATION exception;
   var_aux NUMBER;
BEGIN
    SELECT count(*) quantity into uniqueentry  from book_hierarchy_rpl_pending where book_id = :new.BOOK_ID and (source_system = :new.SOURCE_SYSTEM or (source_system is null and :new.SOURCE_SYSTEM is null)) and asofdate = :new.ASOFDATE;
    IF uniqueentry > 0 THEN
      raise BH_PK_VIOLATION;
    END IF;
SELECT user INTO v_username FROM dual;
:new.create_date := systimestamp;  
:new.create_user := v_username; 

insert into book_hierarchy_rpl_aud values(
:new.ASOFDATE, 
:new.BOOK_ID, 
:new.VOLCKER_TRADING_DESK, 
:new.VOLCKER_TRADING_DESK_FULL, 
:new.LOWEST_LEVEL_RPL_CODE, 
:new.LOWEST_LEVEL_RPL_FULL_NAME, 
:new.LOWEST_LEVEL_RPL, 
:new.SOURCE_SYSTEM, 
:new.LEGAL_ENTITY, 
:new.GLOBAL_TRADER_BOOK_ID, 
:new.PROFIT_CENTER_ID, 
:new.COMMENTS, 
:new.DATA_SOURCE, 
:new.CREATE_DATE, 
:new.LAST_MODIFIED_DATE, 
:new.CHARGE_REPORTING_UNIT_CODE, 
:new.CHARGE_REPORTING_UNIT, 
:new.CHARGE_REPORTING_PARENT_CODE, 
:new.CHARGE_REPORTING_PARENT, 
:new.MI_LOCATION, 
:new.UBR_LEVEL_1_ID, 
:new.UBR_LEVEL_1_NAME, 
:new.UBR_LEVEL_1_RPL_CODE, 
:new.UBR_LEVEL_2_ID, 
:new.UBR_LEVEL_2_NAME, 
:new.UBR_LEVEL_2_RPL_CODE, 
:new.UBR_LEVEL_3_ID, 
:new.UBR_LEVEL_3_NAME, 
:new.UBR_LEVEL_3_RPL_CODE, 
:new.UBR_LEVEL_4_ID, 
:new.UBR_LEVEL_4_NAME, 
:new.UBR_LEVEL_4_RPL_CODE, 
:new.UBR_LEVEL_5_ID, 
:new.UBR_LEVEL_5_NAME, 
:new.UBR_LEVEL_5_RPL_CODE, 
:new.UBR_LEVEL_6_ID, 
:new.UBR_LEVEL_6_NAME, 
:new.UBR_LEVEL_6_RPL_CODE, 
:new.UBR_LEVEL_7_ID, 
:new.UBR_LEVEL_7_NAME, 
:new.UBR_LEVEL_7_RPL_CODE, 
:new.UBR_LEVEL_8_ID, 
:new.UBR_LEVEL_8_NAME, 
:new.UBR_LEVEL_8_RPL_CODE, 
:new.UBR_LEVEL_9_ID, 
:new.UBR_LEVEL_9_NAME, 
:new.UBR_LEVEL_9_RPL_CODE, 
:new.UBR_LEVEL_10_ID, 
:new.UBR_LEVEL_10_NAME, 
:new.UBR_LEVEL_10_RPL_CODE, 
:new.UBR_LEVEL_11_ID, 
:new.UBR_LEVEL_11_NAME, 
:new.UBR_LEVEL_11_RPL_CODE, 
:new.UBR_LEVEL_12_ID, 
:new.UBR_LEVEL_12_NAME, 
:new.UBR_LEVEL_12_RPL_CODE, 
:new.UBR_LEVEL_13_ID, 
:new.UBR_LEVEL_13_NAME, 
:new.UBR_LEVEL_13_RPL_CODE, 
:new.UBR_LEVEL_14_ID, 
:new.UBR_LEVEL_14_NAME, 
:new.UBR_LEVEL_14_RPL_CODE, 
:new.DESK_LEVEL_1_ID, 
:new.DESK_LEVEL_1_NAME, 
:new.DESK_LEVEL_1_RPL_CODE, 
:new.DESK_LEVEL_2_ID, 
:new.DESK_LEVEL_2_NAME, 
:new.DESK_LEVEL_2_RPL_CODE, 
:new.DESK_LEVEL_3_ID, 
:new.DESK_LEVEL_3_NAME, 
:new.DESK_LEVEL_3_RPL_CODE, 
:new.DESK_LEVEL_4_ID, 
:new.DESK_LEVEL_4_NAME, 
:new.DESK_LEVEL_4_RPL_CODE, 
:new.DESK_LEVEL_5_ID, 
:new.DESK_LEVEL_5_NAME, 
:new.DESK_LEVEL_5_RPL_CODE, 
:new.PORTFOLIO_ID, 
:new.PORTFOLIO_NAME, 
:new.PORTFOLIO_RPL_CODE,
:new.BUSINESS, 
:new.SUB_BUSINESS,
:new.CREATE_USER, 
:new.LAST_MODIFICATION_USER,
'INSERT',
:new.REGION, 
:new.SUBREGION,
null,
null,
systimestamp,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
--start GBSVR-28871
null,
null,
null,
null,
--end GBSVR-28871
--start GBSVR-30040
null,
--end GBSVR-30040
--start GBSVR-30224
null,
null,
null
--end GBSVR-30224
);

EXCEPTION
    WHEN BH_PK_VIOLATION THEN
      var_aux:=PKG_MONITORING.F_INSERT_LOG_JOBS ('bRDS',null,:new.SOURCE_SYSTEM,:new.ASOFDATE,'BOOKHIERARCHY','ERROR','FATAL','PK violated','insert into book_hierarchy_rpl_pending values('||:new.ASOFDATE||','||:new.BOOK_ID||','||:new.VOLCKER_TRADING_DESK||','||:new.VOLCKER_TRADING_DESK_FULL||','||:new.LOWEST_LEVEL_RPL_CODE||','||:new.LOWEST_LEVEL_RPL_FULL_NAME||','||:new.LOWEST_LEVEL_RPL||','||:new.SOURCE_SYSTEM||','||:new.LEGAL_ENTITY||','||:new.GLOBAL_TRADER_BOOK_ID||','||:new.PROFIT_CENTER_ID||','||:new.COMMENTS||','||:new.DATA_SOURCE||','||:new.CREATE_DATE||','||:new.LAST_MODIFIED_DATE||','||:new.CHARGE_REPORTING_UNIT_CODE||','||:new.CHARGE_REPORTING_UNIT||','||:new.CHARGE_REPORTING_PARENT_CODE||','||:new.CHARGE_REPORTING_PARENT||','||:new.MI_LOCATION||','||:new.UBR_LEVEL_1_ID||','||:new.UBR_LEVEL_1_NAME||','||:new.UBR_LEVEL_1_RPL_CODE||','||:new.UBR_LEVEL_2_ID||','||:new.UBR_LEVEL_2_NAME||','||:new.UBR_LEVEL_2_RPL_CODE||','||:new.UBR_LEVEL_3_ID||','||:new.UBR_LEVEL_3_NAME||','||:new.UBR_LEVEL_3_RPL_CODE||','||:new.UBR_LEVEL_4_ID||','||:new.UBR_LEVEL_4_NAME||','||:new.UBR_LEVEL_4_RPL_CODE||','||:new.UBR_LEVEL_5_ID||','||:new.UBR_LEVEL_5_NAME||','||:new.UBR_LEVEL_5_RPL_CODE||','||:new.UBR_LEVEL_6_ID||','||:new.UBR_LEVEL_6_NAME||','||:new.UBR_LEVEL_6_RPL_CODE||','||:new.UBR_LEVEL_7_ID||','||:new.UBR_LEVEL_7_NAME||','||:new.UBR_LEVEL_7_RPL_CODE||','||:new.UBR_LEVEL_8_ID||','||:new.UBR_LEVEL_8_NAME||','||:new.UBR_LEVEL_8_RPL_CODE||','||:new.UBR_LEVEL_9_ID||','||:new.UBR_LEVEL_9_NAME||','||:new.UBR_LEVEL_9_RPL_CODE||','||:new.UBR_LEVEL_10_ID||','||:new.UBR_LEVEL_10_NAME||','||:new.UBR_LEVEL_10_RPL_CODE||','||:new.UBR_LEVEL_11_ID||','||:new.UBR_LEVEL_11_NAME||','||:new.UBR_LEVEL_11_RPL_CODE||','||:new.UBR_LEVEL_12_ID||','||:new.UBR_LEVEL_12_NAME||','||:new.UBR_LEVEL_12_RPL_CODE||','||:new.UBR_LEVEL_13_ID||','||:new.UBR_LEVEL_13_NAME||','||:new.UBR_LEVEL_13_RPL_CODE||','||:new.UBR_LEVEL_14_ID||','||:new.UBR_LEVEL_14_NAME||','||:new.UBR_LEVEL_14_RPL_CODE||','||:new.DESK_LEVEL_1_ID||','||:new.DESK_LEVEL_1_NAME||','||:new.DESK_LEVEL_1_RPL_CODE||','||:new.DESK_LEVEL_2_ID||','||:new.DESK_LEVEL_2_NAME||','||:new.DESK_LEVEL_2_RPL_CODE||','||:new.DESK_LEVEL_3_ID||','||:new.DESK_LEVEL_3_NAME||','||:new.DESK_LEVEL_3_RPL_CODE||','||:new.DESK_LEVEL_4_ID||','||:new.DESK_LEVEL_4_NAME||','||:new.DESK_LEVEL_4_RPL_CODE||','||:new.DESK_LEVEL_5_ID||','||:new.DESK_LEVEL_5_NAME||','||:new.DESK_LEVEL_5_RPL_CODE||','||:new.PORTFOLIO_ID||','||:new.PORTFOLIO_NAME||','||:new.PORTFOLIO_RPL_CODE||','||:new.BUSINESS||','||:new.SUB_BUSINESS||','||:new.REGION||','||:new.SUBREGION||')');
      raise;
     WHEN others THEN
		pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,:new.SOURCE_SYSTEM,current_date,'TRG_BH_RPL_PENDING_INSERT','ERROR', 'FATAL', 'Error:'||TO_CHAR(SQLCODE), SUBSTR(SQLERRM, 1, 2500), 'bRDS');
END;
