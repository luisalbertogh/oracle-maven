--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BH_PROCESS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BH_PROCESS" AS


PROCEDURE P_BH_INITIAL_LOADING
IS
  v_max_asofdate DATE := PKG_BH_COMMONS.F_GET_MAX_ASOFDATE_BH_RPL();
  
  CURSOR c1
  IS SELECT *
       FROM BOOK_HIERARCHY_RPL
      WHERE PKG_BH_COMMONS.F_IS_MANUAL(DATA_SOURCE) = 0
        AND asofdate = v_max_asofdate;
        
  v_bh_intermediary_newpk NUMBER;
  v_bh_rpl_row BOOK_HIERARCHY_RPL%ROWTYPE;
BEGIN
  OPEN c1;
  
  LOOP
    FETCH c1 INTO v_bh_rpl_row;
    EXIT WHEN c1%NOTFOUND;
    
    v_bh_intermediary_newpk := SEQ_BH_INTERMEDIARY.NEXTVAL;
    
    INSERT INTO BH_STAGING(
          ASOFDATE,
          BOOK_ID,
          VOLCKER_TRADING_DESK,
          VOLCKER_TRADING_DESK_FULL,
          LOWEST_LEVEL_RPL_CODE,
          LOWEST_LEVEL_RPL_FULL_NAME,
          LOWEST_LEVEL_RPL,
          SOURCE_SYSTEM, 
          LEGAL_ENTITY,
          GLOBAL_TRADER_BOOK_ID,
          PROFIT_CENTER_ID,
          COMMENTS,
          DATA_SOURCE,
          CREATE_DATE,
          LAST_MODIFIED_DATE,
          CHARGE_REPORTING_UNIT_CODE,
          CHARGE_REPORTING_UNIT,
          CHARGE_REPORTING_PARENT_CODE,
          CHARGE_REPORTING_PARENT, 
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          MI_LOCATION,
          UBR_LEVEL_1_ID,
          UBR_LEVEL_1_NAME,
          UBR_LEVEL_1_RPL_CODE,
          UBR_LEVEL_2_ID,
          UBR_LEVEL_2_NAME,
          UBR_LEVEL_2_RPL_CODE,
          UBR_LEVEL_3_ID,
          UBR_LEVEL_3_NAME,
          UBR_LEVEL_3_RPL_CODE,
          UBR_LEVEL_4_ID,
          UBR_LEVEL_4_NAME,
          UBR_LEVEL_4_RPL_CODE,
          UBR_LEVEL_5_ID,
          UBR_LEVEL_5_NAME,
          UBR_LEVEL_5_RPL_CODE,
          UBR_LEVEL_6_ID,
          UBR_LEVEL_6_NAME,
          UBR_LEVEL_6_RPL_CODE,
          UBR_LEVEL_7_ID,
          UBR_LEVEL_7_NAME,
          UBR_LEVEL_7_RPL_CODE,
          UBR_LEVEL_8_ID,
          UBR_LEVEL_8_NAME,
          UBR_LEVEL_8_RPL_CODE,
          UBR_LEVEL_9_ID,
          UBR_LEVEL_9_NAME,
          UBR_LEVEL_9_RPL_CODE,
          UBR_LEVEL_10_ID,
          UBR_LEVEL_10_NAME,
          UBR_LEVEL_10_RPL_CODE,
          UBR_LEVEL_11_ID,
          UBR_LEVEL_11_NAME,
          UBR_LEVEL_11_RPL_CODE,
          UBR_LEVEL_12_ID,
          UBR_LEVEL_12_NAME,
          UBR_LEVEL_12_RPL_CODE,
          UBR_LEVEL_13_ID,
          UBR_LEVEL_13_NAME,
          UBR_LEVEL_13_RPL_CODE,
          UBR_LEVEL_14_ID,
          UBR_LEVEL_14_NAME,
          UBR_LEVEL_14_RPL_CODE,
          DESK_LEVEL_1_ID,
          DESK_LEVEL_1_NAME,
          DESK_LEVEL_1_RPL_CODE,
          DESK_LEVEL_2_ID,
          DESK_LEVEL_2_NAME,
          DESK_LEVEL_2_RPL_CODE,
          DESK_LEVEL_3_ID,
          DESK_LEVEL_3_NAME,
          DESK_LEVEL_3_RPL_CODE,
          DESK_LEVEL_4_ID,
          DESK_LEVEL_4_NAME,
          DESK_LEVEL_4_RPL_CODE,
          DESK_LEVEL_5_ID,
          DESK_LEVEL_5_NAME,
          DESK_LEVEL_5_RPL_CODE,
          PORTFOLIO_ID,
          PORTFOLIO_NAME,
          PORTFOLIO_RPL_CODE,
          BUSINESS,
          SUB_BUSINESS,
          CREATE_USER,
          LAST_MODIFICATION_USER,
          REGION,
          SUBREGION,
          --BH_STAGING extra fields
          OVERRIDDEN_FLAG,
          ACTIVE_FLAG,
          EMERGENCY_FLAG,
          BH_INTERMEDIARY_ID,
          APPROVER_USER,
          APPROVAL_DATE,
          --start GBSVR-28787
          ACC_TREAT_CATEGORY,
          PRIMARY_TRADER,
          PRIMARY_BOOK_RUNNER,
          PRIMARY_FINCON,
          PRIMARY_MOESCALATION,
          LEGAL_ENTITY_CODE,
          BOOK_FUNCTION_CODE,
          REGULATORY_REPORTING_TREATMENT,
          UBR_MA_CODE,
          HIERARCHY_UBR_NODENAME,
          PROFIT_CENTRE_NAME,
          --end GBSVR-28787
          NON_VTD_CODE,
          NON_VTD_NAME,
          NON_VTD_RPL_CODE,
          NON_VTD_EXCLUSION_TYPE,
           --start GBSVR-30678
           VOLCKER_REPORTABLE_FLAG,
          --end GBSVR-30678
           --start GBSVR-30224
          non_vtd_division,  
	      non_vtd_pvf, 
	      non_vtd_business
	       --end GBSVR-30224
        ) values (
          to_date(last_day(current_date)+1, 'DD-MON-YY'),
          v_bh_rpl_row.BOOK_ID,
          v_bh_rpl_row.VOLCKER_TRADING_DESK,
          v_bh_rpl_row.VOLCKER_TRADING_DESK_FULL,
          v_bh_rpl_row.LOWEST_LEVEL_RPL_CODE,
          v_bh_rpl_row.LOWEST_LEVEL_RPL_FULL_NAME,
          v_bh_rpl_row.LOWEST_LEVEL_RPL,
          v_bh_rpl_row.SOURCE_SYSTEM,
          v_bh_rpl_row.LEGAL_ENTITY,
          v_bh_rpl_row.GLOBAL_TRADER_BOOK_ID,
          v_bh_rpl_row.PROFIT_CENTER_ID,
          v_bh_rpl_row.COMMENTS,
          v_bh_rpl_row.DATA_SOURCE,
          v_bh_rpl_row.CREATE_DATE,
          v_bh_rpl_row.LAST_MODIFIED_DATE,
          v_bh_rpl_row.CHARGE_REPORTING_UNIT_CODE,
          v_bh_rpl_row.CHARGE_REPORTING_UNIT,
          v_bh_rpl_row.CHARGE_REPORTING_PARENT_CODE,
          v_bh_rpl_row.CHARGE_REPORTING_PARENT,
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
          v_bh_rpl_row.MI_LOCATION,
          v_bh_rpl_row.UBR_LEVEL_1_ID,
          v_bh_rpl_row.UBR_LEVEL_1_NAME,
          v_bh_rpl_row.UBR_LEVEL_1_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_2_ID,
          v_bh_rpl_row.UBR_LEVEL_2_NAME,
          v_bh_rpl_row.UBR_LEVEL_2_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_3_ID,
          v_bh_rpl_row.UBR_LEVEL_3_NAME,
          v_bh_rpl_row.UBR_LEVEL_3_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_4_ID,
          v_bh_rpl_row.UBR_LEVEL_4_NAME,
          v_bh_rpl_row.UBR_LEVEL_4_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_5_ID,
          v_bh_rpl_row.UBR_LEVEL_5_NAME,
          v_bh_rpl_row.UBR_LEVEL_5_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_6_ID,
          v_bh_rpl_row.UBR_LEVEL_6_NAME,
          v_bh_rpl_row.UBR_LEVEL_6_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_7_ID,
          v_bh_rpl_row.UBR_LEVEL_7_NAME,
          v_bh_rpl_row.UBR_LEVEL_7_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_8_ID,
          v_bh_rpl_row.UBR_LEVEL_8_NAME,
          v_bh_rpl_row.UBR_LEVEL_8_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_9_ID,
          v_bh_rpl_row.UBR_LEVEL_9_NAME,
          v_bh_rpl_row.UBR_LEVEL_9_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_10_ID,
          v_bh_rpl_row.UBR_LEVEL_10_NAME,
          v_bh_rpl_row.UBR_LEVEL_10_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_11_ID,
          v_bh_rpl_row.UBR_LEVEL_11_NAME,
          v_bh_rpl_row.UBR_LEVEL_11_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_12_ID,
          v_bh_rpl_row.UBR_LEVEL_12_NAME,
          v_bh_rpl_row.UBR_LEVEL_12_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_13_ID,
          v_bh_rpl_row.UBR_LEVEL_13_NAME,
          v_bh_rpl_row.UBR_LEVEL_13_RPL_CODE,
          v_bh_rpl_row.UBR_LEVEL_14_ID,
          v_bh_rpl_row.UBR_LEVEL_14_NAME,
          v_bh_rpl_row.UBR_LEVEL_14_RPL_CODE,
          v_bh_rpl_row.DESK_LEVEL_1_ID,
          v_bh_rpl_row.DESK_LEVEL_1_NAME,
          v_bh_rpl_row.DESK_LEVEL_1_RPL_CODE,
          v_bh_rpl_row.DESK_LEVEL_2_ID,
          v_bh_rpl_row.DESK_LEVEL_2_NAME,
          v_bh_rpl_row.DESK_LEVEL_2_RPL_CODE,
          v_bh_rpl_row.DESK_LEVEL_3_ID,
          v_bh_rpl_row.DESK_LEVEL_3_NAME,
          v_bh_rpl_row.DESK_LEVEL_3_RPL_CODE,
          v_bh_rpl_row.DESK_LEVEL_4_ID,
          v_bh_rpl_row.DESK_LEVEL_4_NAME,
          v_bh_rpl_row.DESK_LEVEL_4_RPL_CODE,
          v_bh_rpl_row.DESK_LEVEL_5_ID,
          v_bh_rpl_row.DESK_LEVEL_5_NAME,
          v_bh_rpl_row.DESK_LEVEL_5_RPL_CODE,
          v_bh_rpl_row.PORTFOLIO_ID,
          v_bh_rpl_row.PORTFOLIO_NAME,
          v_bh_rpl_row.PORTFOLIO_RPL_CODE,
          v_bh_rpl_row.BUSINESS,
          v_bh_rpl_row.SUB_BUSINESS,
          v_bh_rpl_row.CREATE_USER,
          v_bh_rpl_row.LAST_MODIFICATION_USER,
          v_bh_rpl_row.REGION,
          v_bh_rpl_row.SUBREGION,
          --BH_STAGING extra fields
          NULL,
          'Y', --ACTIVE_FLAG='Y'
          NULL,
          v_bh_intermediary_newpk,
          v_bh_rpl_row.APPROVER_USER,
          v_bh_rpl_row.APPROVAL_DATE,
          --start GBSVR-28787
          v_bh_rpl_row.ACC_TREAT_CATEGORY,
          v_bh_rpl_row.PRIMARY_TRADER,
          v_bh_rpl_row.PRIMARY_BOOK_RUNNER,
          v_bh_rpl_row.PRIMARY_FINCON,
          v_bh_rpl_row.PRIMARY_MOESCALATION,
          v_bh_rpl_row.LEGAL_ENTITY_CODE,
          v_bh_rpl_row.BOOK_FUNCTION_CODE,
          v_bh_rpl_row.REGULATORY_REPORTING_TREATMENT,
          v_bh_rpl_row.UBR_MA_CODE,
          v_bh_rpl_row.HIERARCHY_UBR_NODENAME,
          v_bh_rpl_row.PROFIT_CENTRE_NAME,
          --end GBSVR-28787
          --start GBSVR-29099
           v_bh_rpl_row.NON_VTD_CODE,
           v_bh_rpl_row.NON_VTD_NAME,
           v_bh_rpl_row.NON_VTD_RPL_CODE,
           v_bh_rpl_row.NON_VTD_EXCLUSION_TYPE,
           --start GBSVR-30678
            v_bh_rpl_row.VOLCKER_REPORTABLE_FLAG,
          --end GBSVR-30678
               --end GBSVR-29099
               --start GBSVR-30224
          v_bh_rpl_row.non_vtd_division,  
	      v_bh_rpl_row.non_vtd_pvf, 
	      v_bh_rpl_row.non_vtd_business
	       --end GBSVR-30224   
	        );
      
      
        
      --Create BH_INTERMEDIARY record for inserted data (bh_intermediary_id = null)
      --so the conflicts report can show both bRDS and manual records in conflict
      INSERT INTO BH_INTERMEDIARY(
          ID,
          UPLOAD_ID,
          ACTION_ID,
          BOOK_ID,
          VOLCKER_TRADING_DESK,
          SOURCE_SYSTEM_ID,
          GLOBAL_TRADER_BOOK_ID,
          EMERGENCY_FLAG,
          CHARGE_REPORTING_UNIT_CODE,
          CHARGE_REPORTING_PARENT_CODE,
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          VALIDATION_MESSAGE,
          BUSINESS,
          SUB_BUSINESS,
          COMMENTS,
          ACTION_NAME,
          REGULATORY_REPORTING_TREATMENT,
          NON_VTD_RPL_CODE  ) --start/end GBSVR-21978 
      VALUES (
          v_bh_intermediary_newpk,
          null,
          1,--ADD
          v_bh_rpl_row.BOOK_ID,
          v_bh_rpl_row.VOLCKER_TRADING_DESK,
          v_bh_rpl_row.SOURCE_SYSTEM,
          v_bh_rpl_row.GLOBAL_TRADER_BOOK_ID,
          'N',--EMERGENCY_FLAG,
          v_bh_rpl_row.CHARGE_REPORTING_UNIT_CODE,
          v_bh_rpl_row.CHARGE_REPORTING_PARENT_CODE,
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          'VALIDATION OK',--VALIDATION_MESSAGE,
          v_bh_rpl_row.BUSINESS,
          v_bh_rpl_row.SUB_BUSINESS,
          v_bh_rpl_row.COMMENTS,
          'ADD',--start/end GBSVR-21978
          v_bh_rpl_row.REGULATORY_REPORTING_TREATMENT,
          v_bh_rpl_row.NON_VTD_RPL_CODE
            );
       
  END LOOP;
  


-- ***************************************************************** 
-- Data fixes to clear up Manuals:

update    bh_staging                                -- New update required as otherwise the following update will fail. 
set       data_source = 'MANUAL'
where     nvl(data_source, ' ') NOT in ( 'bRDS', 'MANUAL' );


delete
from    bh_staging  s6
where   ( s6.book_id, s6.volcker_trading_desk, s6.last_modified_date ) in
(
select  s5.*
from 
(
select  s4.book_id,               -- 1. s5: ALL book/vtd/last_modified_date combos. 
        s4.volcker_trading_desk, 
        s4.last_modified_date
from    bh_staging  s4
) s5, 
(
select  s1.book_id,               
        s1.volcker_trading_desk,  -- 2. s3: Multi SS combos from 1: Get the min date: 
        min(s1.last_modified_date) min_date
from    bh_staging  s1
where   ( s1.book_id, s1.volcker_trading_desk ) in
(
select  s2.book_id,               -- 3. Required for s3: book/vtd combos with more than one source_system. 
        s2.volcker_trading_desk
from    bh_staging  s2
group by 
        s2.book_id, 
        s2.volcker_trading_desk
-- GBSVR-26583: Start 1: 
having  count( distinct nvl(s2.source_system, ' ') ) > 1
-- GBSVR-26583: End 1: 
)
group by s1.book_id, s1.volcker_trading_desk
) s3
where   s5.book_id = s3.book_id
and     s5.volcker_trading_desk = s3.volcker_trading_desk
and     s5.last_modified_date != s3.min_date
);


update  bh_staging  s1
set     s1.source_system = NULL
where   s1.data_source = 'MANUAL'
and     s1.book_id NOT in 
(
select  s2.book_id
from    bh_staging  s2
where   s2.data_source = 'MANUAL'
group by 
        s2.book_id
having  count( distinct nvl(s2.volcker_trading_desk, ' ') ) > 1
and     count( distinct nvl(s2.source_system, ' ') ) > 1          -- Add this: We want same book_id, DIFFERENT VTD and DIFFERENT SS
);


update  bh_intermediary  s1
set     s1.source_system_id = NULL
where   s1.book_id NOT in 
(
select  s2.book_id
from    bh_staging  s2
where   s2.data_source = 'MANUAL'
and     s2.source_system is NOT NULL
group by 
        s2.book_id
having  count( distinct nvl(s2.volcker_trading_desk, ' ') ) > 1
and     count( distinct nvl(s2.source_system, ' ') ) > 1          -- Add this: We want same book_id, DIFFERENT VTD and DIFFERENT SS
);



-- Update CFBU values on Manuals from corresponding bRDS entries: Otherwise we will have no matches: 
-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning


-- Manuals with source system set should NOT have a gtbId value: Set to Null: 
update  bh_staging 
   set  global_trader_book_id = NULL 
 where  data_source = 'MANUAL' 
   and  source_system is not NULL 
   and  source_system != 'null'
   and  global_trader_book_id is not NULL;

-- GBSVR-26583: Start 2: 
-- ***************************************************************** 
-- Remove duplicates: book_id, VTD same, SS Null: Keep only the most recent entry and remove the others: 

delete 
from    bh_staging  s1
where   s1.source_system is NULL 
and     s1.data_source = 'MANUAL'
and     s1.book_id in ( 
  select  s.book_id
  from    bh_staging s
  where   s.source_system is NULL 
  and     s.data_source = 'MANUAL'
  group by s.book_id  
  having count(*) > 1 and count(distinct nvl(s.volcker_trading_desk, ' ')) = 1
  )
and     s1.last_modified_date NOT in  ( 
  select  max(s2.last_modified_date) 
  from    bh_staging s2
  where   s2.source_system is NULL 
  and     s2.data_source = 'MANUAL'
  and     s2.book_id = s1.book_id
); 

-- GBSVR-26583: End 2: 
  
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS.P_BH_INITIAL_LOADING', 'ERROR', 'LOGGING', SQLERRM, 'Initial loading failed.', 'bRDS');
  raise;
END P_BH_INITIAL_LOADING;

FUNCTION F_BH_LOAD_FILE_IN_STAGING(P_UPLOAD_ID IN NUMBER) RETURN BOOLEAN
IS
  cursor c1 is
      SELECT *
      FROM BH_INTERMEDIARY
      WHERE upload_id = P_UPLOAD_ID;
  success BOOLEAN := TRUE;
  v_validation_message VARCHAR2(4000);
  v_cont_error INTEGER;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  v_cont_error:=0;
  
  FOR v_intermediary in c1
  LOOP
      PKG_BH_COMMONS.P_VALIDATION_LOAD_APPROVE(v_intermediary,PROCESS_APPROVAL,v_validation_message,v_cont_error);
      IF v_cont_error > 0 THEN
        success := FALSE;
        UPDATE bh_intermediary
           SET validation_message = 'VALIDATION KO. ' || v_validation_message
         WHERE id = v_intermediary.id;
		 pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'F_BH_LOAD_FILE_IN_STAGING','DEBUG', 'LOGGING', 'success VALIDATION KO', '', 'bRDS');
      END IF;
  END LOOP;
  
  IF success = TRUE THEN
      --Transform each row
      FOR v_intermediary in c1
      LOOP
          --start GBSVR-23447
          BEGIN
            success := PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGING(v_intermediary.id);
          EXCEPTION
            WHEN OTHERS THEN
              success := FALSE;
          END;
          IF success = FALSE THEN
            pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'F_BH_LOAD_FILE_IN_STAGING','DEBUG', 'LOGGING', 'success false when calling F_BH_INTERMEDIARY_TO_STAGING', '', 'bRDS');
            EXIT;
          END IF;
          --end GBSVR-23447
      END LOOP;
  ELSE
      P_BH_UI_UPDATE_CSV(P_UPLOAD_ID);
  END IF;
  
  RETURN success;
END F_BH_LOAD_FILE_IN_STAGING;

PROCEDURE P_BH_GET_CONFLICT_DATA(p_conflict_id IN NUMBER, p_result OUT SYS_REFCURSOR)
IS
BEGIN  
  open p_result for
    SELECT 'bRDS' datasource, book_id, global_trader_book_id, source_system source_system_id, volcker_trading_desk, 
          charge_reporting_unit_code, charge_reporting_parent_code, 
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          business, sub_business
            -- start 1: GBSVR-27301  modify GBSVR-24017 Exception Cause field in Conflicts screen is no more neccesary
          -- end 1: GBSVR-27301  modify GBSVR-24017 Exception Cause field in Conflicts screen is no more neccesary
      FROM BH_CONFLICTS
     WHERE id = p_conflict_id
    UNION ALL
    SELECT 'MANUAL' datasource, i.book_id, i.global_trader_book_id, i.source_system_id, i.volcker_trading_desk, 
          b.charge_reporting_unit_code, b.charge_reporting_parent_code, 
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          b.business, b.sub_business           
            -- start 2: GBSVR-27301  modify GBSVR-24017 Exception Cause field in Conflicts screen is no more neccesary
          -- end 2: GBSVR-27301  modify GBSVR-24017 Exception Cause field in Conflicts screen is no more neccesary
      FROM BH_STAGING b, BH_INTERMEDIARY i, BH_CONFLICTS c
     WHERE c.id = p_conflict_id
       AND b.bh_intermediary_id = i.id
       AND c.bh_intermediary_id = i.id;
END P_BH_GET_CONFLICT_DATA;

--start GBSVR-31206
--start GBSVR-31380
PROCEDURE P_BH_UI_UPDATE_CSV(P_UI_UPLOAD IN NUMBER)
IS
  v_clob clob;
  csv_header varchar2(200);
  v_legal_entity_code bh_intermediary.legal_entity_code%TYPE;
  v_bh_intermediary bh_intermediary%rowtype;     
  
  cursor c_csv (P_UI_UPLOAD IN number) is
       select *
         from bh_intermediary
        where upload_id = P_UI_UPLOAD;  
BEGIN 
  
  v_clob:='';
  
  OPEN c_csv(P_UI_UPLOAD);
  
  LOOP
      FETCH c_csv into v_bh_intermediary;
       EXIT WHEN c_csv%notfound;
      --remove possible "new line" character after last column in the template
      v_legal_entity_code := REPLACE(v_bh_intermediary.legal_entity_code, chr(13));
      v_legal_entity_code := REPLACE(v_bh_intermediary.legal_entity_code, chr(10));
      --end GBSVR-31380 : change regulatory_reporting_treatment for legal_entity_code
      v_clob := v_clob || 
        v_bh_intermediary.action_name||',' ||
        v_bh_intermediary.emergency_flag || ',' ||
        v_bh_intermediary.book_id ||','|| 
        v_bh_intermediary.global_trader_book_id ||','||
        v_bh_intermediary.source_system_id ||','|| 
        v_bh_intermediary.volcker_trading_desk ||','||
        v_bh_intermediary.charge_reporting_unit_code ||','||
        v_bh_intermediary.charge_reporting_parent_code||','||
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        v_bh_intermediary.business ||','||
        v_bh_intermediary.sub_business ||','||
        v_bh_intermediary.comments ||','||
        v_bh_intermediary.non_vtd_rpl_code ||','||
        v_bh_intermediary.regulatory_reporting_treatment ||','||
        v_bh_intermediary.legal_entity ||','||
        v_bh_intermediary.legal_entity_code ||','||	
        v_bh_intermediary.validation_message;        
      v_clob := v_clob  || chr(13) || chr(10);
  END LOOP;
  
  CLOSE c_csv;
  
  update REF_DATA_UI_UPLOAD
  set csv = v_clob
  where id = P_UI_UPLOAD;

END P_BH_UI_UPDATE_CSV;
--end GBSVR-31206

PROCEDURE P_GET_BH_WORKFLOW(P_WORKFLOW_TYPE_ID IN VARCHAR2,P_CONFLICTS_CSV OUT CLOB)
IS
	csv_header varchar2(200);
	v_conflicts_csv clob;
	v_counter number;

	v_asofdate bh_workflow.asofdate%TYPE;
	v_workflow_type_id bh_workflow.WORKFLOW_TYPE_ID%TYPE;
	v_book_id bh_workflow.BOOK_ID%TYPE;
	v_global_trader_book_id bh_workflow.GLOBAL_TRADER_BOOK_ID%TYPE;
	v_volcker_trading_desk bh_workflow.VOLCKER_TRADING_DESK%TYPE;
	v_charge_reporting_unit bh_workflow.CHARGE_REPORTING_UNIT%TYPE;
	v_charge_reporting_parent bh_workflow.CHARGE_REPORTING_PARENT%TYPE;
	v_data_source bh_workflow.DATA_SOURCE%TYPE;
	v_source_system_id bh_workflow.SOURCE_SYSTEM_ID%TYPE;
	v_comments bh_workflow.COMMENTS%TYPE;
      
    cursor c_bh_workflow (c_p_workflow_type_id IN varchar2) is
    select asofdate,workflow_type_id,book_id,global_trader_book_id,volcker_trading_desk,charge_reporting_unit,charge_reporting_parent,data_source,source_system_id,comments 
     from bh_workflow where workflow_type_id in (SELECT DISTINCT regexp_substr(c_p_workflow_type_id,'[^,]+', 1, level) 
        FROM dual
          CONNECT BY regexp_substr(c_p_workflow_type_id, '[^,]+', 1, level) IS NOT NULL);
	 
BEGIN

    v_counter:=0;
	csv_header:='"As of Date","Workflow type","Book ID","Global Trader Book ID","Volcker Trading Desk","Charge Reporting Unit","Charge Reporting Parent","DataSource","Source System","Comments"';
	P_CONFLICTS_CSV:='';
	v_conflicts_csv:='';
	v_conflicts_csv:= csv_header;
	v_conflicts_csv := v_conflicts_csv  || chr(13) || chr(10)  ;


	OPEN c_bh_workflow(P_WORKFLOW_TYPE_ID);

	LOOP

      FETCH c_bh_workflow into v_asofdate,v_workflow_type_id,v_book_id,v_global_trader_book_id,v_volcker_trading_desk,v_charge_reporting_unit,v_charge_reporting_parent,
      v_data_source,v_source_system_id,v_comments;
      EXIT WHEN c_bh_workflow%notfound;
      

      v_conflicts_csv := v_conflicts_csv 
      ||'"'|| to_char(v_asofdate,'DD-MON-YY') ||'","'
      ||to_clob(v_workflow_type_id) ||'","'|| v_book_id ||'","'
      || v_global_trader_book_id ||'","'|| v_volcker_trading_desk ||'","'|| v_charge_reporting_unit ||'","'|| v_charge_reporting_parent ||'","'
      ||v_data_source ||'","'|| v_source_system_id ||'","'|| v_comments||'"';
      v_conflicts_csv := v_conflicts_csv  || chr(13) || chr(10)  ;
	  v_counter:=v_counter+1;
	  if(v_counter>1000)
	  then
	  P_CONFLICTS_CSV:=P_CONFLICTS_CSV||v_conflicts_csv;
	  v_conflicts_csv:='';
	   v_counter:=0;
	  end if;
      
	END LOOP;

P_CONFLICTS_CSV:=P_CONFLICTS_CSV||v_conflicts_csv;

END P_GET_BH_WORKFLOW;

FUNCTION F_IS_EMPTY_RECORD (p_excel_record IN csv_table_array, p_total_columns IN NUMBER) RETURN BOOLEAN
IS
  v_is_empty_record BOOLEAN := TRUE;
  v_cell_value VARCHAR2(4000);
  -- start 1 GBSVR-26874:
  v_is_empty_cell BOOLEAN := TRUE;
  -- start 1 GBSVR-26874:
  v_cell_value_size NUMBER;
  v_count_empty  NUMBER :=0;
BEGIN
-- start 2 GBSVR-26874:
 FOR i in 1 .. p_total_columns LOOP
  	v_is_empty_cell:= TRUE;
    v_cell_value := F_FORMAT_COLUMN_CSV(p_excel_record(i));
    IF v_cell_value IS NOT NULL THEN
      v_cell_value_size := LENGTH(v_cell_value);
      FOR j in 1 .. v_cell_value_size LOOP
        IF ASCII(substr(v_cell_value,j,j+1)) NOT IN (9, 10, 13, 32) THEN --Tabulator, new line, carriage return, blank space
          v_is_empty_cell := FALSE;
          EXIT;
        END IF;
      END LOOP;
    ELSE
    	v_is_empty_cell := TRUE;	
    END IF;
    
    v_is_empty_record := (v_is_empty_record and v_is_empty_cell );
    
    IF not v_is_empty_record THEN      
      EXIT;
    END IF;
  END LOOP;
  -- start 2 GBSVR-26874:
  RETURN v_is_empty_record;
  
  END F_IS_EMPTY_RECORD;

-- **********************************************************************
-- Procedure: P_SPLIT_CSV
-- **********************************************************************
PROCEDURE P_SPLIT_CSV_PROCESS (idUpload IN NUMBER)
IS
   csv                  REF_DATA_UI_UPLOAD.csv%TYPE;
   csv_line             LONG;
   csv_line_out         CLOB;
   csv_line_error       CLOB;
   csv_result           CLOB;
   csv_field            LONG;
   cont_x               INTEGER;
   cont_y               INTEGER;
   total_rows           INTEGER;
   v_intermediary_data  bh_intermediary%ROWTYPE;

   CURSOR c1 IS SELECT COLUMN_VALUE FROM TABLE (f_convert_rows (csv));
   CURSOR c2 IS SELECT COLUMN_VALUE FROM TABLE (f_convert_row (csv_line, ','));

   csv_table                        csv_table_type;
   action_id                        bh_action_lookup.id%TYPE;
   validation_msg                   VARCHAR2 (4000);
   aux_validation_msg               VARCHAR2 (4000);
   validation_code                  NUMBER;
   b_validation                     BOOLEAN;
   action                           bh_action_lookup.name%TYPE;
   --start GBSVR-28969
   cont_error_lines                 INTEGER := 0; --Number of lines with errores
   cont_error                       INTEGER; --Number of errors of a single line
   aux_cont_error                   INTEGER; --Number of errors for validation in PKHG_BH_COMMONS.P_VALIDATION_LOAD_APPROVE
   --end GBSVR-28969
   emergency_flag                   VARCHAR2 (500);-- start / end GBSVR-21978
   b_emergency_flag                 BOOLEAN;
    -- start GBSVR-21978
   action_name			VARCHAR2(500);
   book_id               VARCHAR2(500);
   vtd                   VARCHAR2(500);
   asofdate              DATE;
   b_action              BOOLEAN;
   source_system         VARCHAR2(500);
   gtb                   VARCHAR2(500);
   cru                   VARCHAR2(500);
   crp                   VARCHAR2(500);
   -- GBSVR-33754 Start: CFBU decommissioning
   -- GBSVR-33754 End:   CFBU decommissioning
   business              VARCHAR2(500);
   sub_business          VARCHAR2(500);
   comments              VARCHAR2(500);
   non_vtd_rpl_code      VARCHAR2(500);
   regulatory_reporting_treatment  VARCHAR2(500);

   status                NUMBER;
   validation_result     VARCHAR2(10);
   upload_error          VARCHAR2(4000);
    -- end GBSVR-21978
   c_error_log                      CLOB;
   -- start GBSVR-31380
   c_legal_entity          VARCHAR2(500);
   c_legal_entity_code     VARCHAR2(500);
   -- GBSVR-33754 Start: CFBU decommissioning
   total_colum_valid       CONSTANT INTEGER := 15; -- Increase fields number from 14 to 16: 2 new fields
   pos_validation_msg      CONSTANT INTEGER := 16; -- Increase fields number from 15 to 17: 2 new fields
   pos_validation_result   CONSTANT INTEGER := 17; -- Increase fields number from 16 to 18: 2 new fields
   total_colum             CONSTANT INTEGER := 17; -- Increase fields number from 16 to 18: 2 new fields
   -- GBSVR-33754 end: CFBU decommissioning
   -- end GBSVR-31380
   b_csv_true                       BOOLEAN;
   
   -- start GBSVR-21978  
   --start GBSVR-27301: changed C_SIZE_VTD/GTB/CRU/CFBU/BUSINESS/SUBBUSINESS from 50 to 100
  C_SIZE_EMERGENCY_FLAG CONSTANT INTEGER:=1;
  C_SIZE_EMERGENCY_FLAG_ERROR CONSTANT INTEGER:=500;
  C_SIZE_BOOK_ID CONSTANT INTEGER:=50;
  C_SIZE_BOOK_ID_ERROR CONSTANT INTEGER:=500;
  C_SIZE_ACTION CONSTANT INTEGER:=128;
  C_SIZE_ACTION_NAME_ERROR CONSTANT INTEGER:=500;
  C_SIZE_VTD CONSTANT INTEGER:=100;
  C_SIZE_VTD_ERROR CONSTANT INTEGER:=500;
  C_SIZE_SOURCE_SYSTEM CONSTANT INTEGER:=50;
  C_SIZE_SOURCE_SYSTEM_ERROR CONSTANT INTEGER:=500;
  C_SIZE_GTB CONSTANT INTEGER:=100;
  C_SIZE_GTB_ERROR CONSTANT INTEGER:=500;
  C_SIZE_CRU CONSTANT INTEGER:=100;
  C_SIZE_CRU_ERROR CONSTANT INTEGER:=500;
  C_SIZE_CRP CONSTANT INTEGER:=100;
  C_SIZE_CRP_ERROR CONSTANT INTEGER:=500;
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  C_SIZE_BUSINESS CONSTANT INTEGER:=100;
  C_SIZE_BUSINESS_ERROR CONSTANT INTEGER:=500;
  C_SIZE_SUB_BUSINESS CONSTANT INTEGER:=100;
  C_SIZE_SUB_BUSINESS_ERROR CONSTANT INTEGER:=500;
  C_SIZE_COMMENTS CONSTANT INTEGER:=300;
  C_SIZE_COMMENTS_ERROR CONSTANT INTEGER:=500;
  -- end GBSVR-27301 
  -- end GBSVR-21978 
  -- start GBSVR-28882 
   C_SIZE_NON_VTD_RPL_CODE CONSTANT INTEGER:=100;
   C_SIZE_NON_VTD_RPL_CODE_ERROR CONSTANT INTEGER:=500;
   C_SIZE_REGULATORY_TREATMENT CONSTANT INTEGER:=1;
   C_SIZE_REGU_TREATMENT_ERROR CONSTANT INTEGER:=50;
  -- end GBSVR-28882 
  -- start GBSVR-31380 
   C_SIZE_LEGAL_ENTITY CONSTANT INTEGER:=200;
   C_SIZE_LEGAL_ENTITY_ERROR CONSTANT INTEGER:=500;
   C_SIZE_LEGAL_ENTITY_CODE CONSTANT INTEGER:=100;
   C_SIZE_LEGAL_ENTITY_CODE_ERROR CONSTANT INTEGER:=500;
  -- end GBSVR-31380   
   field_value                      VARCHAR2 (4000);
   PROCESS_UPLOAD          CONSTANT INT := 1;
   v_book_id_searched_in_bRDS       BOOLEAN := FALSE;
   C_EMPTY_LINE            CONSTANT VARCHAR2(20) := '[EMPTY_LINE]';
   cont_empty                       INTEGER := 0;
   l_clob							CLOB;-- start / end 1 GBSVR-24430
   FIELDS_LESS_THAN_EXPECTED EXCEPTION;-- start / end 1 GBSVR-21252
   -- start GBSVR-28392
   v_validation_message   VARCHAR2(4000);
   v_output_line_csv clob;
   -- end 1 GBSVR-28392
   --start GBSVR-28837
   v_status_processing_validation CONSTANT INTEGER := 11;
   --end GBSVR-28837
   -- start GBSVR-31380
   MORE_FIELDS_THAN_EXPECTED EXCEPTION;
   --end GBSVR-31380
BEGIN
   IF P_PROCESS_ETL_STATUS(IDUPLOAD, 1) = FALSE THEN
     RETURN;
   END IF;

   --start GBSVR-28837
   UPDATE REF_DATA_UI_UPLOAD
      SET STATUS_ID = v_status_processing_validation,
          COMMENTS = 'Processing validation'
   WHERE ID = IDUPLOAD;
   IF SQL%ROWCOUNT = 0 THEN
      RAISE NO_DATA_FOUND;
   END IF;
   COMMIT;
   --end GBSVR-28837
   
   -- control csv when the csv row has bad format
   b_csv_true := TRUE;
   cont_error := 0;
   csv_table := csv_table_type ();

   SELECT csv INTO csv
     FROM REF_DATA_UI_UPLOAD REF_DATA_UI_UPLOAD
    WHERE REF_DATA_UI_UPLOAD.id = idUpload;

   -- count number of rows
   cont_x := 0;

   --1 read record and split the clob in rows within a bidimensional array
   -- 1.1- inizialitation number of lines
  --start 2: GBSVR-24430
   l_clob := csv||CHR(10);
   
   select length(l_clob) - length (replace(l_clob,CHR(10))) into cont_x from dual;
   --end 2: GBSVR-24430
   --DBMS_OUTPUT.put_line( 'number of rows: '|| cont_x);
   csv_table.EXTEND (cont_x);

   cont_x := 0;

   OPEN c1;

   --1.2 read record and split the clob in rows within a bidimensional array
   LOOP
      FETCH c1 INTO csv_line;

      EXIT WHEN c1%NOTFOUND;
      cont_y := 0;
      cont_x := cont_x + 1;
      -- init 16 fields (16 template + 1 validation message + 1 validation result)
      -- GBSVR-33754 Start: CFBU decommissioning
      csv_table(cont_x) := csv_table_array (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
      -- GBSVR-33754 End: CFBU decommissioning

      OPEN c2;

      LOOP
         FETCH c2 INTO csv_field;

         EXIT WHEN c2%NOTFOUND;
         cont_y := cont_y + 1;

         --If input excel file has 15 or more columns, force raising error (csv_table_array has 16 positions and won't be raised till number of columns >= 17)
         IF cont_y > total_colum_valid
         THEN
            RAISE MORE_FIELDS_THAN_EXPECTED;
         END IF;

         csv_table(cont_x)(cont_y) := csv_field;
      END LOOP;

      -- validate template should have 18 fields
      IF cont_y != total_colum_valid
      THEN
        RAISE FIELDS_LESS_THAN_EXPECTED;
       ELSE
         csv_table(cont_x)(pos_validation_result) := 'OK';
      END IF;

      CLOSE c2;
   END LOOP;

   CLOSE c1;

   total_rows := cont_x;

   FOR i IN 1 .. total_rows
   LOOP
      validation_msg := NULL;
      validation_code := NULL;
      cont_error := 0;
      b_csv_true := TRUE;
      --2 validate process
      -- validate format template is OK
      validation_result := csv_table(i)(pos_validation_result);

      --DBMS_OUTPUT.put_line( 'ACTION: '|| csv_table(i)(1));
      --DBMS_OUTPUT.put_line( 'EMERGENCY: '|| csv_table(i)(2));
      --DBMS_OUTPUT.put_line( 'BOOK ID: ' || csv_table(i)(3));
      --DBMS_OUTPUT.put_line( 'GLOBAL_TRADER_BOOK_ID: ' || csv_table(i)(4));
      --DBMS_OUTPUT.put_line( 'SOURCE_SYSTEM_ID: ' || csv_table(i)(5));
      --DBMS_OUTPUT.put_line( 'VOLCKER_TRADING_DESK: ' || csv_table(i)(6));
      --DBMS_OUTPUT.put_line( 'CHARGE_REPORTING_UNIT_CODE: ' || csv_table(i)(7));
      --DBMS_OUTPUT.put_line( 'CHARGE_REPORTING_PARENT_CODE: ' || csv_table(i)(8));
      --DBMS_OUTPUT.put_line( 'COVERED_FUNDS_UNITS: ' || csv_table(i)(9));
      --DBMS_OUTPUT.put_line( 'BUSINESS: ' || csv_table(i)(10));

--DBMS_OUTPUT.put_line( 'SUB_BUSINESS: ' || csv_table(i)(11));
      --DBMS_OUTPUT.put_line( 'COMMENTS: ' || csv_table(i)(12));
	  --DBMS_OUTPUT.put_line( 'NON-VTD RPL CODE: ' || csv_table(i)(13));
	  --DBMS_OUTPUT.put_line( 'REGULATORY TREATMENT: ' || csv_table(i)(14));
      
      --Mark as discarded and continue if it is an empty record
      IF F_IS_EMPTY_RECORD(csv_table(i), total_colum_valid) THEN
        csv_table(i)(pos_validation_msg) := C_EMPTY_LINE;
        --start GBSVR-28392
        cont_empty := cont_empty + 1;
        --end GBSVR-28392
        CONTINUE;
      END IF;
      
      
      --------------------- INIT VALIDATION CSV correct -----------------------------------------------------
      --- validate length each column
      BEGIN
          -- start GBSVR-21978
      -- action id
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(1));
      IF  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_ACTION THEN
	  	 IF   LENGTH( field_value ) > C_SIZE_ACTION_NAME_ERROR THEN
		 	action_name := SUBSTR(field_value, 1 , C_SIZE_ACTION_NAME_ERROR);
		 ELSE
		     action_name := field_value;
		 END IF;
        action := '?';
        validation_code:= 1007;
        validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
       action := field_value;
	   action_name := field_value;
      END IF;

         -- emergency value
         field_value:= F_FORMAT_COLUMN_CSV(csv_table(i)(2));
      IF  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_EMERGENCY_FLAG THEN
	  	 IF   LENGTH( field_value ) > C_SIZE_EMERGENCY_FLAG_ERROR THEN
		 	emergency_flag := SUBSTR(field_value, 1 , C_SIZE_EMERGENCY_FLAG_ERROR);
		 ELSE
		     emergency_flag := field_value;
		 END IF;
         validation_code:= 1006;
         validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
         cont_error     := cont_error + 1;
         b_csv_true := false;
	  ELSE 
		emergency_flag := field_value;
      END IF;

         -- book id
         field_value := F_FORMAT_COLUMN_CSV(csv_table(i)(3));

         IF field_value IS NOT NULL AND LENGTH (field_value) > C_SIZE_BOOK_ID THEN
            IF field_value IS NOT NULL AND LENGTH (field_value) > C_SIZE_BOOK_ID_ERROR
            THEN
               -- length max bh_intermediary tables and so avoid tec. error
               book_id := SUBSTR (field_value, 1, C_SIZE_BOOK_ID_ERROR);
            ELSE
               book_id := field_value;
            END IF;

            validation_code := 1008;
            validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            cont_error := cont_error + 1;
            b_csv_true := FALSE;
         ELSE
            book_id := field_value;
         END IF;

           -- vtd
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(6));
      IF  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_VTD THEN
	  	 IF  LENGTH( field_value ) > C_SIZE_VTD_ERROR THEN
		 	 vtd := SUBSTR(field_value, 1 , C_SIZE_VTD_ERROR);
		 ELSE
             vtd:=field_value;
		 END IF;
         validation_code:= 1011;
         validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
         cont_error     := cont_error + 1;
         b_csv_true := false;
	  ELSE
		 vtd:=field_value;
      END IF;

          -- source system
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(5));
      IF  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_SOURCE_SYSTEM THEN
         IF  LENGTH( field_value ) > C_SIZE_SOURCE_SYSTEM_ERROR THEN
		 	source_system := SUBSTR(field_value, 1 , C_SIZE_SOURCE_SYSTEM_ERROR);
		 ELSE
            source_system:=field_value;
		 END IF;
         validation_code:= 1010;
         validation_msg :=validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
         cont_error     := cont_error + 1;
         b_csv_true := false;
      ELSE
        source_system:=field_value;
      END IF; 

          -- gbt
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(4));
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_GTB THEN
         IF  LENGTH( field_value ) > C_SIZE_GTB_ERROR THEN
		 	gtb := SUBSTR(field_value, 1 , C_SIZE_GTB_ERROR);
		 ELSE
            gtb:= field_value;
		 END IF;
        validation_code:= 1009;
        validation_msg :=validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
        gtb:= field_value;
      END IF;

          -- cru
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(7));
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_CRU THEN
         IF  LENGTH( field_value ) > C_SIZE_CRU_ERROR THEN
		 	cru := SUBSTR(field_value, 1 , C_SIZE_CRU_ERROR);
		 ELSE
            cru:= field_value;
		 END IF;
         validation_code:= 1012;
         validation_msg :=validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
         cont_error     := cont_error + 1;
         b_csv_true := false;
      ELSE
          cru:= field_value;
      END IF;

          -- crp
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(8));
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_CRP THEN
	  	 IF LENGTH( field_value ) > C_SIZE_CRP_ERROR THEN
			 crp := SUBSTR(field_value, 1 , C_SIZE_CRP_ERROR);
		 ELSE
             crp:= field_value;
		 END IF;
        validation_code:= 1013;
        validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
        crp:= field_value;
      END IF;


      -- GBSVR-33754 Start: CFBU decommissioning
      -- GBSVR-33754 End:   CFBU decommissioning

         -- business
      -- GBSVR-35791 Start:   
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(9));
      -- GBSVR-35791 End: 
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_BUSINESS THEN
	  	IF LENGTH( field_value ) > C_SIZE_BUSINESS_ERROR THEN
			business := SUBSTR(field_value, 1 , C_SIZE_BUSINESS_ERROR);
		ELSE
            business:= field_value;
		END IF;
        validation_code:= 1015;
        validation_msg :=validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
          business:= field_value;
      END IF;
         -- sub-business
      -- GBSVR-35791 Start:   
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(10));
      -- GBSVR-35791 End:   
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_SUB_BUSINESS THEN
	  	IF LENGTH( field_value ) > C_SIZE_SUB_BUSINESS_ERROR THEN
			sub_business := SUBSTR(field_value, 1 , C_SIZE_SUB_BUSINESS_ERROR);
		ELSE
            sub_business:= field_value;
		END IF;
        validation_code:= 1016;
        validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
          sub_business:= field_value;
      END IF;

        -- comments
      -- GBSVR-35791 Start: 
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(11));
      -- GBSVR-35791 End: 
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_COMMENTS THEN
	  	IF LENGTH( field_value ) > C_SIZE_COMMENTS_ERROR THEN
			comments := SUBSTR(field_value, 1 , C_SIZE_COMMENTS_ERROR);
		ELSE
            comments:= field_value;
		END IF;
        validation_code:= 1017;
        validation_msg :=   validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
         comments:= field_value;
      END IF;
	  
  -- GBSVR-28882  NON-VTD
  
      -- GBSVR-35791 Start:   
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(12));
      -- GBSVR-35791 End:   
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_NON_VTD_RPL_CODE THEN
	  	IF LENGTH( field_value ) > C_SIZE_NON_VTD_RPL_CODE_ERROR THEN
			non_vtd_rpl_code := SUBSTR(field_value, 1 , C_SIZE_NON_VTD_RPL_CODE_ERROR);
		ELSE
            non_vtd_rpl_code:= field_value;
		END IF;
        validation_code:= 4001;
        validation_msg :=   validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
        non_vtd_rpl_code:= field_value;
      END IF;
	  
      -- GBSVR-35791 Start:   
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(13));
      -- GBSVR-35791 End:   
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_REGULATORY_TREATMENT THEN
	  	IF LENGTH( field_value ) > C_SIZE_REGU_TREATMENT_ERROR THEN
			regulatory_reporting_treatment := SUBSTR(field_value, 1 , C_SIZE_REGU_TREATMENT_ERROR);
		ELSE
            regulatory_reporting_treatment:= field_value;
		END IF;
        validation_code:= 4002;
        validation_msg :=   validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
         regulatory_reporting_treatment:= field_value;
      END IF;
-- end GBSVR-28882

-- GBSVR-31380
      -- GBSVR-35791 Start:   
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(14));
      -- GBSVR-35791 End:   
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_LEGAL_ENTITY THEN
	  	IF LENGTH( field_value ) > C_SIZE_LEGAL_ENTITY_ERROR THEN
			c_legal_entity := SUBSTR(field_value, 1 , C_SIZE_LEGAL_ENTITY_ERROR);
		ELSE
            c_legal_entity:= field_value;
		END IF;
        validation_code:= 1022;
        validation_msg :=   validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
         c_legal_entity:= field_value;
      END IF;
      
      -- GBSVR-35791 Start:   
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(15));
      -- GBSVR-35791 End: 
      IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_LEGAL_ENTITY_CODE THEN
	  	IF LENGTH( field_value ) > C_SIZE_LEGAL_ENTITY_CODE_ERROR THEN
			c_legal_entity_code := SUBSTR(field_value, 1 , C_SIZE_LEGAL_ENTITY_CODE_ERROR);
		ELSE
            c_legal_entity_code:= field_value;
		END IF;
        validation_code:= 1023;
        validation_msg :=   validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
      ELSE
         c_legal_entity_code:= field_value;
      END IF;  
      
-- end GBSVR-31380
	  
     END;
-- end GBSVR-21978
      --- end validation length
      -- validation if the action is correct
      BEGIN
         SELECT bh_action_lookup.id
           INTO action_id
           FROM bh_action_lookup
          WHERE name = UPPER (action);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            validation_code := 1001;
            validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            cont_error := cont_error + 1;
            action_id := 4;
            b_csv_true := FALSE;
      END;

      BEGIN
         IF emergency_flag IS NULL OR ( UPPER (emergency_flag) != 'N' AND UPPER (emergency_flag) != 'Y') THEN
            emergency_flag := '?';
            validation_code := 1002;
            validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            cont_error := cont_error + 1;
            b_emergency_flag := NULL;
            b_csv_true := FALSE;
         ELSE
            IF UPPER (emergency_flag) = 'N' THEN
               b_emergency_flag := FALSE;
            ELSIF UPPER (emergency_flag) = 'Y' THEN
               b_emergency_flag := TRUE;
            END IF;
         END IF;
      END;

      -- Validate book id not null
      BEGIN
         IF book_id IS NULL AND gtb IS NULL AND action_id IN (2, 3) THEN -- all:  MODIFY, DELETE
            book_id := '?';
            validation_code := 1021; --error if not exit book then gtb is madatory
            validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            cont_error := cont_error + 1;
            b_csv_true := FALSE;
         END IF;

         IF book_id IS NULL AND action_id = 1 THEN --action ADD book id shoulb always exist
            book_id := '?';
            validation_code := 1003; -- error the book should exist
            validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            cont_error := cont_error + 1;
            b_csv_true := FALSE;
         END IF;

         IF book_id IS NULL AND gtb IS NOT NULL AND pkg_bh_commons.F_EXIST_BRDS_GTB (gtb) AND action_id IN (2) THEN -- MODIFY
            -- search book_id in brds if exist gtb
            book_id := pkg_bh_commons.F_GET_BRDS_BOOK (gtb);
            csv_table(i)(3) := book_id;
            v_book_id_searched_in_bRDS := TRUE;
         END IF;
      END;

      BEGIN
         -- Validate  Volcker Trading Desk id not null
         IF vtd IS NULL AND action_id IN (1, 2) THEN --VTD only needed for ADD and MODIFY, not DELETE
            vtd := '?';
            validation_code := 1004;
            validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            cont_error := cont_error + 1;
            b_csv_true := FALSE;
         END IF;
      END;

      --- Validate CRU and CRP if a user enters one of them then both must be entered
      BEGIN
         IF cru IS NOT NULL OR crp IS NOT NULL THEN
            IF cru IS NULL OR crp IS NULL THEN
               validation_code := 1005;
               validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
               cont_error := cont_error + 1;
               b_csv_true := FALSE;
            END IF;
         END IF;
      END;


	    --- Validate Business and Sub-business if a user enters one of them then both must be entered
      BEGIN
         IF business IS NOT NULL OR sub_business IS NOT NULL THEN
            IF business IS NULL OR sub_business IS NULL THEN
               validation_code := 1018;
               validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
               cont_error := cont_error + 1;
               b_csv_true := FALSE;
            END IF;
         END IF;
      END;
	   -- GBSVR-28882  NON-VTD
      --- Valid values for �Regulatory Treatment� are �B, �T� OR NULL
      BEGIN
	   IF regulatory_reporting_treatment IS NOT NULL THEN
		IF UPPER(regulatory_reporting_treatment) NOT IN('B','T') THEN
               validation_code := 4003;
               validation_msg := validation_msg || pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
               cont_error := cont_error + 1;
               b_csv_true := FALSE;
        END IF;
       END IF;
      END;		
		--END  GBSVR-28882


      ----------------- END VALIDATION CSV--------------------
      IF validation_result = 'OK' THEN
         IF b_csv_true THEN
            v_intermediary_data.global_trader_book_id := gtb;
            v_intermediary_data.action_id := action_id;
            v_intermediary_data.emergency_flag := emergency_flag;
            v_intermediary_data.source_system_id := source_system;
            v_intermediary_data.book_id := book_id;
            v_intermediary_data.volcker_trading_desk := vtd;
            v_intermediary_data.charge_reporting_unit_code := cru;
            v_intermediary_data.charge_reporting_parent_code := crp;
            -- GBSVR-33754 Start: CFBU decommissioning
            -- GBSVR-33754 End:   CFBU decommissioning
            v_intermediary_data.business := business;
            v_intermediary_data.sub_business := sub_business;
            -- GBSVR-33754 Start: CFBU decommissioning
            -- GBSVR-33754 End:   CFBU decommissioning
            v_intermediary_data.non_vtd_rpl_code := non_vtd_rpl_code;
            v_intermediary_data.regulatory_reporting_treatment := regulatory_reporting_treatment;
      -- GBSVR-31380
            v_intermediary_data.legal_entity := c_legal_entity;
            v_intermediary_data.legal_entity_code := c_legal_entity_code;
			-- end GBSVR-31380

            PKG_BH_COMMONS.P_VALIDATION_LOAD_APPROVE (v_intermediary_data, PROCESS_UPLOAD, aux_validation_msg, aux_cont_error);

            IF aux_cont_error IS NULL THEN
               aux_cont_error := 0;
            END IF;

            cont_error := cont_error + aux_cont_error;
            validation_msg := validation_msg || aux_validation_msg;
         END IF;

         -----------------------------END VALIDATIONS ------------------------------------------------------
         -- Validation OK
         IF cont_error = 0 THEN
            validation_code := 100;
            validation_msg := pkg_bh_commons.F_GET_VALIDATION_MSG (validation_code);
            csv_table(i)(pos_validation_result) := 'OK';
         ELSE
         	--start GBSVR-28969
            cont_error_lines := cont_error_lines + 1;
            --end GBSVR-28969
            --Undo book_id searched in bRDS to avoid changing the original user file
            IF v_book_id_searched_in_bRDS = TRUE
            THEN
               csv_table(i)(3) := NULL;
            END IF;
            csv_table(i)(pos_validation_result) := 'KO';
         END IF;

         -- insert the validation message into to the array
         csv_table(i)(pos_validation_msg) := validation_msg;
      END IF; -- end validatate error template

      -- insert row
      INSERT INTO BH_INTERMEDIARY (ID,
                                   UPLOAD_ID,
                                   ACTION_ID,
                                   BOOK_ID,
                                   VOLCKER_TRADING_DESK,
                                   SOURCE_SYSTEM_ID,
                                   GLOBAL_TRADER_BOOK_ID,
                                   EMERGENCY_FLAG,
                                   CHARGE_REPORTING_UNIT_CODE,
                                   CHARGE_REPORTING_PARENT_CODE,
                                   -- GBSVR-33754 Start: CFBU decommissioning
                                   -- GBSVR-33754 End:   CFBU decommissioning
                                   VALIDATION_MESSAGE,
                                   BUSINESS,
                                   SUB_BUSINESS,
                                   COMMENTS,
                                   NON_VTD_RPL_CODE,
                                   REGULATORY_REPORTING_TREATMENT,
                                   LEGAL_ENTITY,  -- GBSVR-31380
                                   LEGAL_ENTITY_CODE,  -- GBSVR-31380
                                   ACTION_NAME, --start/end GBSVR-21978
                                   CSV_LINE_ID  --start/end GBSVR-28392
                                  )
           VALUES (SEQ_BH_INTERMEDIARY.NEXTVAL,
                   idUpload                          /* UPLOAD_ID */,
                   action_id                         /* ACTION_ID */,
                   book_id                             /* BOOK_ID */,
                   vtd                    /* VOLCKER_TRADING_DESK */,
                   source_system              /* SOURCE_SYSTEM_ID */,
                   gtb                   /* GLOBAL_TRADER_BOOK_ID */,
                   emergency_flag               /* EMERGENCY_FLAG */,
                   cru              /* CHARGE_REPORTING_UNIT_CODE */,
                   crp            /* CHARGE_REPORTING_PARENT_CODE */,
                   -- GBSVR-33754 Start: CFBU decommissioning
                   -- GBSVR-33754 End:   CFBU decommissioning
                   validation_msg           /* VALIDATION_MESSAGE */,
                   business                           /* BUSINESS */,
                   sub_business                   /* SUB_BUSINESS */,
                   comments                           /* COMMENTS */,
                   non_vtd_rpl_code                           /* NON_VTD_RPL_CODE */,
                   regulatory_reporting_treatment                           /* REGULATORY_REPORTING_TREATMENT */,
                   c_legal_entity                   /* LEGAL_ENTITY */,
                   c_legal_entity_code              /* LEGAL_ENTITY_CODE */,
                   action_name                      /*ACTION NAME */, -- start/end GBSVR-21978
                   i   /*CSV_LINE_ID*/  --start/end GBSVR-28392 
                 );
   END LOOP;

   -- 4 Update Upload clob with adding validation message and and status such as from SUBMITTED to VALID / PENDING APPROVAL, or UPLOAD INVALID
   --4.1 build the csv output
   csv_line_out := NULL;
   csv_line_error := NULL;
   
   --cont_error := 0;  --start/end GBSVR-28392 
/* old code pre GBSVR-28392
   FOR i IN 1 .. total_rows
   LOOP
      validation_result := csv_table(i)(pos_validation_result);
      IF csv_table(i)(pos_validation_msg) = C_EMPTY_LINE THEN
        cont_empty := cont_empty + 1;
        CONTINUE;
      END IF;

      FOR j IN 1 .. total_colum
      LOOP
         -- last field
         IF pos_validation_msg = j THEN
            IF validation_result = 'KO' THEN
               csv_line_error := csv_line_error || csv_table(i)(j) || CHR (10);
               cont_error := cont_error + 1;
            ELSE
               csv_line_out := csv_line_out || csv_table(i)(j) || CHR (10);
            END IF;
         ELSE
            IF pos_validation_msg > j THEN
               IF validation_result = 'KO' THEN
                  csv_line_error := csv_line_error || csv_table(i)(j) || ',';
               ELSE
                  csv_line_out := csv_line_out || csv_table(i)(j) || ',';
               END IF;
            END IF;
         END IF;
      END LOOP;
   END LOOP;

   csv_result := csv_line_error || csv_line_out;
*/ --old code pre GBSVR-28392
   
   
-- start GBSVR-28392
 FOR i IN 1 .. total_rows
   LOOP   
    select validation_message into v_validation_message from BH_INTERMEDIARY where UPLOAD_ID = idUpload and CSV_LINE_ID= i;
       v_output_line_csv :=csv_table(i)(1) ||','
				      || csv_table(i)(2) ||','
				      || csv_table(i)(3) ||','
				      || csv_table(i)(4) ||','
				      || csv_table(i)(5) ||','
				      || csv_table(i)(6) ||','
				      || csv_table(i)(7) ||','
				      || csv_table(i)(8) ||','
				      || csv_table(i)(9) ||','
				      || csv_table(i)(10) ||','
				      || csv_table(i)(11) ||','
				      || csv_table(i)(12) ||','	
              || csv_table(i)(13) ||','
              || csv_table(i)(14) ||',' 
              || csv_table(i)(15) ||',' 
              -- GBSVR-33754 Start: CFBU decommissioning
              -- GBSVR-33754 End:   CFBU decommissioning
				      || v_validation_message||'';
      csv_result := csv_result || v_output_line_csv  || chr(13) || chr(10)  ;
   END LOOP;
-- end GBSVR-28392
     --Error: file with no records detected
   -- start 2 GBSVR-26874:  
   IF csv_result is null THEN
   -- end 2 GBSVR-26874:
     --start GBSVR-28969
     cont_error_lines := 1;
     --end GBSVR-28969
     --csv_result cannot be saved as null in ref_data_ui_upload. create dummy record
     csv_result := '';
     FOR pos IN 1 .. total_colum-1 LOOP
       csv_result := csv_result || ',';
     END LOOP;
     csv_result := csv_result || 'ERROR file. The uploaded file has no valid records.';     
   END IF;
   
   --dbms_output.put_line('----------------output CSV init-----------------------');
   -- start 3 GBSVR-26874: should be commented always 
   --dbms_output.put_line(csv_result);
   -- end 3 GBSVR-26874: should be commented always
   --dbms_output.put_line('----------------output CSV end-----------------------');
   --4.2 update table  REF_DATA_UI_UPLOAD
   --dbms_output.put_line(':::::::::::::::::init SUMMARY::::::::::::::::::::::::::');
   --start GBSVR-28969
   --dbms_output.put_line('- TOTAL ERRORS:' || cont_error_lines);
   --end GBSVR-28969
   --dbms_output.put_line('- TOTAL EMPTY:' || cont_empty);
   --dbms_output.put_line('- TOTAL ROWS:' || total_rows);

   --start GBSVR-28969
   IF cont_error_lines > 0 THEN
   --end GBSVR-28969
      -- UPLOADED INVALID
      status := 3;
   ELSE
      status := 2;
   -- VALID - PENDING APPROVAL
   END IF;

   IF status = 2 THEN
      UPDATE REF_DATA_UI_UPLOAD
         SET status_id = status, csv = csv_result
             --start GBSVR-28837
             , comments = null
             --end GBSVR-28837
       WHERE REF_DATA_UI_UPLOAD.id = idUpload;

      COMMIT;
   --Now we have to wait for approval to load in staging
   ELSE
      -- status  UPLOADED INVALID
      UPDATE REF_DATA_UI_UPLOAD
         SET status_id = status,
             csv = csv_result,
             comments = upload_error,
             error_log = c_error_log,
             rejected_by = 'admin_pl',
             rejected_on = SYSDATE
       WHERE REF_DATA_UI_UPLOAD.id = idUpload;

      COMMIT;
   END IF;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      upload_error := 'IdUpload NOT EXIST. ' || SQLERRM;
      ROLLBACK;
      dbms_output.put_line(upload_error);
      RAISE;
   WHEN MORE_FIELDS_THAN_EXPECTED THEN
      status := 3;
      --The uploaded spreadsheet has rows with more columns than allowed
      upload_error := pkg_bh_commons.F_GET_VALIDATION_MSG (5002);
      c_error_log := SQLERRM;
      ROLLBACK;

      -- DBMS_OUTPUT.put_line(upload_error);
      BEGIN
         UPDATE REF_DATA_UI_UPLOAD
            SET status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          WHERE REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
      END;
    -- start 3: GBSVR-21252
      WHEN FIELDS_LESS_THAN_EXPECTED THEN
      status := 3;       
      --The uploaded spreadsheet has rows with less columns than allowed
      upload_error := pkg_bh_commons.F_GET_VALIDATION_MSG (5004);
      c_error_log := SQLERRM;
      ROLLBACK;

      -- DBMS_OUTPUT.put_line(upload_error);
      BEGIN
         UPDATE REF_DATA_UI_UPLOAD
            SET status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          WHERE REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
      END;
      -- end 3: GBSVR-21252
   WHEN OTHERS
   THEN
      status := 4;
      c_error_log := SQLERRM;
      ROLLBACK;
      dbms_output.put_line(c_error_log);

      BEGIN
         -- UPLOAD ERROR, Technical error
         upload_error := pkg_bh_commons.F_GET_VALIDATION_MSG (5001);

         UPDATE REF_DATA_UI_UPLOAD
            SET status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          WHERE REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS
         THEN
            ROLLBACK;
            RAISE;
      END;
END P_SPLIT_CSV_PROCESS;
------------------------------------------------------------------------
--Allow to convert a column value to the right format. 
-------------------------------------------------------------------------
FUNCTION F_FORMAT_COLUMN_CSV(field IN VARCHAR) 
    RETURN  VARCHAR2
AS 
  out_field VARCHAR2(4000);
  C_REF_DATA_COMMA CONSTANT VARCHAR2(20):='[REF_DATA_COMMA]';
  C_REF_DATA_NEW_LINE CONSTANT VARCHAR2(20):='[REF_DATA_LF]';
  C_REF_DATA_QUOTE CONSTANT VARCHAR2(20):='[REF_DATA_QUOTE]';
BEGIN
  out_field:=field;
  IF field IS NOT NULL THEN
	  out_field:=REPLACE(out_field,C_REF_DATA_COMMA , ',');
	  out_field:=REPLACE(out_field,C_REF_DATA_NEW_LINE,' ');
	  out_field:=REPLACE(out_field,C_REF_DATA_QUOTE,'"');
	  out_field:=TRIM(out_field);
  END IF;
  RETURN out_field;
END F_FORMAT_COLUMN_CSV;
---------------------------------------------------------------------------
-- Allow to work with the rows of clob. 
---------------------------------------------------------------------------
FUNCTION F_CONVERT_ROWS(p_list IN CLOB)
  RETURN bh_clob_type PIPELINED
AS
  --l_string LONG := p_list || CHR(10);  --start / end 3: GBSVR-24430
  l_line_index PLS_INTEGER;
  l_index PLS_INTEGER := 1;
  --start 4 GBSVR-24430
  l_clob  CLOB := p_list || CHR(10);
  -- end 4: GBSVR-24430
BEGIN
  --start 5: GBSVR-24430
  LOOP
    l_line_index := dbms_lob.instr(l_clob, CHR(10), l_index); 
    EXIT
  WHEN l_line_index = 0;    
    PIPE ROW ( dbms_lob.substr(l_clob, l_line_index - l_index, l_index ) ); 
    l_index := l_line_index + 1;
  END LOOP;
  --end 5: GBSVR-24430
  RETURN;
END F_CONVERT_ROWS;
-----------------------------------------------------------------------------
-- Allow to work with the columns of each row of csv file.
------------------------------------------------------------------------------
FUNCTION F_CONVERT_ROW(
    p_list IN VARCHAR2,
    token  IN VARCHAR2)
  RETURN bh_varchar_type PIPELINED
AS
  l_string LONG := p_list || token;
  l_line_index PLS_INTEGER;
  l_index PLS_INTEGER := 1;
BEGIN
  LOOP
    l_line_index := INSTR(l_string, token, l_index);
    EXIT
  WHEN l_line_index = 0;
    PIPE ROW ( SUBSTR(l_string, l_index, l_line_index - l_index) );
    l_index := l_line_index                           + 1;
  END LOOP;
  RETURN;
END F_CONVERT_ROW;

PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
  --start GBSVR-28837
  v_count_active_approvals integer;
  v_status_processing_approval CONSTANT INT := 12;
BEGIN    
  IF P_PROCESS_ETL_STATUS(IDUPLOAD, 2) = FALSE THEN
    RETURN;
  END IF;
   
  --Check that there is not another approval being processed
  SELECT NVL(COUNT(*), 0) INTO v_count_active_approvals
    FROM REF_DATA_UI_UPLOAD
   WHERE status_id = v_status_processing_approval;
  
  IF v_count_active_approvals > 0 THEN
    UPDATE REF_DATA_UI_UPLOAD
       SET comments = 'Another file is currently being approved, please wait some minutes before trying to approve again this file.'
     WHERE id = idupload;     
     RETURN;
  END IF;
   
  --Set status to PROCESSING_APPROVAL
  --PKG_BH_COMMONS.P_UPDATE_REF_DATA_STATUS_PAT(IDUPLOAD, v_status_processing_approval, 'Processing approval');
  UPDATE REF_DATA_UI_UPLOAD
     SET STATUS_ID = v_status_processing_approval,
         COMMENTS = 'Processing approval'
  WHERE ID = IDUPLOAD;
  COMMIT;
  --end GBSVR-28837
  
  SELECT uploaded_by INTO v_uploaded_by
    FROM REF_DATA_UI_UPLOAD
   WHERE id = idupload;
   
  IF v_uploaded_by = IDUSER THEN
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS.P_BH_INITIAL_LOADING', 'ERROR', 'LOGGING', 'Users cannot change status of their own uploaded files', '', 'bRDS');
  ELSIF v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS.P_BH_INITIAL_LOADING', 'ERROR', 'LOGGING', 'The status file is not ready for approval/reject', '', 'bRDS');
  --start GBSVR-28837
  ELSE
    IF F_BH_LOAD_FILE_IN_STAGING(idUpload) = TRUE THEN
        UPDATE REF_DATA_UI_UPLOAD
            --start GBSVR-30255
           SET status_id = 5, comments = null --Set status to USER APPROVED and comments empty (removing 'Processing approval' message)
           --end GBSVR-30255
         WHERE id = IDUPLOAD;
    ELSE
        UPDATE REF_DATA_UI_UPLOAD
           SET status_id = 7, --Set status to APPROVED INVALID
               rejected_by='admin_pl', rejected_on=SYSDATE,
               comments=PKG_BH_COMMONS.F_GET_VALIDATION_MSG(5001)
         WHERE id = IDUPLOAD;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'P_ACCEPT_UPLOAD','DEBUG', 'LOGGING', 'Set status to APPROVED INVALID', '', 'bRDS');
    END IF;  

      --start GBSVR-30255
    UPDATE REF_DATA_UI_UPLOAD
      SET approved_by = IDUSER, approved_on = systimestamp --we save who tried to approve the file and when
    WHERE id = IDUPLOAD;
    --end GBSVR-30255
  END IF;
  --end GBSVR-28837
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  --start GBSVR-28837
  UPDATE REF_DATA_UI_UPLOAD
     SET status_id = 7, --Set status to APPROVED INVALID
         rejected_by='admin_pl', rejected_on=SYSDATE,
         comments=PKG_BH_COMMONS.F_GET_VALIDATION_MSG(5001)
   WHERE id = IDUPLOAD;
  --end GBSVR-28837
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'P_ACCEPT_UPLOAD','DEBUG', 'LOGGING', 'ROLLBACK: '||SQLERRM, '', 'bRDS');
  RAISE;
END P_ACCEPT_UPLOAD;

PROCEDURE P_REJECT_UPLOAD (P_IDUPLOAD IN NUMBER, P_IDUSER IN VARCHAR, P_COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
BEGIN  

  SELECT uploaded_by, status_id INTO v_uploaded_by, v_status_id
    FROM REF_DATA_UI_UPLOAD
   WHERE id = P_IDUPLOAD;
   
  IF v_uploaded_by = P_IDUSER THEN
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS.P_BH_INITIAL_LOADING', 'ERROR', 'LOGGING', 'Users cannot change status of their own uploaded files.', '', 'bRDS');
  ELSIF v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS.P_BH_INITIAL_LOADING', 'ERROR', 'LOGGING', 'The status file is not ready for approval/reject', '', 'bRDS');
  END IF;
  
  UPDATE REF_DATA_UI_UPLOAD
     SET rejected_by = P_IDUSER, rejected_on = systimestamp,
         status_id = 6, --User rejected
         comments = P_COMMENTS
   WHERE id = P_IDUPLOAD;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  RAISE;
END P_REJECT_UPLOAD;

PROCEDURE P_BH_RESOLVE_CONFLICT(p_conflict_id IN NUMBER, p_selected_choice IN VARCHAR2, p_user_name IN VARCHAR2, p_comments IN VARCHAR2)
IS
  v_conflict_row BH_CONFLICTS%ROWTYPE;
  v_manual_update_rows NUMBER;
  v_brds_update_rows NUMBER;
  v_conflict_update_rows NUMBER;
  v_full_match_update_rows NUMBER; 
BEGIN 
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','INFO', 'LOGGING', 'Resolving conflict', '', 'bRDS');   
  
  SELECT * INTO v_conflict_row
    FROM bh_conflicts
   WHERE id = p_conflict_id;
     
  --If user approves, conflict manual resolution will be rejected
  IF UPPER(p_selected_choice) = 'BRDS' THEN
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','INFO', 'LOGGING', 'Approval selected', '', 'bRDS');   
    --Deactive manual in BH_STAGING  
    UPDATE BH_STAGING
       SET ACTIVE_FLAG = 'N'
     WHERE bh_intermediary_id = v_conflict_row.bh_intermediary_id
       AND PKG_BH_COMMONS.F_IS_MANUAL(DATA_SOURCE) = 0;
    v_manual_update_rows := SQL%ROWCOUNT;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','DEBUG', 'LOGGING', 'ROWCOUNT Updated BH_STAGING manual', v_manual_update_rows, 'bRDS');
     
    --Activate BRDS in BH_STAGING
    UPDATE BH_STAGING
       SET ACTIVE_FLAG = 'Y'
     WHERE global_trader_book_id = v_conflict_row.global_trader_book_id
       and asofdate = v_conflict_row.asofdate
       AND PKG_BH_COMMONS.F_IS_BRDS(DATA_SOURCE) = 0;
    v_brds_update_rows := SQL%ROWCOUNT;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','DEBUG', 'LOGGING', 'ROWCOUNT Updated BH_STAGING brds', v_brds_update_rows, 'bRDS');
    
    --Resolve conflict    
    UPDATE BH_CONFLICTS
       SET STATUS = 'APPROVED',
          -- start 1: GBSVR-25021
       	   comments = p_comments,
       	  -- end 1: GBSVR-25021
           resolved_by = p_user_name,
           resolved_on = current_timestamp
     WHERE id = p_conflict_id;
    v_conflict_update_rows := SQL%ROWCOUNT;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','DEBUG', 'LOGGING', 'ROWCOUNT Updated BH_CONFLICTS', v_conflict_update_rows, 'bRDS');
  
    -- This is the update for fullmatch when it is resolving conflict in favour of bRDS
    UPDATE BH_STAGING
       SET ACTIVE_FLAG = 'N'
     WHERE PKG_BH_COMMONS.F_IS_MANUAL(DATA_SOURCE) = 0
       	AND (GLOBAL_TRADER_BOOK_ID = v_conflict_row.GLOBAL_TRADER_BOOK_ID OR BOOK_ID = v_conflict_row.BOOK_ID)
       	  AND VOLCKER_TRADING_DESK = v_conflict_row.VOLCKER_TRADING_DESK
       		 AND CHARGE_REPORTING_UNIT_CODE = v_conflict_row.CHARGE_REPORTING_UNIT_CODE
       	 	  AND CHARGE_REPORTING_PARENT_CODE = v_conflict_row.CHARGE_REPORTING_PARENT_CODE;
            -- GBSVR-33754 Start: CFBU decommissioning
            -- GBSVR-33754 End:   CFBU decommissioning

    v_full_match_update_rows := SQL%ROWCOUNT;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','DEBUG', 'LOGGING', 'ROWCOUNT Updated BH_STAGING Full Match', v_full_match_update_rows, 'bRDS');
    
	IF v_manual_update_rows < 1 OR v_brds_update_rows < 1 OR v_conflict_update_rows < 1 THEN
	  --Error: all the tables need to be updated
	  ROLLBACK;
	  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','ERROR', 'LOGGING', 'Either manual, brds or conflict data was not updated in the conflict resolution.', '', 'bRDS');
	END IF;  
  --If user rejects, conflict manual resolution will be activated and brds will be ignored
  ELSE
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','INFO', 'LOGGING', 'Reject selected', '', 'bRDS');   
	-- Code eliminated it is not necessary because the registers have active flag to yes and no.
    
    --Resolve conflict    
    UPDATE BH_CONFLICTS
       SET STATUS = 'REJECTED',
       	   comments = p_comments,
           resolved_by = p_user_name,
           resolved_on = current_timestamp
     WHERE id = p_conflict_id;
    v_conflict_update_rows := SQL%ROWCOUNT;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','DEBUG', 'LOGGING', 'ROWCOUNT Updated BH_CONFLICTS', v_conflict_update_rows, 'bRDS');
    
  	IF v_conflict_update_rows < 1 THEN
	  ROLLBACK;
	  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','ERROR', 'LOGGING', 'Conflict data was not updated in the conflict resolution.', '', 'bRDS');
	END IF;
  END IF;
  
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.P_BH_RESOLVE_CONFLICT','INFO', 'LOGGING', 'Conflict resolution successfully finished', '', 'bRDS');
END P_BH_RESOLVE_CONFLICT;

FUNCTION P_PROCESS_ETL_STATUS(p_upload_id IN INTEGER, p_operation_type IN INTEGER) RETURN BOOLEAN
IS
  v_etl_status_record BRDS_VW_STATUS%ROWTYPE;
  v_result BOOLEAN := TRUE;
  v_status_id_result INTEGER;
BEGIN

  --For uploading(p_operation_type=1), set UPLOAD_ERROR(status_id=3). For approving(p_operation_type=not 1), return to VALID_PEDING_APPROVAL(status_id=2)
  IF p_operation_type = 1 THEN
    v_status_id_result := 3;
  ELSE
    v_status_id_result := 2;
  END IF;

  BEGIN
    SELECT * INTO v_etl_status_record
      FROM BRDS_VW_STATUS;
  EXCEPTION
    WHEN NO_DATA_FOUND OR TOO_MANY_ROWS THEN -- only one record in the table is only possible
      P_UPDATE_UPLOAD_ID_WITH_ETL(p_upload_id, v_status_id_result, 5003);
      v_result := FALSE;
      pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.PROCESS_ETL_STATUS','ERROR', 'LOGGING', SQLERRM, '', 'bRDS');
      RETURN v_result;
  END;
  
  IF v_etl_status_record.endTime IS NOT NULL and NVL(v_etl_status_record.result, ' ') = 'OK' THEN
     v_result := TRUE;
     pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.PROCESS_ETL_STATUS','DEBUG', 'LOGGING', 'OK', '', 'bRDS');
  ELSIF v_etl_status_record.endTime IS NOT NULL and NVL(v_etl_status_record.result, ' ') <> 'OK' THEN
     P_UPDATE_UPLOAD_ID_WITH_ETL(p_upload_id, v_status_id_result, 5003);
     v_result := FALSE;
     pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.PROCESS_ETL_STATUS','ERROR', 'LOGGING', 'not OK', '', 'bRDS');
  ELSIF v_etl_status_record.endTime IS NULL THEN
     v_result := FALSE;
     --Stop trying to run the ETL after 10 hours
     IF v_etl_status_record.startTime + interval '10' hour < systimestamp THEN
       P_UPDATE_UPLOAD_ID_WITH_ETL(p_upload_id, v_status_id_result, 5003);
       UPDATE BRDS_VW_STATUS 
          SET endTime = systimestamp, 
              result = 'ERROR',
              error_message = 'ETL cancelled because the running time was greater than 10 hours';
       pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.PROCESS_ETL_STATUS','ERROR', 'LOGGING', 'ETL running time greater than 10 hours', '', 'bRDS');
     --Set message asking to try later
     ELSE
       P_UPDATE_UPLOAD_ID_WITH_ETL(p_upload_id, v_status_id_result, 2046);
       pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS.PROCESS_ETL_STATUS','ERROR', 'LOGGING', 'ETL running time less than 10 hours', '', 'bRDS');
     END IF;
  END IF;
  
  RETURN v_result;
END P_PROCESS_ETL_STATUS;

PROCEDURE P_UPDATE_UPLOAD_ID_WITH_ETL(p_upload_id IN INTEGER, p_status_id IN INTEGER, p_error_return_code IN INTEGER)
IS
BEGIN
  UPDATE REF_DATA_UI_UPLOAD
     SET status_id = p_status_id,
         rejected_by='admin_pl', rejected_on=SYSDATE,
         comments=PKG_BH_COMMONS.F_GET_VALIDATION_MSG(p_error_return_code)
   WHERE id = p_upload_id;
END P_UPDATE_UPLOAD_ID_WITH_ETL;
  

END PKG_BH_PROCESS;
