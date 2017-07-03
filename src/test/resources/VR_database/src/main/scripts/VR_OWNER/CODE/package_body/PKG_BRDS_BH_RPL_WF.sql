--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BRDS_BH_RPL_WF runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BRDS_BH_RPL_WF" AS



-- ********************************************************************** 
-- Procedure: p_brds_etl_val_book_id_length
-- ********************************************************************** 

procedure p_brds_etl_val_book_id_length
as
begin 

dbms_output.put_line('Running p_brds_etl_val_book_id_length');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_book_id_length', 'Step 1 Start', 'bRDS');

-- book_id > 50 chars: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        1                                                   workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'bookName (book_id) greater than 50 characters'     comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   length(vb1.bookName) > 50;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_book_id_length', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

dbms_output.put_line('Finished p_brds_etl_val_book_id_length');


end p_brds_etl_val_book_id_length;



-- ********************************************************************** 
-- Procedure: p_brds_etl_val_dup_gtbid
-- ********************************************************************** 

procedure p_brds_etl_val_dup_gtbid
as
begin 

dbms_output.put_line('Running p_brds_etl_val_dup_gtbid');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_gtbid', 'Step 1 Start', 'bRDS');

-- Duplicate global_trader_book_id: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        2                                                   workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'Duplicate globalTraderBookId in brds_vw_book'      comments 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1, 
        (
        select  vb2.globalTraderBookId  globalTraderBookId
        from    brds_vw_book  vb2
        --start GBSVR-27552
        where   NVL(vb2.volckerTradingDesk, 'null') != 'null'
        --end GBSVR-27552
        group by vb2.globalTraderBookId
        having count(*) > 1 
        ) dupId
where   vb1.globalTraderBookId = dupId.globalTraderBookId
--start GBSVR-27552
and     NVL(vb1.volckerTradingDesk, 'null') != 'null';
--end GBSVR-27552

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_gtbid', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

dbms_output.put_line('Finished p_brds_etl_val_dup_gtbid');


end p_brds_etl_val_dup_gtbid;




-- start 2: GBSVR-24023
-- ********************************************************************** 
-- Procedure: p_brds_etl_val_dups
-- ********************************************************************** 

procedure p_brds_etl_val_dups
-- end 2: GBSVR-24023
as
begin 
-- start 3: GBSVR-24023
dbms_output.put_line('Running p_brds_etl_val_dups');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 1 Start', 'bRDS');


-- Duplicates:
-- end 3: GBSVR-24023
-- Duplicate book_id with different volcker_trading_desk/charge_reporting_unit/charge_reporting_parent/covered fund business Unit: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')      asofdate, 
        3                                                       workflow_type_id, 
        vb1.bookName                                            book_id, 
        vb1.globalTraderBookId                                  global_trader_book_id, 
        vb1.volckerTradingDesk                                  volcker_trading_desk, 
        vb1.chargeReportingUnitCode                             charge_reporting_unit, 
        vb1.chargeReportingParentCode                           charge_reporting_parent, 
        'bRDS'                                                  data_source, 
        vb1.tradeCaptureSystemName                              source_system_id,
       -- start 1: GBSVR-27301
        -- start 4: GBSVR-24023
        'Duplicate book_id, Different volckerTradingDesk'       comments
            --/chargeReportingUnit/chargeReportingParent'  comments,
        -- end 4: GBSVR-24023        
        -- end 1: GBSVR-27301
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   vb1.bookName in ( 
  select  vb2.bookName
  from    brds_vw_book  vb2
  --start GBSVR-27552
  where   NVL(vb2.volckerTradingDesk, 'null') != 'null'
  --end GBSVR-27552
  group by vb2.bookName
  having count( distinct nvl(vb2.volckerTradingDesk, ' ') ) > 1) 
        -- start 2: GBSVR-27301 3.1 Modify where clauses 
       -- end 2: GBSVR-27301
--start GBSVR-27552
and    NVL(vb1.volckerTradingDesk, 'null') != 'null'
--end GBSVR-27552
order by 
        vb1.bookName;

-- start 3: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 3: GBSVR-24023

commit;




-- ************************************************************************ 
-- Remove invalid dups where one side is NULL (VTD/CRU/CRP): VTD: 

-- start 4: GBSVR-24023
-- New Exception: dups with NULL
-- Look at  dups excpetions (type 3)
-- end 4: GBSVR-24023
-- Select the book_ids where a NULL VTD exits
-- Copy these over to a new type: 26
-- Remove these from type 3
-- Update the type from 3 to 26
-- Filter out 26

--start 3 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: 'NULL volckerTradingDesk Duplicate'
--end 3 GBSVR-27301

-- start 5: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 5: GBSVR-24023

commit;

-- We ONLY want to flag and exclude the entries with the NULL VTDs; So remove the Non NULL entry from bh_workflow: 
delete  
from    bh_workflow w
where   w.workflow_type_id = 26
and     w.volcker_trading_desk is NOT NULL 
and     w.volcker_trading_desk != 'null';

-- start 6: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 6: GBSVR-24023
commit;



-- ************************************************************************ 
-- Remove invalid dups where one side is NULL (VTD/CRU/CRP): CRU: 

--start 4 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: 'NULL volckerTradingDesk Duplicate'
--end 4 GBSVR-27301
-- start 7: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 5: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 7: GBSVR-24023

commit;


-- We ONLY want to flag and exclude the entries with the NULL CRUs; So remove the Non NULL entry from bh_workflow: 
delete  
from    bh_workflow w
where   w.workflow_type_id = 27
and     w.charge_reporting_unit is NOT NULL 
and     w.charge_reporting_unit != 'null';

-- start 8: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 6: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 8: GBSVR-24023

commit;



-- ************************************************************************ 
-- Remove invalid dups where one side is NULL (VTD/CRU/CRP): CRP: 


--start 5 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: NULL chargeReportingParent Duplicate
--end 5 GBSVR-27301

-- start 9: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 7: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 9: GBSVR-24023

commit;



-- We ONLY want to flag and exclude the entries with the NULL CRPs; So remove the Non NULL entry from bh_workflow: 
delete  
from    bh_workflow w
where   w.workflow_type_id = 28
and     w.charge_reporting_parent is NOT NULL 
and     w.charge_reporting_parent != 'null';
-- start 10: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 8: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 10: GBSVR-24023
commit;



-- ************************************************************************ 
-- Remove invalid dups where one side is NULL (VTD/CRU/CRP): CFBU: 

--start 6 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: 'NULL coveredFundBusinessUnitRplCode Duplicate'
--end 6 GBSVR-27301

-- start 11: GBSVR-24023
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 9: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 11: GBSVR-24023
commit;


-- GBSVR-33754 Start: CFBU decommissioning
-- We ONLY want to flag and exclude the entries with the NULL CRPs; So remove the Non NULL entry from bh_workflow: 
--delete  
--from    bh_workflow w
--where   w.workflow_type_id = 29
--and     w.covered_fund_business_unit is NOT NULL 
--and     w.covered_fund_business_unit != 'null';
-- start 12: GBSVR-24023
--pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dups', 'Step 10 End: '||to_char(SQL%ROWCOUNT), 'bRDS');
-- end 12: GBSVR-24023
--commit;
-- GBSVR-33754 End:   CFBU decommissioning

-- start 13: GBSVR-24023
dbms_output.put_line('Finished p_brds_etl_val_dups');
-- end 13: GBSVR-24023


end p_brds_etl_val_dups;



-- ********************************************************************** 
-- Procedure: p_brds_etl_val_dup_vtd
-- ********************************************************************** 

procedure p_brds_etl_val_dup_book_id
as
begin 

dbms_output.put_line('Running p_brds_etl_val_dup_book_id');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_book_id', 'Step 1 Start', 'bRDS');


-- True Duplicates: 
-- Duplicate book_ids: differences in globaltraderBookId

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')      asofdate, 
        4                                                       workflow_type_id, 
        vb1.bookName                                            book_id, 
        vb1.globalTraderBookId                                  global_trader_book_id, 
        vb1.volckerTradingDesk                                  volcker_trading_desk, 
        vb1.chargeReportingUnitCode                             charge_reporting_unit, 
        vb1.chargeReportingParentCode                           charge_reporting_parent, 
        'bRDS'                                                  data_source, 
        vb1.tradeCaptureSystemName                              source_system_id, 
        'Duplicate book_id, Different globaltraderBookId'       comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   vb1.bookName in ( 
  select  vb2.bookName
  from    brds_vw_book  vb2
  --start GBSVR-27552
  where   NVL(vb2.volckerTradingDesk, 'null') != 'null'
  --end GBSVR-27552
  group by vb2.bookName
  having count( distinct vb2.globaltraderBookId ) > 1 )
--start GBSVR-27552
and     NVL(vb1.volckerTradingDesk, 'null') != 'null'
--end GBSVR-27552
order by 
        vb1.bookName;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_book_id', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

dbms_output.put_line('Finished p_brds_etl_val_dup_book_id');


end p_brds_etl_val_dup_book_id;





-- ********************************************************************** 
-- Procedure: p_brds_etl_val_no_hierarchy
-- ********************************************************************** 

procedure p_brds_etl_val_no_hierarchy
as
begin 

dbms_output.put_line('Running p_brds_etl_val_no_hierarchy');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_no_hierarchy', 'Step 1 Start', 'bRDS');


-- bRDS book records with no matching entry in hierarchy view: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        5                                                   workflow_type_id, 
        b.bookName                                          book_id, 
        b.globalTraderBookId                                global_trader_book_id, 
        b.volckerTradingDesk                                volcker_trading_desk, 
        b.chargeReportingUnitCode                           charge_reporting_unit, 
        b.chargeReportingParentCode                         charge_reporting_parent, 
        'bRDS'                                              data_source, 
        b.tradeCaptureSystemName                            source_system_id, 
        'Not present in Hierarchy view'                     comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  b
--start GBSVR-28737
where  (b.bookFunctionCode != 'S' and NOT exists ( select * from brds_vw_hierarchy h where h.nodeId = b.globalTraderBookId and h.nodeType = 'BOOK') )
UNION ALL
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        5                                                   workflow_type_id, 
        b.bookName                                          book_id, 
        b.globalTraderBookId                                global_trader_book_id, 
        b.volckerTradingDesk                                volcker_trading_desk, 
        b.chargeReportingUnitCode                           charge_reporting_unit, 
        b.chargeReportingParentCode                         charge_reporting_parent, 
        'bRDS'                                              data_source, 
        b.tradeCaptureSystemName                            source_system_id, 
        'Not present in Hierarchy view'                     comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  b
where  (b.bookFunctionCode = 'S' and b.portfolioid = 'null' );
--end GBSVR-28737

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_no_hierarchy', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl_val_no_hierarchy');


end p_brds_etl_val_no_hierarchy;




-- ********************************************************************** 
-- Procedure: p_brds_etl_val_dup_node_rpl
-- ********************************************************************** 

procedure p_brds_etl_val_dup_node_rpl
as
begin 

dbms_output.put_line('Running p_brds_etl_val_dup_node_rpl');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_node_rpl', 'Step 1 Start', 'bRDS');


-- Duplicated nodeId (globalTraderBookId) and rplCode: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        6                                                   workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        -- start 1:  GBSVR-26935
        'Duplicated nodeId (globalTraderBookId) and different rplCode' comments 
        -- end 1: GBSVR-26935
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1, 
        (
        select  vh.nodeId nodeId
        from    brds_vw_hierarchy vh
        group by vh.nodeId
        having count(distinct nvl(vh.rplCode, ' ')) > 1 
        ) dupNodes
where   vb1.globalTraderBookId = dupNodes.nodeId;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_node_rpl', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl_val_dup_node_rpl');


end p_brds_etl_val_dup_node_rpl;



-- ********************************************************************** 
-- Procedure: p_brds_etl_val_null_vtd
-- ********************************************************************** 

procedure p_brds_etl_val_null_vtd
as
begin 

dbms_output.put_line('Running p_brds_etl_val_null_vtd');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_null_vtd', 'Step 1 Start', 'bRDS');

-- Null volckerTradingDesk: 
-- start GBSVR-23354
INSERT INTO bh_workflow 
(BOOK_ID,  ASOFDATE, WORKFLOW_TYPE_ID, GLOBAL_TRADER_BOOK_ID, VOLCKER_TRADING_DESK, CHARGE_REPORTING_UNIT, 
        CHARGE_REPORTING_PARENT, DATA_SOURCE, SOURCE_SYSTEM_ID, COMMENTS
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        )
select  DISTINCT(vb1.bookName)                              book_id,
        to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        7                                                   workflow_type_id,          
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'Null volckerTradingDesk'                           comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   ( vb1.volckerTradingDesk = 'null' or vb1.volckerTradingDesk is NULL );
-- end GBSVR-23354
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_null_vtd', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl_val_null_vtd');


end p_brds_etl_val_null_vtd;



--start 7 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: 'NULL chargeReportingUnitCode'

-- ********************************************************************** 
-- Procedure: p_brds_etl_val_null_cru
-- ********************************************************************** 
--end 7 GBSVR-27301


--start 8 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: 'NULL chargeReportingParentCode'
-- ********************************************************************** 
-- Procedure: p_brds_etl_val_null_crp
-- ********************************************************************** 
--end 8 GBSVR-27301




--start 9 GBSVR-27301:Remove ETL validations for CRU/CRP/CFBU: 'NULL coveredFundBusinessUnitRplCode'
-- ********************************************************************** 
-- Procedure: p_brds_etl_val_null_cfbu
-- ********************************************************************** 
--end 9 GBSVR-27301


--start GBSVR-28997
-- ********************************************************************** 
-- Procedure: p_brds_etl_val_vtd
-- **********************************************************************
procedure p_brds_etl_init_hierarchy
as
begin

dbms_output.put_line('Running p_brds_etl_init_hierarchy');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_init_hierarchy', 'Step 1 Start', 'bRDS');

-- ************************************************************************ 
-- Insert hierarchy trees: 

insert into bh_ubr_desk_hierarchy 
select  h.nodeType, 
        connect_by_root h.nodeId book, 
        h.nodeId, 
        h.nodeName, 
        h.rplCode, 
        level   ubr_desk_level, 
        0 num_desks, 
        0 num_ubrs, 
        h.volckerTradingDesk, 
        h.chargeReportingUnit, 
        h.chargeReportingParent,
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        'N'
from    brds_vw_hierarchy h
connect by prior parentNodeId = nodeId; 
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_init_hierarchy', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Remove hierarchies which do not relate to a valid book: 

delete
from    bh_ubr_desk_hierarchy h
where   NOT exists ( select * from brds_vw_book b where b.globalTraderBookId = h.global_trader_book_id );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_init_hierarchy', 'Step 3 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

end p_brds_etl_init_hierarchy;

--end GBSVR-28997

-- ********************************************************************** 
-- Procedure: p_brds_etl_val_dup_node_rpl_nb
-- ********************************************************************** 


procedure p_brds_etl_val_dup_node_rpl_nb
as
begin 


dbms_output.put_line('Running p_brds_etl_val_dup_node_rpl_nb');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_node_rpl_nb', 'Step 1 Start', 'bRDS');


-- ************************************************************************ 
-- Flag any book that has a VTD that occurs more than once in it's hierarchy tree: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        33                                                  workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
         -- start 2: GBSVR-26935
        'Duplicated Non-Book nodeId (globalTraderBookId) and different rplCode' comments 
        -- end 2: GBSVR-26935
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   vb1.globalTraderBookId in 
  (
  select  distinct node_id                  -- Get the list of other books in the same hierarchy as the underlying book
  from    bh_ubr_desk_hierarchy h
  where   global_trader_book_id in 
    (
    select  distinct global_trader_book_id  -- Get the underlying book for each book-hierarchy
    from    bh_ubr_desk_hierarchy h
    where   node_id in 
      (
      select  vh.nodeId nodeId          -- Duplicate nodeIds in the hierarchy that have different books
      from    brds_vw_hierarchy vh
      where   nodeType != 'BOOK' 
      group by vh.nodeId
      having count(distinct nvl(vh.rplCode, ' ')) > 1 
      )
    )
  );
  
  
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_dup_node_rpl_nb', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl_val_dup_node_rpl_nb');


end p_brds_etl_val_dup_node_rpl_nb;

-- ********************************************************************** 
-- Procedure: p_brds_etl_val_vtd
-- ********************************************************************** 


procedure p_brds_etl_val_vtd
as
begin 


dbms_output.put_line('Running p_brds_etl_val_vtd');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_vtd', 'Step 1 Start', 'bRDS');


-- ************************************************************************ 
-- Flag any book that has a VTD that occurs more than once in it's hierarchy tree: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        8                                                   workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'Invalid volckerTradingDesk: VTD occurs more than once in hierarchy' comments 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   vb1.globalTraderBookId in ( 
  select  h.global_trader_book_id
  from    bh_ubr_desk_hierarchy h, 
          brds_vw_book          b
  where   h.volcker_trading_desk = 'Yes'
  and     h.global_trader_book_id = b.globalTraderBookId
  and     h.rpl_code = b.volckerTradingDesk
  group by h.global_trader_book_id, 
          h.rpl_code
  having  count(*) > 1 );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_vtd', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Flag any book where the VTD does not exist on the VTD view: 

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        9                                                   workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'Invalid volckerTradingDesk: Not present in VTD view' comments 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   NOT exists ( select * from brds_vw_vtd  v where v.volckerTradingDesk = vb1.volckerTradingDesk );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_vtd', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;



-- ************************************************************************ 
-- Other nodes on the hierarchy of the same book must NOT be a VTD: 


insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        10                                                  workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'Invalid volckerTradingDesk: Multiple VTDs in hierarchy' comments 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   vb1.globalTraderBookId in ( 
  select  h.global_trader_book_id
  from    bh_ubr_desk_hierarchy h, 
          brds_vw_vtd           v
  where   h.rpl_code != vb1.volckerTradingDesk    -- Is different from the VTD in brds_vw_book � So we have multiple VTDs in the book�s hierarchy
  and     h.rpl_code = v.volckerTradingDesk );    -- Valid VTD as it exists on the VTD view
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_vtd', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Flag entries where VTD is '00_NO_MATCH', but do NOT filter these out for now: 


insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY')  asofdate, 
        25                                                  workflow_type_id, 
        vb1.bookName                                        book_id, 
        vb1.globalTraderBookId                              global_trader_book_id, 
        vb1.volckerTradingDesk                              volcker_trading_desk, 
        vb1.chargeReportingUnitCode                         charge_reporting_unit, 
        vb1.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                              data_source, 
        vb1.tradeCaptureSystemName                          source_system_id, 
        'volckerTradingDesk exception: 00_NO_MATCH'         comments 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  vb1
where   vb1.volckerTradingDesk = '00_NO_MATCH';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_vtd', 'Step 5 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl_val_vtd');


end p_brds_etl_val_vtd;





-- ********************************************************************** 
-- Procedure: p_brds_etl_val_cru
-- ********************************************************************** 


procedure p_brds_etl_val_cru
as
begin 


dbms_output.put_line('Running p_brds_etl_val_cru');

-- start 10 GBSVR-27301 Remove ETL validations for CRU/CRP/CFBU:

-- ************************************************************************ 
-- Flag any book that has a CRU that occurs more than once in it's hierarchy tree:

-- ************************************************************************ 
-- Flag any book where the CRU does not exist on the CRU view: 

-- ************************************************************************ 
-- Other nodes on the hierarchy of the same book must NOT be a CRU: 
-- end 10 GBSVR-27301

dbms_output.put_line('Finished p_brds_etl_val_cru');


end p_brds_etl_val_cru;

-- ********************************************************************** 
-- Procedure: p_brds_etl_val_crp
-- ********************************************************************** 


procedure p_brds_etl_val_crp
as
begin 


dbms_output.put_line('Running p_brds_etl_val_crp');

-- start 11 GBSVR-27301  not necesary
-- ************************************************************************ 
-- Flag any book that has a CRP that occurs more than once in it's hierarchy tree: 

-- ************************************************************************ 
-- Flag any book where the CRP does not exist on the CRP view: 

-- ************************************************************************ 
-- Other nodes on the hierarchy of the same book must NOT be a CRP: 

-- end 11 GBSVR-27301


dbms_output.put_line('Finished p_brds_etl_val_crp');


end p_brds_etl_val_crp;

-- ********************************************************************** 
-- Procedure: p_brds_etl_val_cfbu
-- ********************************************************************** 


-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning



-- ********************************************************************** 
-- Procedure: p_brds_etl_workflow_items
-- ********************************************************************** 

procedure p_brds_etl_workflow_items
as
begin 

dbms_output.put_line('Running p_brds_etl_workflow_items');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_workflow_items', 'Step 1 Start', 'bRDS');



-- ************************************************************************ 
-- Workflow items: 

-- 1. Manual Overrides entered from the UI: 

insert into bh_workflow
select  b1.asofdate, 
        18, 
        b1.book_id, 
        b1.global_trader_book_id, 
        b1.volcker_trading_desk, 
        b1.charge_reporting_unit_code, 
        b1.charge_reporting_parent_code,
        b1.data_source, 
        b1.source_system, 
        'Manual Override' comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    bh_staging  b1
where   upper(nvl(b1.data_source, 'MANUAL')) = 'MANUAL';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_workflow_items', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;



-- 2. bRDS overriding previous manual override records (full match): 

insert into bh_workflow
select  b1.asofdate, 
        19, 
        b1.book_id, 
        b1.global_trader_book_id, 
        b1.volcker_trading_desk, 
        b1.charge_reporting_unit_code, 
        b1.charge_reporting_parent_code,
        b1.data_source, 
        b1.source_system, 
        'bRDS Overrides Manual entry (full match)' comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    bh_staging  b1, 
        bh_staging  b2
where   upper(b1.data_source) = 'BRDS'
and     upper(nvl(b2.data_source, 'MANUAL')) = 'MANUAL'
and     b2.source_system is NULL
and     b1.last_modified_date >= b2.last_modified_date
and     b1.book_id = b2.book_id
and     nvl(b1.volcker_trading_desk, ' ') = nvl(b2.volcker_trading_desk, ' ')
 -- start13: GBSVR-27301 Modify ETL validations    
-- end 13: GBSVR-27301
UNION
select  b1.asofdate, 
        19, 
        b1.book_id, 
        b1.global_trader_book_id, 
        b1.volcker_trading_desk, 
         b1.charge_reporting_unit_code, 
        b1.charge_reporting_parent_code, 
        --start GBSVR-28723
        b1.data_source, 
        b1.source_system, 
        'bRDS Overrides Manual entry (full match)' comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        --end GBSVR-28723 
from    bh_staging  b1, 
        bh_staging  b2 
where   upper(b1.data_source) = 'BRDS' 
and     upper(nvl(b2.data_source, 'MANUAL')) = 'MANUAL'
and     b2.source_system is NULL
and     b1.last_modified_date >= b2.last_modified_date 
and     b1.global_trader_book_id = b2.global_trader_book_id 
and     nvl(b1.volcker_trading_desk, ' ') = nvl(b2.volcker_trading_desk, ' ');
 -- start 14: GBSVR-27301 Modify ETL validations
 -- end 14: GBSVR-27301
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_workflow_items', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- 3. bRDS overriding previous manual override records (global_trader_book_id match): 

insert into bh_workflow
select  b1.asofdate, 
        20, 
        b1.book_id, 
        b1.global_trader_book_id, 
        b1.volcker_trading_desk, 
        b1.charge_reporting_unit_code, 
        b1.charge_reporting_parent_code, 
        b1.data_source, 
        b1.source_system, 
        'bRDS Overrides Manual entry (global_trader_book_id match)' comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    bh_staging  b1, 
        bh_staging  b2
where   upper(b1.data_source) = 'BRDS'
and     b1.book_id = b2.book_id
and     b1.global_trader_book_id = b2.global_trader_book_id
and     b1.last_modified_date > b2.last_modified_date
and     upper(nvl(b2.data_source, 'MANUAL')) = 'MANUAL';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_workflow_items', 'Step 4 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl_workflow_items');


end p_brds_etl_workflow_items;

--start GBSVR-28877
procedure p_brds_etl_val_non_vtd
as
begin

dbms_output.put_line('Running p_brds_etl_val_non_vtd');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_non_vtd', 'Step 1 Start', 'bRDS');

insert into bh_workflow
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY') asofdate, 
        11                                                 workflow_type_id, 
        b.bookName                                        book_id, 
        b.globalTraderBookId                              global_trader_book_id, 
        b.volckerTradingDesk                              volcker_trading_desk, 
        b.chargeReportingUnitCode                         charge_reporting_unit, 
        b.chargeReportingParentCode                       charge_reporting_parent, 
        'bRDS'                                             data_source, 
        b.tradeCaptureSystemName                          source_system_id, 
        'Book�s hierarchy contains two or more Non-VTD nodes'  comments
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
from    brds_vw_book  b
where   b.globalTraderBookId in ( 
  select h1.global_trader_book_id
  from   bh_ubr_desk_hierarchy  h1
  where ( select count(*) 
                  from bh_ubr_desk_hierarchy h2
                 where h2.global_trader_book_id = h1.global_trader_book_id 
                   -- GBSVR-35909: Start: 
                   and h2.node_type in ('UBR', 'DESK', 'PORTFOLIO') 
                   -- GBSVR-35909: End: 
                   and h2.non_vtd = 'Y') > 1);
                   
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_val_non_vtd', 'Step 1 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

dbms_output.put_line('Finished p_brds_etl_val_non_vtd');

end p_brds_etl_val_non_vtd;
--end GBSVR-28877


END PKG_BRDS_BH_RPL_WF;
