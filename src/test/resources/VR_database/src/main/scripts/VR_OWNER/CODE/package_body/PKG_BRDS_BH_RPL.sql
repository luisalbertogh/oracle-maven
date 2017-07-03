--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BRDS_BH_RPL runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BRDS_BH_RPL" AS



-- ********************************************************************** 
-- Procedure: p_brds_init
-- ********************************************************************** 

procedure p_brds_init ( pTableList IN varchar2, pRunId OUT int )
as
begin 

dbms_output.put_line('Running p_brds_init');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_init', 'Step 1 Start', 'bRDS');


select brds_runs_seq.NEXTVAL into pRunId from dual;

-- Delete previous entry
delete from brds_vw_status;
-- Insert a new entry into the Status table for this run: 
insert into brds_vw_status 
( runId, tables, startTime)
values ( pRunId, pTableList, current_timestamp ); 

commit;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_init', 'Step 2 End', 'bRDS');

dbms_output.put_line('Finished p_brds_init');


end p_brds_init;


-- ********************************************************************** 
-- Procedure: p_brds_etl_load_core_data
-- ********************************************************************** 

procedure p_brds_etl_load_core_data
as
begin 

dbms_output.put_line('Running p_brds_etl_load_core_data');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_core_data', 'Step 1 Start', 'bRDS');

-- ************************************************************************ 
-- Insert core data into Pre-Stage table: 


insert into bh_staging_intermed (
        asofdate, 
        book_id, 
        volcker_trading_desk, 
        volcker_trading_desk_full, 
        lowest_level_rpl_code, 
        lowest_level_rpl_full_name, 
        lowest_level_rpl, 
        source_system, 
        legal_entity, 
        global_trader_book_id, 
        profit_center_id, 
        comments, 
        data_source, 
        create_date, 
        last_modified_date, 
        charge_reporting_unit_code, 
        charge_reporting_unit, 
        charge_reporting_parent_code, 
        charge_reporting_parent, 
        mi_location, 
        portfolio_id, 
        portfolio_name, 
        portfolio_rpl_code,
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        acc_treat_category,
        primary_trader,
        primary_book_runner,
        primary_fincon,
        primary_moescalation,
        legal_entity_code,
        book_function_code,
        regulatory_reporting_treatment,
        ubr_ma_code,
        hierarchy_ubr_nodename,
        profit_centre_name,
        create_user, 
        last_modification_user )
select  to_char(last_day(current_date) + 1, 'DD-MON-YYYY') asofdate, 
        b.bookName                  book_id, 
        b.volckerTradingDesk        volcker_trading_desk, 
        NULL                        volcker_trading_desk_full, 
        NULL                        lowest_level_rpl_code, 
        NULL                        lowest_level_rpl_full_name, 
        NULL                        lowest_level_rpl, 
        NULL                        source_system,    -- We cannot map tradeCaptureSystemName to FeedID in dbVolt, so set this to NULL
        b.legalEntityName           legal_entity, 
        b.globalTraderBookId        global_trader_book_id, 
        b.profitCentreCode          profit_center_id, 
        'bRDS'                      comments, 
        'bRDS'                      data_source, 
        systimestamp                create_date, 
        systimestamp                last_modified_date, 
        b.chargeReportingUnitCode   charge_reporting_unit_code, 
        NULL                        charge_reporting_unit, 
        b.chargeReportingParentCode charge_reporting_parent_code, 
        NULL                        charge_reporting_parent, 
        b.miLocation                mi_location, 
        b.portfolioId               portfolio_id, 
        b.portfolioName             portfolio_name, 
        p.rplCode                   portfolio_rpl_code, 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        b.accTreatCategory			acc_treat_category,
        b.primaryTrader				primary_trader,
        b.primaryBookRunner			primary_book_runner,
        b.primaryFincon				primary_fincon,
        b.primaryMoescalation		primary_moescalation,
        b.legalEntityCode			legal_entity_code,
        b.bookFunctionCode			book_function_code,
        b.regulatoryReportingTreatment regulatory_reporting_treatment,
        b.ubrmacode					ubr_ma_code,
        h.nodename					hierarchy_ubr_nodename,
        b.profitcentre				profit_centre_name,
        'bRDS'                      create_user, 
        'bRDS'                      last_modification_user
from    brds_vw_book         b, 
        brds_vw_portfolio    p,
        brds_vw_hierarchy    h
where   b.portfolioId = p.portfolioId(+)
  and   b.ubrmacode = h.nodeId(+)
and     NOT exists ( select w.* from bh_workflow w where w.global_trader_book_id = b.globalTraderBookId 
                                                     and NVL(w.volcker_trading_desk, ' ') = NVL(b.volckerTradingDesk, ' ')
                                                     and NVL(w.charge_reporting_unit, ' ') = NVL(b.chargeReportingUnitCode, ' ')
                                                     and NVL(w.charge_reporting_parent, ' ') = NVL(b.chargeReportingParentCode, ' ')
                                                     and workflow_type_id in ( 1, 2, 3, 4, 5, 7, 26, 27, 28, 29 ) );
                                                     

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_core_data', 'Step 2 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;



dbms_output.put_line('Finished p_brds_etl_load_core_data');


end p_brds_etl_load_core_data;

-- ********************************************************************** 
-- Procedure: p_brds_etl_build_hierarchy
-- ********************************************************************** 

procedure p_brds_etl_build_hierarchy
as
begin 

dbms_output.put_line('Running p_brds_etl_build_hierarchy');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 1 Start', 'bRDS');

-- ************************************************************************ 
-- UBR/DESK level processing: Build hierarchy table: 


-- ************************************************************************ 
-- Clear down hierarchy table: 

execute immediate 'truncate table bh_ubr_desk_hierarchy';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');

-- ************************************************************************ 
-- Populate intermediary table with desk and ubr hierarchy: 
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
--nodetype='BOOK' for detected duplicate books (workflow type = 6)
start with h.nodetype = 'BOOK'
connect by prior parentNodeId = nodeId; 
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Delete all bh_ubr_desk_hierarchy which do not have a current global_trader_book_id: 
delete
from    bh_ubr_desk_hierarchy h
where   NOT exists ( select * from bh_staging_intermed i where i.global_trader_book_id = h.global_trader_book_id );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Set Number of desk levels
update  bh_ubr_desk_hierarchy h1
set     num_desks = ( select  count(*) 
                      from    bh_ubr_desk_hierarchy h2 
                      where   h2.global_trader_book_id = h1.global_trader_book_id 
                      and     h2.node_type = 'DESK' );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 5: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Set Number of ubr levels
update  bh_ubr_desk_hierarchy h1
set     num_ubrs = ( select  count(*) 
                      from    bh_ubr_desk_hierarchy h2 
                      where   h2.global_trader_book_id = h1.global_trader_book_id 
                      --start GBSVR-30032
                      --and     h2.node_name != 'Group (aggregated)'
                      --end GBSVR-30032
                      and     h2.node_type = 'UBR' );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 6: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Set adjusted ubr and desk levels: 
update  bh_ubr_desk_hierarchy
set     ubr_desk_level = ( 3 + num_ubrs + num_desks - ubr_desk_level )
where   node_type = 'UBR';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 7: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;



update  bh_ubr_desk_hierarchy 
set     ubr_desk_level = ( 3 + num_desks - ubr_desk_level )
where   node_type = 'DESK';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 8: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_ubr_desk_hierarchy 
set     ubr_desk_level = NULL
where   node_type NOT in ( 'UBR', 'DESK' );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_build_hierarchy', 'Step 9 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

dbms_output.put_line('Finished p_brds_etl_build_hierarchy');


end p_brds_etl_build_hierarchy;

-- ********************************************************************** 
-- Procedure: p_brds_etl_set_hierarchy
-- ********************************************************************** 

-- create or replace procedure p_brds_etl_set_hierarchy
procedure p_brds_etl_set_hierarchy
as
v_num_ctr                   int;
v_str_sql                   varchar2(2000);
begin 

dbms_output.put_line('Running p_brds_etl_set_hierarchy');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 1 Start', 'bRDS');

-- ************************************************************************ 
-- Set ubr level data on bh_staging_intermed: 


for v_num_ctr in 1..14 loop
  v_str_sql := 'update  bh_staging_intermed  b
    set     ( b.ubr_level_@@@_id, 
              b.ubr_level_@@@_name, 
              b.ubr_level_@@@_rpl_code ) = ( select t.node_id, 
                                                  t.node_name, 
                                                  t.rpl_code
                                           from   bh_ubr_desk_hierarchy t
                                           where  t.global_trader_book_id = b.global_trader_book_id
                                           and    t.node_type = ''UBR''
                                           and    t.ubr_desk_level = @@@ )
    where b.global_trader_book_id NOT in ( 
      select t2.global_trader_book_id
      from   bh_ubr_desk_hierarchy t2,
            bh_staging_intermed b2
      where  t2.global_trader_book_id = b2.global_trader_book_id
      and    t2.node_type = ''UBR''
      and    t2.ubr_desk_level = @@@
      group by t2.global_trader_book_id
      having count(*) > 1)
      and NOT EXISTS (
          select 1
            from bh_workflow bw
          where  bw.workflow_type_id = 33
          and    bw.global_trader_book_id = b.global_trader_book_id)';

  
  v_str_sql := replace( v_str_sql, '@@@', to_char(v_num_ctr));
  
  execute immediate v_str_sql;
 
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');
  
  commit;

  end loop;



-- ************************************************************************ 
-- Set desk level data on bh_staging_intermed: 


for v_num_ctr in 1..5
loop
  v_str_sql := 'update  bh_staging_intermed  b
    set     ( b.desk_level_@@@_id, 
              b.desk_level_@@@_name, 
              b.desk_level_@@@_rpl_code ) = ( select t.node_id, 
                                                     t.node_name, 
                                                     t.rpl_code
                                              from   bh_ubr_desk_hierarchy t
                                              where  t.global_trader_book_id = b.global_trader_book_id
                                              and    t.node_type = ''DESK''
                                              and    t.ubr_desk_level = @@@ )
    where b.global_trader_book_id NOT in ( 
    select t2.global_trader_book_id
    from   bh_ubr_desk_hierarchy t2, 
           bh_staging_intermed b2
    where  t2.global_trader_book_id = b2.global_trader_book_id
    and    t2.node_type = ''DESK''
    and    t2.ubr_desk_level = @@@ 
    group by t2.global_trader_book_id
    having count(*) > 1)
      and NOT EXISTS (
          select 1
            from bh_workflow bw
          where  bw.workflow_type_id = 33
          and    bw.global_trader_book_id = b.global_trader_book_id)';
  
  v_str_sql := replace( v_str_sql, '@@@', to_char(v_num_ctr));
  
  execute immediate v_str_sql;

  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');
  
  commit;

end loop;





-- ************************************************************************ 
-- Any ubr/desk levels that are not set should have the value of the lowest level that has been set: 

-- ************************************************************************ 
-- Desks: Set lowest level: 

update  bh_staging_intermed  b
set     b.desk_level_2_id       = b.desk_level_1_id, 
        b.desk_level_2_name     = b.desk_level_1_name, 
        b.desk_level_2_rpl_code = b.desk_level_1_rpl_code
where   b.desk_level_2_id is NULL
and     b.desk_level_1_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.desk_level_3_id       = b.desk_level_2_id, 
        b.desk_level_3_name     = b.desk_level_2_name, 
        b.desk_level_3_rpl_code = b.desk_level_2_rpl_code
where   b.desk_level_3_id is NULL
and     b.desk_level_2_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 5: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.desk_level_4_id       = b.desk_level_3_id, 
        b.desk_level_4_name     = b.desk_level_3_name, 
        b.desk_level_4_rpl_code = b.desk_level_3_rpl_code
where   b.desk_level_4_id is NULL
and     b.desk_level_3_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 6: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.desk_level_5_id       = b.desk_level_4_id, 
        b.desk_level_5_name     = b.desk_level_4_name, 
        b.desk_level_5_rpl_code = b.desk_level_4_rpl_code
where   b.desk_level_5_id is NULL
and     b.desk_level_4_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 7: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Ubrs: Set lowest level: 

update  bh_staging_intermed  b
set     b.ubr_level_2_id       = b.ubr_level_1_id, 
        b.ubr_level_2_name     = b.ubr_level_1_name, 
        b.ubr_level_2_rpl_code = b.ubr_level_1_rpl_code
where   b.ubr_level_2_id is NULL
and     b.ubr_level_1_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 8: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_3_id       = b.ubr_level_2_id, 
        b.ubr_level_3_name     = b.ubr_level_2_name, 
        b.ubr_level_3_rpl_code = b.ubr_level_2_rpl_code
where   b.ubr_level_3_id is NULL
and     b.ubr_level_2_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 9: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_4_id       = b.ubr_level_3_id, 
        b.ubr_level_4_name     = b.ubr_level_3_name, 
        b.ubr_level_4_rpl_code = b.ubr_level_3_rpl_code
where   b.ubr_level_4_id is NULL
and     b.ubr_level_3_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 10: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_5_id       = b.ubr_level_4_id, 
        b.ubr_level_5_name     = b.ubr_level_4_name, 
        b.ubr_level_5_rpl_code = b.ubr_level_4_rpl_code
where   b.ubr_level_5_id is NULL
and     b.ubr_level_4_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 11: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_6_id       = b.ubr_level_5_id, 
        b.ubr_level_6_name     = b.ubr_level_5_name, 
        b.ubr_level_6_rpl_code = b.ubr_level_5_rpl_code
where   b.ubr_level_6_id is NULL
and     b.ubr_level_5_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 12: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_7_id       = b.ubr_level_6_id, 
        b.ubr_level_7_name     = b.ubr_level_6_name, 
        b.ubr_level_7_rpl_code = b.ubr_level_6_rpl_code
where   b.ubr_level_7_id is NULL
and     b.ubr_level_6_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 13: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_8_id       = b.ubr_level_7_id, 
        b.ubr_level_8_name     = b.ubr_level_7_name, 
        b.ubr_level_8_rpl_code = b.ubr_level_7_rpl_code
where   b.ubr_level_8_id is NULL
and     b.ubr_level_7_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 14: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_9_id       = b.ubr_level_8_id, 
        b.ubr_level_9_name     = b.ubr_level_8_name, 
        b.ubr_level_9_rpl_code = b.ubr_level_8_rpl_code
where   b.ubr_level_9_id is NULL
and     b.ubr_level_8_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 15: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_10_id       = b.ubr_level_9_id, 
        b.ubr_level_10_name     = b.ubr_level_9_name, 
        b.ubr_level_10_rpl_code = b.ubr_level_9_rpl_code
where   b.ubr_level_10_id is NULL
and     b.ubr_level_9_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 16: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_11_id       = b.ubr_level_10_id, 
        b.ubr_level_11_name     = b.ubr_level_10_name, 
        b.ubr_level_11_rpl_code = b.ubr_level_10_rpl_code
where   b.ubr_level_11_id is NULL
and     b.ubr_level_10_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 17: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_12_id       = b.ubr_level_11_id, 
        b.ubr_level_12_name     = b.ubr_level_11_name, 
        b.ubr_level_12_rpl_code = b.ubr_level_11_rpl_code
where   b.ubr_level_12_id is NULL
and     b.ubr_level_11_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 18: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_13_id       = b.ubr_level_12_id, 
        b.ubr_level_13_name     = b.ubr_level_12_name, 
        b.ubr_level_13_rpl_code = b.ubr_level_12_rpl_code
where   b.ubr_level_13_id is NULL
and     b.ubr_level_12_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 19: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging_intermed  b
set     b.ubr_level_14_id       = b.ubr_level_13_id, 
        b.ubr_level_14_name     = b.ubr_level_13_name, 
        b.ubr_level_14_rpl_code = b.ubr_level_13_rpl_code
where   b.ubr_level_14_id is NULL
and     b.ubr_level_13_id is NOT NULL;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_set_hierarchy', 'Step 20 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;



dbms_output.put_line('Finished p_brds_etl_set_hierarchy');


end p_brds_etl_set_hierarchy;



-- ********************************************************************** 
-- Procedure: p_brds_etl_update_core_data
-- ********************************************************************** 

procedure p_brds_etl_update_core_data
as
begin 

dbms_output.put_line('Running p_brds_etl_update_core_data');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 1 Start', 'bRDS');




-- ************************************************************************ 
-- Update mi_location


update  bh_staging_intermed  b
set     b.mi_location = ( select upper(c.description) from brds_country c where c.country_code = b.mi_location);
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Update charge_reporting_unit_code: 

update  bh_staging_intermed  b
set     charge_reporting_unit     =  ( select   cru.chargeReportingUnit
                                       from     brds_vw_cru cru
                                       where    cru.chargeReportingUnitCode = b.charge_reporting_unit_code )
where exists (
        select  *
        from    brds_vw_cru cru2
        where   cru2.chargeReportingUnitCode = b.charge_reporting_unit_code );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Update charge_reporting_parent_code: 

update  bh_staging_intermed  b
set     charge_reporting_parent   =  ( select   crp.chargeReportingParent
                                       from     brds_vw_crp crp
                                       where    crp.chargeReportingParentCode = b.charge_reporting_parent_code )
where exists (
        select  *
        from    brds_vw_crp crp2
        where   crp2.chargeReportingParentCode = b.charge_reporting_parent_code );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Update region, subregion

update  bh_staging_intermed  b
set     ( region, 
          subregion ) =                    ( select  crp.chargeHierarchyRegion, 
                                                     crp.chargeSubArea 
                                             from    brds_vw_crp crp
                                             where   crp.chargeReportingParentCode = b.charge_reporting_parent_code );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 5: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- exceptional case. Region 'AMER' = 'AMERICAS' and string 'null' = null
update bh_staging_intermed b set region = 'AMERICAS' where upper(b.region) = 'AMER';
update bh_staging_intermed b set region = null where upper(b.region) = 'NULL';
update bh_staging_intermed b set subregion = null where upper(b.subregion) = 'NULL';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 5.1: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Update volcker_trading_desk_full: 

update  bh_staging_intermed  b
set     volcker_trading_desk_full = ( select  v.volckerTradingDeskFull
                                      from    brds_vw_vtd v
                                      where   v.volckerTradingDesk = b.volcker_trading_desk );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 6: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- lowest_level_rpl_code, and lowest_level_rpl_full_name:

update  bh_staging_intermed i
set     (i.lowest_level_rpl_code, i.lowest_level_rpl_full_name)  = ( select h.rpl_code, h.node_name
                                                                      from bh_ubr_desk_hierarchy h
                                                                     where h.global_trader_book_id = i.global_trader_book_id
                                                                       and h.node_type = 'PORTFOLIO' )
where i.global_trader_book_id NOT in ( 
    select t2.global_trader_book_id
    from   bh_ubr_desk_hierarchy t2, 
           bh_staging_intermed b2
    where  t2.global_trader_book_id = b2.global_trader_book_id
    and    t2.node_type = 'PORTFOLIO'
    group by t2.global_trader_book_id
    having count(*) > 1 ); 
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 7: '||to_char(SQL%ROWCOUNT), 'bRDS');


commit;

-- ************************************************************************ 
-- lowest_level_rpl (level number): 



update  bh_staging_intermed i
set     i.lowest_level_rpl = (( select  max(ubr_desk_level) 
                                from    bh_ubr_desk_hierarchy h
                                where   h.global_trader_book_id = i.global_trader_book_id
                                and     h.node_type = 'UBR' ) + 
                              ( select  max(ubr_desk_level) 
                                from    bh_ubr_desk_hierarchy h
                                where   h.global_trader_book_id = i.global_trader_book_id
                                and     h.node_type = 'DESK' ) + 
                              ( select  count(*) 
                                from    bh_ubr_desk_hierarchy h
                                where   h.global_trader_book_id = i.global_trader_book_id
                                and     h.node_type = 'PORTFOLIO' ));
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 8: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- business, sub_business: 


-- ************************************************************************ 
-- 8 or more Ubr levels: Just use levels 8 and 7 as sub-business and business

update  bh_staging_intermed i
set     i.business      = i.ubr_level_7_name, 
        i.sub_business  = i.ubr_level_8_name
where   i.global_trader_book_id in ( 
  select  h.global_trader_book_id
  from    bh_ubr_desk_hierarchy   h
  where   h.node_type = 'UBR'
  group by h.global_trader_book_id
  having max(h.ubr_desk_level) >= 8 );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 9: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- Fewer than 8 Ubr levels: Use level 14 Ubr name for sub-business and the next different one beneath that as business: 

update  bh_staging_intermed i
set     i.sub_business  = i.ubr_level_14_name
where   i.global_trader_book_id in ( 
  select  h.global_trader_book_id
  from    bh_ubr_desk_hierarchy   h
  where   h.node_type = 'UBR'
  group by h.global_trader_book_id
  having max(h.ubr_desk_level) < 8 );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 10: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


update  bh_staging_intermed i
set     i.business = case 
                      when ubr_level_7_name != ubr_level_14_name then ubr_level_7_name
                      when ubr_level_6_name != ubr_level_14_name then ubr_level_6_name
                      when ubr_level_5_name != ubr_level_14_name then ubr_level_5_name
                      when ubr_level_4_name != ubr_level_14_name then ubr_level_4_name
                      when ubr_level_3_name != ubr_level_14_name then ubr_level_3_name
                      when ubr_level_2_name != ubr_level_14_name then ubr_level_2_name
                      when ubr_level_1_name != ubr_level_14_name then ubr_level_1_name
                      else NULL
                     end
where   i.global_trader_book_id in ( 
  select  h.global_trader_book_id
  from    bh_ubr_desk_hierarchy   h
  where   h.node_type = 'UBR'
  group by h.global_trader_book_id
  having max(h.ubr_desk_level) < 8 );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_update_core_data', 'Step 11 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning


dbms_output.put_line('Finished p_brds_etl_update_core_data');


end p_brds_etl_update_core_data;

-- ********************************************************************** 
-- Procedure: p_brds_etl_non_vtd
-- **********************************************************************
procedure p_brds_etl_non_vtd
as
  v_non_vtd_asofdate date;
begin 


select  max(asofdate) into v_non_vtd_asofdate
from    bh_non_vtd
-- Start: GBSVR-31639
where   asofdate <= ( last_day( current_date ) + 1 );
-- End: GBSVR-31639 

 
update	bh_ubr_desk_hierarchy b
set   	b.non_vtd = 'Y'
where   exists (select * from bh_non_vtd nvtd
                 where nvtd.non_vtd_code  = b.node_id 
                   -- GBSVR-35909: Start: 
                   and lower(nvtd.non_vtd_type)  = lower(b.node_type)
                   and nvl(nvtd.non_vtd_level,0) = nvl(b.ubr_desk_level,0)
                   -- GBSVR-35909: End: 
                   and nvtd.asofdate = v_non_vtd_asofdate);
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_non_vtd', 'Step 1: '||to_char(SQL%ROWCOUNT), 'bRDS');


-- 2. Capture duplicates (bh workflow id 11)
pkg_brds_bh_rpl_wf.p_brds_etl_val_non_vtd;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_non_vtd', 'Step 2', 'bRDS');

-- 3. Set Non-VTD values
update bh_staging_intermed s
set (non_vtd_code,
    non_vtd_name,
    non_vtd_rpl_code,
    non_vtd_exclusion_type,
    non_vtd_division,  
	non_vtd_pvf, 
	non_vtd_business ) = (select h.node_id, 
                                 h.node_name,
                                 h.rpl_code,
                                 nvtd.non_vtd_exclusion_type,
                                 nvtd.non_vtd_division,
	 						     nvtd.non_vtd_pvf,
	 							 nvtd.non_vtd_business
                            from bh_ubr_desk_hierarchy h, bh_non_vtd nvtd
                           where h.global_trader_book_id = s.global_trader_book_id
                             and h.node_id = nvtd.non_vtd_code
                             and h.non_vtd = 'Y'
                             and not exists (select *
                                               from bh_workflow w
                                              where w.global_trader_book_id = h.global_trader_book_id
                                                and w.workflow_type_id = 11)
                             -- added this condition in order to protect the update against duplicates in BH_NON_VTD
                             and rownum = 1
                             and nvtd.asofdate = v_non_vtd_asofdate
);
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_non_vtd', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

-- 4. Update non_vtd_exclusion_type from bh_non_vtd_exceptions if needed
--BOOK_ID + GTB
update bh_staging_intermed s
set ( non_vtd_exclusion_type ) = (select nonvtde.value
                                 from bh_ubr_desk_hierarchy h,bh_non_vtd_exceptions nonvtde
                                 where h.global_trader_book_id = s.global_trader_book_id
                                  and h.non_vtd = 'Y'
                                  and nonvtde.book_name = s.book_id
                                  and nonvtde.global_trader_book_id = s.global_trader_book_id
                                  and nonvtde.EXCEPTION_TYPE like 'ET'
                                  -- GBSVR-34454: Start: 
                                  and rownum = 1
                                  -- GBSVR-34454: End: 
                                  and not exists (select *
                                                    from bh_workflow w
                                                       where w.global_trader_book_id = h.global_trader_book_id
                                                       and w.workflow_type_id = 11)
								)
				where exists (select *
                                 from bh_ubr_desk_hierarchy h2,bh_non_vtd_exceptions nonvtde2
                                 where h2.global_trader_book_id = s.global_trader_book_id
                                  and h2.non_vtd = 'Y'
                                  and nonvtde2.book_name = s.book_id
                                  and nonvtde2.global_trader_book_id = s.global_trader_book_id
                                  and nonvtde2.EXCEPTION_TYPE like 'ET'
                                  and not exists (select *
                                                    from bh_workflow w
                                                       where w.global_trader_book_id = h2.global_trader_book_id
                                                       and w.workflow_type_id = 11)
								 );
--BOOK_ID + null  _ GENERIC CASE
update bh_staging_intermed s
set ( non_vtd_exclusion_type ) = (select nonvtde.value
                                 from bh_ubr_desk_hierarchy h,bh_non_vtd_exceptions nonvtde
                                 where h.global_trader_book_id = s.global_trader_book_id
                                  and h.non_vtd = 'Y'
                                  and nonvtde.book_name = s.book_id
                                  and nonvtde.global_trader_book_id is null
                                  and nonvtde.EXCEPTION_TYPE like 'ET'
                                  -- GBSVR-34454: Start: 
                                  and rownum = 1
                                  -- GBSVR-34454: End: 
                                  and not exists (select *
                                                    from bh_workflow w
                                                       where w.global_trader_book_id = h.global_trader_book_id
                                                       and w.workflow_type_id = 11)
								)
				where exists (select *
                                 from bh_ubr_desk_hierarchy h2,bh_non_vtd_exceptions nonvtde2
                                 where h2.global_trader_book_id = s.global_trader_book_id
                                  and h2.non_vtd = 'Y'
                                  and nonvtde2.book_name = s.book_id
                                  and nonvtde2.global_trader_book_id is null
                                  and nonvtde2.EXCEPTION_TYPE like 'ET'
                                  and not exists (select *
                                                    from bh_workflow w
                                                       where w.global_trader_book_id = h2.global_trader_book_id
                                                       and w.workflow_type_id = 11)
								 );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_non_vtd', 'Step 4 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

end p_brds_etl_non_vtd;



-- ********************************************************************** 
-- Procedure: p_brds_etl_apply_deltas
-- ********************************************************************** 

procedure p_brds_etl_apply_deltas
as
v_num_manuals_this_month  int;
v_num_manuals_last_month  int;
begin 

dbms_output.put_line('Running p_brds_etl_apply_deltas');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_apply_deltas', 'Step 1 Start', 'bRDS');



-- ************************************************************************ 
-- Update any new or changed items in bh_staging from bh_staging_intermed. 
-- Similarly remove any books on bh_staging which are not in bh_staging_intermed
insert 
into bh_staging
  select asofdate,book_id,volcker_trading_desk,volcker_trading_desk_full,lowest_level_rpl_code,lowest_level_rpl_full_name,
      lowest_level_rpl,source_system,legal_entity,global_trader_book_id,profit_center_id,comments,data_source,create_date,
      last_modified_date,charge_reporting_unit_code,charge_reporting_unit,charge_reporting_parent_code,charge_reporting_parent,
      mi_location,ubr_level_1_id,ubr_level_1_name,ubr_level_1_rpl_code,ubr_level_2_id,ubr_level_2_name,ubr_level_2_rpl_code,ubr_level_3_id,
      ubr_level_3_name,ubr_level_3_rpl_code,ubr_level_4_id,ubr_level_4_name,ubr_level_4_rpl_code,ubr_level_5_id,ubr_level_5_name,ubr_level_5_rpl_code,
      ubr_level_6_id,ubr_level_6_name,ubr_level_6_rpl_code,ubr_level_7_id,ubr_level_7_name,ubr_level_7_rpl_code,ubr_level_8_id,ubr_level_8_name,
      ubr_level_8_rpl_code,ubr_level_9_id,ubr_level_9_name,ubr_level_9_rpl_code,ubr_level_10_id,ubr_level_10_name,ubr_level_10_rpl_code,ubr_level_11_id,
      ubr_level_11_name,ubr_level_11_rpl_code,ubr_level_12_id,ubr_level_12_name,ubr_level_12_rpl_code,ubr_level_13_id,ubr_level_13_name,ubr_level_13_rpl_code,
      ubr_level_14_id,ubr_level_14_name,ubr_level_14_rpl_code,desk_level_1_id,desk_level_1_name,desk_level_1_rpl_code,desk_level_2_id,desk_level_2_name,
      desk_level_2_rpl_code,desk_level_3_id,desk_level_3_name,desk_level_3_rpl_code,desk_level_4_id,desk_level_4_name,desk_level_4_rpl_code,desk_level_5_id,
      desk_level_5_name,desk_level_5_rpl_code,portfolio_id,portfolio_name,portfolio_rpl_code,business,sub_business,create_user,last_modification_user,region,
      subregion,overridden_flag,active_flag,emergency_flag,bh_intermediary_id,approver_user,approval_date,'N' rpl_load, 
      -- GBSVR-33754 Start: CFBU decommissioning
      -- GBSVR-33754 End:   CFBU decommissioning
      acc_treat_category, primary_trader, primary_book_runner, primary_fincon, primary_moescalation, legal_entity_code, book_function_code,
      regulatory_reporting_treatment, ubr_ma_code, hierarchy_ubr_nodename,
      profit_centre_name,
      non_vtd_code,
      non_vtd_name,
      non_vtd_rpl_code,
      non_vtd_exclusion_type,
      'N' volcker_reportable_flag,
	  non_vtd_division,  
	  non_vtd_pvf, 
	  non_vtd_business 
      from  bh_staging_intermed i
where   NOT exists (select * from bh_staging s where s.data_source = 'bRDS' and s.global_trader_book_id = i.global_trader_book_id );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_apply_deltas', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');


commit;

delete
from    bh_staging  s
where   s.data_source = 'bRDS'
and     NOT exists (select  i.* 
                    from    bh_staging_intermed i 
                    where   i.global_trader_book_id = s.global_trader_book_id and s.data_source = 'bRDS' );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_apply_deltas', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- Update bh_staging based on global_trader_book_id, but ONLY if any field value has changed: i.e. deltas only. 
update  bh_staging  s
set     ( s.book_id, 
          s.volcker_trading_desk, 
          s.volcker_trading_desk_full, 
          s.lowest_level_rpl_code, 
          s.lowest_level_rpl_full_name, 
          s.lowest_level_rpl, 
          s.source_system, 
          s.legal_entity, 
          s.profit_center_id, 
          s.comments, 
          s.data_source, 
          s.last_modified_date, 
          s.charge_reporting_unit_code, 
          s.charge_reporting_unit, 
          s.charge_reporting_parent_code, 
          s.charge_reporting_parent, 
          s.mi_location, 
          s.ubr_level_1_id, 
          s.ubr_level_1_name, 
          s.ubr_level_1_rpl_code, 
          s.ubr_level_2_id, 
          s.ubr_level_2_name, 
          s.ubr_level_2_rpl_code, 
          s.ubr_level_3_id, 
          s.ubr_level_3_name, 
          s.ubr_level_3_rpl_code, 
          s.ubr_level_4_id, 
          s.ubr_level_4_name, 
          s.ubr_level_4_rpl_code, 
          s.ubr_level_5_id, 
          s.ubr_level_5_name, 
          s.ubr_level_5_rpl_code, 
          s.ubr_level_6_id, 
          s.ubr_level_6_name, 
          s.ubr_level_6_rpl_code, 
          s.ubr_level_7_id, 
          s.ubr_level_7_name, 
          s.ubr_level_7_rpl_code, 
          s.ubr_level_8_id, 
          s.ubr_level_8_name, 
          s.ubr_level_8_rpl_code, 
          s.ubr_level_9_id, 
          s.ubr_level_9_name, 
          s.ubr_level_9_rpl_code, 
          s.ubr_level_10_id, 
          s.ubr_level_10_name, 
          s.ubr_level_10_rpl_code, 
          s.ubr_level_11_id, 
          s.ubr_level_11_name, 
          s.ubr_level_11_rpl_code, 
          s.ubr_level_12_id, 
          s.ubr_level_12_name, 
          s.ubr_level_12_rpl_code, 
          s.ubr_level_13_id, 
          s.ubr_level_13_name, 
          s.ubr_level_13_rpl_code, 
          s.ubr_level_14_id, 
          s.ubr_level_14_name, 
          s.ubr_level_14_rpl_code, 
          s.desk_level_1_id, 
          s.desk_level_1_name, 
          s.desk_level_1_rpl_code, 
          s.desk_level_2_id, 
          s.desk_level_2_name, 
          s.desk_level_2_rpl_code, 
          s.desk_level_3_id, 
          s.desk_level_3_name, 
          s.desk_level_3_rpl_code, 
          s.desk_level_4_id, 
          s.desk_level_4_name, 
          s.desk_level_4_rpl_code, 
          s.desk_level_5_id, 
          s.desk_level_5_name, 
          s.desk_level_5_rpl_code, 
          s.portfolio_id, 
          s.portfolio_name, 
          s.portfolio_rpl_code, 
          s.business, 
          s.sub_business, 
          s.region, 
          s.subregion, 
          s.bh_intermediary_id, 
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          s.acc_treat_category,
          s.primary_trader,
          s.primary_book_runner,
          s.primary_fincon,
          s.primary_moescalation,
          s.legal_entity_code,
          s.book_function_code,
          s.regulatory_reporting_treatment, 
          s.ubr_ma_code, 
          s.hierarchy_ubr_nodename,
          s.profit_centre_name,
          s.non_vtd_code,
          s.non_vtd_name,
          s.non_vtd_rpl_code,
          s.non_vtd_exclusion_type,
			s.non_vtd_division,  
			s.non_vtd_pvf, 
			s.non_vtd_business ) 
			= ( select i.book_id, 
                                            i.volcker_trading_desk, 
                                            i.volcker_trading_desk_full, 
                                            i.lowest_level_rpl_code, 
                                            i.lowest_level_rpl_full_name, 
                                            i.lowest_level_rpl, 
                                            i.source_system, 
                                            i.legal_entity, 
                                            i.profit_center_id, 
                                            i.comments, 
                                            i.data_source, 
                                            systimestamp, 
                                            i.charge_reporting_unit_code, 
                                            i.charge_reporting_unit, 
                                            i.charge_reporting_parent_code, 
                                            i.charge_reporting_parent, 
                                            i.mi_location, 
                                            i.ubr_level_1_id, 
                                            i.ubr_level_1_name, 
                                            i.ubr_level_1_rpl_code, 
                                            i.ubr_level_2_id, 
                                            i.ubr_level_2_name, 
                                            i.ubr_level_2_rpl_code, 
                                            i.ubr_level_3_id, 
                                            i.ubr_level_3_name, 
                                            i.ubr_level_3_rpl_code, 
                                            i.ubr_level_4_id, 
                                            i.ubr_level_4_name, 
                                            i.ubr_level_4_rpl_code, 
                                            i.ubr_level_5_id, 
                                            i.ubr_level_5_name, 
                                            i.ubr_level_5_rpl_code, 
                                            i.ubr_level_6_id, 
                                            i.ubr_level_6_name, 
                                            i.ubr_level_6_rpl_code, 
                                            i.ubr_level_7_id, 
                                            i.ubr_level_7_name, 
                                            i.ubr_level_7_rpl_code, 
                                            i.ubr_level_8_id, 
                                            i.ubr_level_8_name, 
                                            i.ubr_level_8_rpl_code, 
                                            i.ubr_level_9_id, 
                                            i.ubr_level_9_name, 
                                            i.ubr_level_9_rpl_code, 
                                            i.ubr_level_10_id, 
                                            i.ubr_level_10_name, 
                                            i.ubr_level_10_rpl_code, 
                                            i.ubr_level_11_id, 
                                            i.ubr_level_11_name, 
                                            i.ubr_level_11_rpl_code, 
                                            i.ubr_level_12_id, 
                                            i.ubr_level_12_name, 
                                            i.ubr_level_12_rpl_code, 
                                            i.ubr_level_13_id, 
                                            i.ubr_level_13_name, 
                                            i.ubr_level_13_rpl_code, 
                                            i.ubr_level_14_id, 
                                            i.ubr_level_14_name, 
                                            i.ubr_level_14_rpl_code, 
                                            i.desk_level_1_id, 
                                            i.desk_level_1_name, 
                                            i.desk_level_1_rpl_code, 
                                            i.desk_level_2_id, 
                                            i.desk_level_2_name, 
                                            i.desk_level_2_rpl_code, 
                                            i.desk_level_3_id, 
                                            i.desk_level_3_name, 
                                            i.desk_level_3_rpl_code, 
                                            i.desk_level_4_id, 
                                            i.desk_level_4_name, 
                                            i.desk_level_4_rpl_code, 
                                            i.desk_level_5_id, 
                                            i.desk_level_5_name, 
                                            i.desk_level_5_rpl_code, 
                                            i.portfolio_id, 
                                            i.portfolio_name, 
                                            i.portfolio_rpl_code, 
                                            i.business, 
                                            i.sub_business, 
                                            i.region, 
                                            i.subregion, 
                                            i.bh_intermediary_id, 
                                            -- GBSVR-33754 Start: CFBU decommissioning
                                            -- GBSVR-33754 End:   CFBU decommissioning
                                            i.acc_treat_category,
                                            i.primary_trader,
                                            i.primary_book_runner,
                                            i.primary_fincon,
                                            i.primary_moescalation,
                                            i.legal_entity_code,
                                            i.book_function_code,
                                            i.regulatory_reporting_treatment,
                                            i.ubr_ma_code,
                                            i.hierarchy_ubr_nodename,
                                            i.profit_centre_name,
                                            i.non_vtd_code,
                                            i.non_vtd_name,
                                            i.non_vtd_rpl_code,
                                            i.non_vtd_exclusion_type,
											i.non_vtd_division,  
											i.non_vtd_pvf, 
											i.non_vtd_business 
                                     from   bh_staging_intermed i 
                                     where  i.global_trader_book_id = s.global_trader_book_id 
                                     and    i.data_source = 'bRDS'
                                     and    s.data_source = 'bRDS'
                                     and  ( nvl(i.book_id,' ') != nvl(s.book_id,' ') or 
                                            nvl(i.volcker_trading_desk,' ') != nvl(s.volcker_trading_desk,' ') or 
                                            nvl(i.volcker_trading_desk_full,' ') != nvl(s.volcker_trading_desk_full,' ') or 
                                            nvl(i.lowest_level_rpl_code,' ') != nvl(s.lowest_level_rpl_code,' ') or 
                                            nvl(i.lowest_level_rpl_full_name,' ') != nvl(s.lowest_level_rpl_full_name,' ') or 
                                            nvl(i.lowest_level_rpl,' ') != nvl(s.lowest_level_rpl,' ') or 
                                            nvl(i.source_system,' ') != nvl(s.source_system,' ') or 
                                            nvl(i.legal_entity,' ') != nvl(s.legal_entity,' ') or 
                                            nvl(i.profit_center_id,' ') != nvl(s.profit_center_id,' ') or 
                                            nvl(i.comments,' ') != nvl(s.comments,' ') or 
                                            nvl(i.charge_reporting_unit_code,' ') != nvl(s.charge_reporting_unit_code,' ') or 
                                            nvl(i.charge_reporting_unit,' ') != nvl(s.charge_reporting_unit,' ') or 
                                            nvl(i.charge_reporting_parent_code,' ') != nvl(s.charge_reporting_parent_code,' ') or 
                                            nvl(i.charge_reporting_parent,' ') != nvl(s.charge_reporting_parent,' ') or 
                                            nvl(i.mi_location,' ') != nvl(s.mi_location,' ') or 
                                            nvl(i.ubr_level_1_id,' ') != nvl(s.ubr_level_1_id,' ') or 
                                            nvl(i.ubr_level_1_name,' ') != nvl(s.ubr_level_1_name,' ') or 
                                            nvl(i.ubr_level_1_rpl_code,' ') != nvl(s.ubr_level_1_rpl_code,' ') or 
                                            nvl(i.ubr_level_2_id,' ') != nvl(s.ubr_level_2_id,' ') or 
                                            nvl(i.ubr_level_2_name,' ') != nvl(s.ubr_level_2_name,' ') or 
                                            nvl(i.ubr_level_2_rpl_code,' ') != nvl(s.ubr_level_2_rpl_code,' ') or 
                                            nvl(i.ubr_level_3_id,' ') != nvl(s.ubr_level_3_id,' ') or 
                                            nvl(i.ubr_level_3_name,' ') != nvl(s.ubr_level_3_name,' ') or 
                                            nvl(i.ubr_level_3_rpl_code,' ') != nvl(s.ubr_level_3_rpl_code,' ') or 
                                            nvl(i.ubr_level_4_id,' ') != nvl(s.ubr_level_4_id,' ') or 
                                            nvl(i.ubr_level_4_name,' ') != nvl(s.ubr_level_4_name,' ') or 
                                            nvl(i.ubr_level_4_rpl_code,' ') != nvl(s.ubr_level_4_rpl_code,' ') or 
                                            nvl(i.ubr_level_5_id,' ') != nvl(s.ubr_level_5_id,' ') or 
                                            nvl(i.ubr_level_5_name,' ') != nvl(s.ubr_level_5_name,' ') or 
                                            nvl(i.ubr_level_5_rpl_code,' ') != nvl(s.ubr_level_5_rpl_code,' ') or 
                                            nvl(i.ubr_level_6_id,' ') != nvl(s.ubr_level_6_id,' ') or 
                                            nvl(i.ubr_level_6_name,' ') != nvl(s.ubr_level_6_name,' ') or 
                                            nvl(i.ubr_level_6_rpl_code,' ') != nvl(s.ubr_level_6_rpl_code,' ') or 
                                            nvl(i.ubr_level_7_id,' ') != nvl(s.ubr_level_7_id,' ') or 
                                            nvl(i.ubr_level_7_name,' ') != nvl(s.ubr_level_7_name,' ') or 
                                            nvl(i.ubr_level_7_rpl_code,' ') != nvl(s.ubr_level_7_rpl_code,' ') or 
                                            nvl(i.ubr_level_8_id,' ') != nvl(s.ubr_level_8_id,' ') or 
                                            nvl(i.ubr_level_8_name,' ') != nvl(s.ubr_level_8_name,' ') or 
                                            nvl(i.ubr_level_8_rpl_code,' ') != nvl(s.ubr_level_8_rpl_code,' ') or 
                                            nvl(i.ubr_level_9_id,' ') != nvl(s.ubr_level_9_id,' ') or 
                                            nvl(i.ubr_level_9_name,' ') != nvl(s.ubr_level_9_name,' ') or 
                                            nvl(i.ubr_level_9_rpl_code,' ') != nvl(s.ubr_level_9_rpl_code,' ') or 
                                            nvl(i.ubr_level_10_id,' ') != nvl(s.ubr_level_10_id,' ') or 
                                            nvl(i.ubr_level_10_name,' ') != nvl(s.ubr_level_10_name,' ') or 
                                            nvl(i.ubr_level_10_rpl_code,' ') != nvl(s.ubr_level_10_rpl_code,' ') or 
                                            nvl(i.ubr_level_11_id,' ') != nvl(s.ubr_level_11_id,' ') or 
                                            nvl(i.ubr_level_11_name,' ') != nvl(s.ubr_level_11_name,' ') or 
                                            nvl(i.ubr_level_11_rpl_code,' ') != nvl(s.ubr_level_11_rpl_code,' ') or 
                                            nvl(i.ubr_level_12_id,' ') != nvl(s.ubr_level_12_id,' ') or 
                                            nvl(i.ubr_level_12_name,' ') != nvl(s.ubr_level_12_name,' ') or 
                                            nvl(i.ubr_level_12_rpl_code,' ') != nvl(s.ubr_level_12_rpl_code,' ') or 
                                            nvl(i.ubr_level_13_id,' ') != nvl(s.ubr_level_13_id,' ') or 
                                            nvl(i.ubr_level_13_name,' ') != nvl(s.ubr_level_13_name,' ') or 
                                            nvl(i.ubr_level_13_rpl_code,' ') != nvl(s.ubr_level_13_rpl_code,' ') or 
                                            nvl(i.ubr_level_14_id,' ') != nvl(s.ubr_level_14_id,' ') or 
                                            nvl(i.ubr_level_14_name,' ') != nvl(s.ubr_level_14_name,' ') or 
                                            nvl(i.ubr_level_14_rpl_code,' ') != nvl(s.ubr_level_14_rpl_code,' ') or 
                                            nvl(i.desk_level_1_id,' ') != nvl(s.desk_level_1_id,' ') or 
                                            nvl(i.desk_level_1_name,' ') != nvl(s.desk_level_1_name,' ') or 
                                            nvl(i.desk_level_1_rpl_code,' ') != nvl(s.desk_level_1_rpl_code,' ') or 
                                            nvl(i.desk_level_2_id,' ') != nvl(s.desk_level_2_id,' ') or 
                                            nvl(i.desk_level_2_name,' ') != nvl(s.desk_level_2_name,' ') or 
                                            nvl(i.desk_level_2_rpl_code,' ') != nvl(s.desk_level_2_rpl_code,' ') or 
                                            nvl(i.desk_level_3_id,' ') != nvl(s.desk_level_3_id,' ') or 
                                            nvl(i.desk_level_3_name,' ') != nvl(s.desk_level_3_name,' ') or 
                                            nvl(i.desk_level_3_rpl_code,' ') != nvl(s.desk_level_3_rpl_code,' ') or 
                                            nvl(i.desk_level_4_id,' ') != nvl(s.desk_level_4_id,' ') or 
                                            nvl(i.desk_level_4_name,' ') != nvl(s.desk_level_4_name,' ') or 
                                            nvl(i.desk_level_4_rpl_code,' ') != nvl(s.desk_level_4_rpl_code,' ') or 
                                            nvl(i.desk_level_5_id,' ') != nvl(s.desk_level_5_id,' ') or 
                                            nvl(i.desk_level_5_name,' ') != nvl(s.desk_level_5_name,' ') or 
                                            nvl(i.desk_level_5_rpl_code,' ') != nvl(s.desk_level_5_rpl_code,' ') or 
                                            nvl(i.portfolio_id,' ') != nvl(s.portfolio_id,' ') or 
                                            nvl(i.portfolio_name,' ') != nvl(s.portfolio_name,' ') or 
                                            nvl(i.portfolio_rpl_code,' ') != nvl(s.portfolio_rpl_code,' ') or 
                                            nvl(i.business,' ') != nvl(s.business,' ') or 
                                            nvl(i.sub_business,' ') != nvl(s.sub_business,' ') or 
                                            nvl(i.region,' ') != nvl(s.region,' ') or 
                                            nvl(i.subregion,' ') != nvl(s.subregion,' ') or 
                                            nvl(i.bh_intermediary_id, 0) != nvl(s.bh_intermediary_id, 0) or
                                            -- GBSVR-33754 Start: CFBU decommissioning
                                            -- GBSVR-33754 End:   CFBU decommissioning
                                            nvl(i.acc_treat_category, ' ') != nvl(s.acc_treat_category, ' ') or
                                            nvl(i.primary_trader, ' ') != nvl(s.primary_trader, ' ') or
                                            nvl(i.primary_book_runner, ' ') != nvl(s.primary_book_runner, ' ') or
                                            nvl(i.primary_fincon, ' ') != nvl(s.primary_fincon, ' ') or
                                            nvl(i.primary_moescalation, ' ') != nvl(s.primary_moescalation, ' ') or
                                            nvl(i.legal_entity_code, ' ') != nvl(s.legal_entity_code, ' ') or
                                            nvl(i.book_function_code, ' ') != nvl(s.book_function_code, ' ') or
                                            nvl(i.regulatory_reporting_treatment, ' ') != nvl(s.regulatory_reporting_treatment, ' ') or
                                            nvl(i.ubr_ma_code, ' ') != nvl(s.ubr_ma_code, ' ') or
                                            nvl(i.hierarchy_ubr_nodename, ' ') != nvl(s.hierarchy_ubr_nodename, ' ') or
                                            nvl(i.profit_centre_name, ' ') != nvl(s.profit_centre_name, ' ') or
                                            --start GBSVR-28875
                                            nvl(i.non_vtd_code, ' ') != nvl(s.non_vtd_code, ' ') or
                                            nvl(i.non_vtd_name, ' ') != nvl(s.non_vtd_name, ' ') or
                                            nvl(i.non_vtd_rpl_code, ' ') != nvl(s.non_vtd_rpl_code, ' ') or
                                            nvl(i.non_vtd_exclusion_type, ' ') != nvl(s.non_vtd_exclusion_type, ' ') or
											nvl(i.non_vtd_division, ' ') != nvl(s.non_vtd_division, ' ') or
                                            nvl(i.non_vtd_pvf, ' ') != nvl(s.non_vtd_pvf, ' ') or
                                            nvl(i.non_vtd_business, ' ') != nvl(s.non_vtd_business, ' ')
											))
where                                s.data_source = 'bRDS'
and   exists (                       select * from bh_staging_intermed i
                                     where  i.global_trader_book_id = s.global_trader_book_id 

and    i.data_source = 'bRDS'
                                     and    s.data_source = 'bRDS'
                                     and  ( nvl(i.book_id,' ') != nvl(s.book_id,' ') or 
                                            nvl(i.volcker_trading_desk,' ') != nvl(s.volcker_trading_desk,' ') or 
                                            nvl(i.volcker_trading_desk_full,' ') != nvl(s.volcker_trading_desk_full,' ') or 
                                            nvl(i.lowest_level_rpl_code,' ') != nvl(s.lowest_level_rpl_code,' ') or 
                                            nvl(i.lowest_level_rpl_full_name,' ') != nvl(s.lowest_level_rpl_full_name,' ') or 
                                            nvl(i.lowest_level_rpl,' ') != nvl(s.lowest_level_rpl,' ') or 
                                            nvl(i.source_system,' ') != nvl(s.source_system,' ') or 
                                            nvl(i.legal_entity,' ') != nvl(s.legal_entity,' ') or 
                                            nvl(i.profit_center_id,' ') != nvl(s.profit_center_id,' ') or 
                                            nvl(i.comments,' ') != nvl(s.comments,' ') or 
                                            nvl(i.data_source,' ') != nvl(s.data_source,' ') or 
                                            nvl(i.charge_reporting_unit_code,' ') != nvl(s.charge_reporting_unit_code,' ') or 
                                            nvl(i.charge_reporting_unit,' ') != nvl(s.charge_reporting_unit,' ') or 
                                            nvl(i.charge_reporting_parent_code,' ') != nvl(s.charge_reporting_parent_code,' ') or 
                                            nvl(i.charge_reporting_parent,' ') != nvl(s.charge_reporting_parent,' ') or 
                                            nvl(i.mi_location,' ') != nvl(s.mi_location,' ') or 
                                            nvl(i.ubr_level_1_id,' ') != nvl(s.ubr_level_1_id,' ') or 
                                            nvl(i.ubr_level_1_name,' ') != nvl(s.ubr_level_1_name,' ') or 
                                            nvl(i.ubr_level_1_rpl_code,' ') != nvl(s.ubr_level_1_rpl_code,' ') or 
                                            nvl(i.ubr_level_2_id,' ') != nvl(s.ubr_level_2_id,' ') or 
                                            nvl(i.ubr_level_2_name,' ') != nvl(s.ubr_level_2_name,' ') or 
                                            nvl(i.ubr_level_2_rpl_code,' ') != nvl(s.ubr_level_2_rpl_code,' ') or 
                                            nvl(i.ubr_level_3_id,' ') != nvl(s.ubr_level_3_id,' ') or 
                                            nvl(i.ubr_level_3_name,' ') != nvl(s.ubr_level_3_name,' ') or 
                                            nvl(i.ubr_level_3_rpl_code,' ') != nvl(s.ubr_level_3_rpl_code,' ') or 
                                            nvl(i.ubr_level_4_id,' ') != nvl(s.ubr_level_4_id,' ') or 
                                            nvl(i.ubr_level_4_name,' ') != nvl(s.ubr_level_4_name,' ') or 
                                            nvl(i.ubr_level_4_rpl_code,' ') != nvl(s.ubr_level_4_rpl_code,' ') or 
                                            nvl(i.ubr_level_5_id,' ') != nvl(s.ubr_level_5_id,' ') or 
                                            nvl(i.ubr_level_5_name,' ') != nvl(s.ubr_level_5_name,' ') or 
                                            nvl(i.ubr_level_5_rpl_code,' ') != nvl(s.ubr_level_5_rpl_code,' ') or 
                                            nvl(i.ubr_level_6_id,' ') != nvl(s.ubr_level_6_id,' ') or 
                                            nvl(i.ubr_level_6_name,' ') != nvl(s.ubr_level_6_name,' ') or 
                                            nvl(i.ubr_level_6_rpl_code,' ') != nvl(s.ubr_level_6_rpl_code,' ') or 
                                            nvl(i.ubr_level_7_id,' ') != nvl(s.ubr_level_7_id,' ') or 
                                            nvl(i.ubr_level_7_name,' ') != nvl(s.ubr_level_7_name,' ') or 
                                            nvl(i.ubr_level_7_rpl_code,' ') != nvl(s.ubr_level_7_rpl_code,' ') or 
                                            nvl(i.ubr_level_8_id,' ') != nvl(s.ubr_level_8_id,' ') or 
                                            nvl(i.ubr_level_8_name,' ') != nvl(s.ubr_level_8_name,' ') or 
                                            nvl(i.ubr_level_8_rpl_code,' ') != nvl(s.ubr_level_8_rpl_code,' ') or 
                                            nvl(i.ubr_level_9_id,' ') != nvl(s.ubr_level_9_id,' ') or 
                                            nvl(i.ubr_level_9_name,' ') != nvl(s.ubr_level_9_name,' ') or 
                                            nvl(i.ubr_level_9_rpl_code,' ') != nvl(s.ubr_level_9_rpl_code,' ') or 
                                            nvl(i.ubr_level_10_id,' ') != nvl(s.ubr_level_10_id,' ') or 
                                            nvl(i.ubr_level_10_name,' ') != nvl(s.ubr_level_10_name,' ') or 
                                            nvl(i.ubr_level_10_rpl_code,' ') != nvl(s.ubr_level_10_rpl_code,' ') or 
                                            nvl(i.ubr_level_11_id,' ') != nvl(s.ubr_level_11_id,' ') or 
                                            nvl(i.ubr_level_11_name,' ') != nvl(s.ubr_level_11_name,' ') or 
                                            nvl(i.ubr_level_11_rpl_code,' ') != nvl(s.ubr_level_11_rpl_code,' ') or 
                                            nvl(i.ubr_level_12_id,' ') != nvl(s.ubr_level_12_id,' ') or 
                                            nvl(i.ubr_level_12_name,' ') != nvl(s.ubr_level_12_name,' ') or 
                                            nvl(i.ubr_level_12_rpl_code,' ') != nvl(s.ubr_level_12_rpl_code,' ') or 
                                            nvl(i.ubr_level_13_id,' ') != nvl(s.ubr_level_13_id,' ') or 
                                            nvl(i.ubr_level_13_name,' ') != nvl(s.ubr_level_13_name,' ') or 
                                            nvl(i.ubr_level_13_rpl_code,' ') != nvl(s.ubr_level_13_rpl_code,' ') or 
                                            nvl(i.ubr_level_14_id,' ') != nvl(s.ubr_level_14_id,' ') or 
                                            nvl(i.ubr_level_14_name,' ') != nvl(s.ubr_level_14_name,' ') or 
                                            nvl(i.ubr_level_14_rpl_code,' ') != nvl(s.ubr_level_14_rpl_code,' ') or 
                                            nvl(i.desk_level_1_id,' ') != nvl(s.desk_level_1_id,' ') or 
                                            nvl(i.desk_level_1_name,' ') != nvl(s.desk_level_1_name,' ') or 
                                            nvl(i.desk_level_1_rpl_code,' ') != nvl(s.desk_level_1_rpl_code,' ') or 
                                            nvl(i.desk_level_2_id,' ') != nvl(s.desk_level_2_id,' ') or 
                                            nvl(i.desk_level_2_name,' ') != nvl(s.desk_level_2_name,' ') or 
                                            nvl(i.desk_level_2_rpl_code,' ') != nvl(s.desk_level_2_rpl_code,' ') or 
                                            nvl(i.desk_level_3_id,' ') != nvl(s.desk_level_3_id,' ') or 
                                            nvl(i.desk_level_3_name,' ') != nvl(s.desk_level_3_name,' ') or 
                                            nvl(i.desk_level_3_rpl_code,' ') != nvl(s.desk_level_3_rpl_code,' ') or 
                                            nvl(i.desk_level_4_id,' ') != nvl(s.desk_level_4_id,' ') or 
                                            nvl(i.desk_level_4_name,' ') != nvl(s.desk_level_4_name,' ') or 
                                            nvl(i.desk_level_4_rpl_code,' ') != nvl(s.desk_level_4_rpl_code,' ') or 
                                            nvl(i.desk_level_5_id,' ') != nvl(s.desk_level_5_id,' ') or 
                                            nvl(i.desk_level_5_name,' ') != nvl(s.desk_level_5_name,' ') or 
                                            nvl(i.desk_level_5_rpl_code,' ') != nvl(s.desk_level_5_rpl_code,' ') or 
                                            nvl(i.portfolio_id,' ') != nvl(s.portfolio_id,' ') or 
                                            nvl(i.portfolio_name,' ') != nvl(s.portfolio_name,' ') or 
                                            nvl(i.portfolio_rpl_code,' ') != nvl(s.portfolio_rpl_code,' ') or 
                                            nvl(i.business,' ') != nvl(s.business,' ') or 
                                            nvl(i.sub_business,' ') != nvl(s.sub_business,' ') or 
                                            nvl(i.region,' ') != nvl(s.region,' ') or 
                                            nvl(i.subregion,' ') != nvl(s.subregion,' ') or 
                                            nvl(i.bh_intermediary_id, 0) != nvl(s.bh_intermediary_id, 0) or
                                            -- GBSVR-33754 Start: CFBU decommissioning
                                            -- GBSVR-33754 End:   CFBU decommissioning
                                            nvl(i.acc_treat_category, ' ') != nvl(s.acc_treat_category, ' ') or
                                            nvl(i.primary_trader, ' ') != nvl(s.primary_trader, ' ') or
                                            nvl(i.primary_book_runner, ' ') != nvl(s.primary_book_runner, ' ') or
                                            nvl(i.primary_fincon, ' ') != nvl(s.primary_fincon, ' ') or
                                            nvl(i.primary_moescalation, ' ') != nvl(s.primary_moescalation, ' ') or
                                            nvl(i.legal_entity_code, ' ') != nvl(s.legal_entity_code, ' ') or
                                            nvl(i.book_function_code, ' ') != nvl(s.book_function_code, ' ') or
                                            nvl(i.regulatory_reporting_treatment, ' ') != nvl(s.regulatory_reporting_treatment, ' ') or
                                            nvl(i.ubr_ma_code, ' ') != nvl(s.ubr_ma_code, ' ') or
                                            nvl(i.hierarchy_ubr_nodename, ' ') != nvl(s.hierarchy_ubr_nodename, ' ') or
                                            nvl(i.profit_centre_name, ' ') != nvl(s.profit_centre_name, ' ') or
                                            --start GBSVR-28875
                                            nvl(i.non_vtd_code, ' ') != nvl(s.non_vtd_code, ' ') or
                                            nvl(i.non_vtd_name, ' ') != nvl(s.non_vtd_name, ' ') or
                                            nvl(i.non_vtd_rpl_code, ' ') != nvl(s.non_vtd_rpl_code, ' ') or
                                            nvl(i.non_vtd_exclusion_type, ' ') != nvl(s.non_vtd_exclusion_type, ' ') or
											nvl(i.non_vtd_division, ' ') != nvl(s.non_vtd_division, ' ') or
                                            nvl(i.non_vtd_pvf, ' ') != nvl(s.non_vtd_pvf, ' ') or
                                            nvl(i.non_vtd_business, ' ') != nvl(s.non_vtd_business, ' ')
											));
                                            


pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_apply_deltas', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- Set the volcker_reportable_flag for each record ('N' by default, 'Y' if different than null, 00_EXCLUDE and 00_NO_MATCH)
update bh_staging
set volcker_reportable_flag = (
  case when volcker_trading_desk is not null and upper(volcker_trading_desk) not in ('00_EXCLUDE','00_NO_MATCH') then 
          'Y' else 'N' end);
          
commit;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_apply_deltas', 'Step 5 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

dbms_output.put_line('Finished p_brds_etl_apply_deltas');


end p_brds_etl_apply_deltas;


-- ********************************************************************** 
-- Procedure: p_brds_etl_manual_conflicts
-- ********************************************************************** 

procedure p_brds_etl_manual_conflicts
as
begin 

dbms_output.put_line('Running p_brds_etl_manual_conflicts');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 1 Start', 'bRDS');


-- ************************************************************************ 
-- Main processing for conflicts between bRDS and Manual entries: 




-- ************************************************************************ 
-- 1. Update all bRDS items to active at the start of the Conflicts Resolutions (CR) process: 

update  bh_staging  s
set     s.active_flag = 'Y'
where   s.data_source = 'bRDS';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 1: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- 2 Update all Manual items to active
-- 2.1 where there is no match to a bRDS (book_id/gtbId does not exist in bRDS entries): 


update  bh_staging  s
set     s.active_flag = 'Y'
where   upper(nvl(s.data_source, 'MANUAL')) = 'MANUAL'
and     NOT exists ( 
  select  *
  from    bh_staging s2
  where   s2.data_source = 'bRDS'
  and     s2.book_id = s.book_id );
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 2.1.1: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  bh_staging  s
set     s.active_flag = 'Y'
where   upper(nvl(s.data_source, 'MANUAL')) = 'MANUAL'
and     s.global_trader_book_id is not null
and     NOT exists ( 
  select  *
  from    bh_staging s2
  where   s2.data_source = 'bRDS'
  and     s2.global_trader_book_id = s.global_trader_book_id );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 2.1.2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- 2.2 where the source_system is set: 


update  bh_staging  s
set     s.active_flag = 'Y'
where   upper(nvl(s.data_source, 'MANUAL')) = 'MANUAL'
and     s.source_system is NOT NULL; 
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 2.2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- 3. Update CFBU values on Manuals from corresponding bRDS entries: Otherwise we will have no matches: 


-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning



-- ************************************************************************ 
-- 4. Partial match conflict: bRDS entry conflicts with Manual entry: Manual entry is always active: 

update  bh_staging  s
set     s.active_flag = 'N'
where   s.data_source = 'bRDS'
and     exists ( 
  select  1
  from    bh_staging s2
  where   upper(nvl(s2.data_source, 'MANUAL')) = 'MANUAL'
  and     s2.active_flag = 'Y'
  and     (( s2.book_id = s.book_id ) or ( s2.global_trader_book_id = s.global_trader_book_id ))
  and     s2.source_system is NULL
  and     nvl(s2.volcker_trading_desk, ' ') != nvl(s.volcker_trading_desk, ' ')
);
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- 5. Full match conflict: bRDS entry supercedes Manual entry: 

-- Matching criteria: 
--book_id
--volcker_trading_desk
--charge_reporting_unit (no longer)
--charge_reporting_parent (no longer)
--covered_fund_bus_unit_rpl_code (no longer)

update  bh_staging  s
set     s.active_flag = 'N'
where   upper(nvl(s.data_source, 'MANUAL')) = 'MANUAL'
and     exists ( 
  select  * 
  from    bh_staging s2
  where   s2.data_source = 'bRDS'
  and     s2.active_flag = 'Y'
  and     s2.book_id = s.book_id 
  and     nvl(s2.volcker_trading_desk, ' ') = nvl(s.volcker_trading_desk, ' '));


pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 5.1: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- ************************************************************************ 
-- Match on global_trader_book_id: 


update  bh_staging  s
set     s.active_flag = 'N'
where   upper(nvl(s.data_source, 'MANUAL')) = 'MANUAL'
and     exists ( 
  select  *
  from    bh_staging s2
  where   s2.data_source = 'bRDS'
  and     s2.active_flag = 'Y'
  and     s2.global_trader_book_id = s.global_trader_book_id
  and     nvl(s2.volcker_trading_desk, ' ') = nvl(s.volcker_trading_desk, ' '));
  
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 5.2: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

-- 6. Delete all existing PENDING conflicts: 
delete from bh_conflicts where status = 'PENDING';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 6: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- ************************************************************************ 
-- 7. All Partial matches need to be inserted to bh_conflicts for review: 

-- Insert the latest conflicts: 
insert into bh_conflicts (
        id, 
        status, 
        asofdate, 
        book_id, 
        volcker_trading_desk, 
        created_on, 
        resolved_by, 
        resolved_on, 
        global_trader_book_id, 
        source_system, 
        charge_reporting_unit_code, 
        charge_reporting_parent_code, 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        business, 
        sub_business, 
        comments, 
        bh_intermediary_id
        )
select  seq_bh_conflicts.nextval          id, 
        c.*
from    (
select  distinct 
        'PENDING'                         status, 
        b2.asofdate, 
        b1.book_id, 
        b1.volcker_trading_desk, 
        SYSDATE                           created_on,
        NULL                              resolved_by, 
        NULL                              resolved_on, 
        b1.global_trader_book_id, 
        b1.source_system, 
        b1.charge_reporting_unit_code, 
        b1.charge_reporting_parent_code, 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        b1.business, 
        b1.sub_business, 
        NULL                              comments, 
        nvl(b2.bh_intermediary_id, 0)     bh_intermediary_id
from    bh_staging  b1, 
        bh_staging  b2
where   upper(b1.data_source) = 'BRDS'
and     b1.active_flag = 'N'
and     upper(nvl(b2.data_source, 'MANUAL')) = 'MANUAL' 
and     b2.active_flag = 'Y'
and     b1.book_id = b2.book_id
and     b2.source_system is NULL 
and     nvl(b1.volcker_trading_desk, ' ') != nvl(b2.volcker_trading_desk, ' ')
--avoid to create conflict again if latest conflict about this book was not rejected (bRDS was decided to activate)
and     not exists (
          select 1
            from bh_conflicts c
           where b1.global_trader_book_id = c.global_trader_book_id
             and nvl(b1.volcker_trading_desk,' ') = nvl(c.volcker_trading_desk,' ')
             and c.status = 'REJECTED'
             and c.id in (
              select max(c2.id)
                from bh_conflicts c2
               where c.global_trader_book_id=c2.global_trader_book_id
                 and c.bh_intermediary_id = c2.bh_intermediary_id 
              )
        )
union
select  distinct 
        'PENDING'                         status, 
        b2.asofdate, 
        b1.book_id, 
        b1.volcker_trading_desk, 
        SYSDATE                           created_on,
        NULL                              resolved_by, 
        NULL                              resolved_on, 
        b1.global_trader_book_id, 
        b1.source_system, 
        b1.charge_reporting_unit_code, 
        b1.charge_reporting_parent_code, 
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        b1.business, 
        b1.sub_business, 
        NULL                              comments, 
        nvl(b2.bh_intermediary_id, 0)     bh_intermediary_id
from    bh_staging  b1, 
        bh_staging  b2
where   upper(b1.data_source) = 'BRDS'
and     b1.active_flag = 'N'
and     upper(nvl(b2.data_source, 'MANUAL')) = 'MANUAL' 
and     b2.active_flag = 'Y'
and     b1.global_trader_book_id = b2.global_trader_book_id
and     b2.source_system is NULL 
and     nvl(b1.volcker_trading_desk, ' ') != nvl(b2.volcker_trading_desk, ' ')
--avoid to create conflict again if latest conflict about this book was not rejected (bRDS was decided to activate)
and     not exists (
          select 1
            from bh_conflicts c
           where b1.global_trader_book_id = c.global_trader_book_id
             and nvl(b1.volcker_trading_desk,' ') = nvl(c.volcker_trading_desk,' ')
             and c.status = 'REJECTED'
             and c.id in (
              select max(c2.id)
                from bh_conflicts c2
               where c.global_trader_book_id=c2.global_trader_book_id
                 and c.bh_intermediary_id = c2.bh_intermediary_id 
              )
        )
 ) c;


pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_manual_conflicts', 'Step 7 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

dbms_output.put_line('Finished p_brds_etl_manual_conflicts');


end p_brds_etl_manual_conflicts;


-- ********************************************************************** 
-- Procedure: p_brds_etl_load_rpl
-- ********************************************************************** 

procedure p_brds_etl_load_rpl ( pMode int DEFAULT 0 )
as
v_run_date varchar2(20);
begin 

dbms_output.put_line('Running p_brds_etl_load_rpl');

if ( pMode = 1 ) then
if (PKG_BH_COMMONS.F_IS_BRDS_INTEGRATION_ACTIVE = TRUE) then

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_rpl', 'Step 1 Start', 'bRDS');
select distinct asofdate into v_run_date from bh_staging;


-- ************************************************************************ 
-- Final load of active entries in bh_staging into table book_hierarchy_rpl

  -- Clear down entries for the first of next month: 

  delete
  from    book_hierarchy_rpl
  where   asofdate = v_run_date;
  --and     data_source = 'bRDS';
  
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_rpl', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');
  
  commit;
  
  
  -- Only insert active entries from bh_staging into book_hierarchy_rpl
  insert into book_hierarchy_rpl
  (       asofdate, 
          book_id, 
          volcker_trading_desk, 
          volcker_trading_desk_full, 
          lowest_level_rpl_code, 
          lowest_level_rpl_full_name, 
          lowest_level_rpl, 
          source_system, 
          legal_entity, 
          global_trader_book_id, 
          profit_center_id, 
          comments, 
          data_source, 
          create_date, 
          last_modified_date, 
          charge_reporting_unit_code, 
          charge_reporting_unit, 
          charge_reporting_parent_code, 
          charge_reporting_parent, 
          mi_location, 
          ubr_level_1_id, 
          ubr_level_1_name, 
          ubr_level_1_rpl_code, 
          ubr_level_2_id, 
          ubr_level_2_name, 
          ubr_level_2_rpl_code, 
          ubr_level_3_id, 
          ubr_level_3_name, 
          ubr_level_3_rpl_code, 
          ubr_level_4_id, 
          ubr_level_4_name, 
          ubr_level_4_rpl_code, 
          ubr_level_5_id, 
          ubr_level_5_name, 
          ubr_level_5_rpl_code, 
          ubr_level_6_id, 
          ubr_level_6_name, 
          ubr_level_6_rpl_code, 
          ubr_level_7_id, 
          ubr_level_7_name, 
          ubr_level_7_rpl_code, 
          ubr_level_8_id, 
          ubr_level_8_name, 
          ubr_level_8_rpl_code, 
          ubr_level_9_id, 
          ubr_level_9_name, 
          ubr_level_9_rpl_code, 
          ubr_level_10_id, 
          ubr_level_10_name, 
          ubr_level_10_rpl_code, 
          ubr_level_11_id, 
          ubr_level_11_name, 
          ubr_level_11_rpl_code, 
          ubr_level_12_id, 
          ubr_level_12_name, 
          ubr_level_12_rpl_code, 
          ubr_level_13_id, 
          ubr_level_13_name, 
          ubr_level_13_rpl_code, 
          ubr_level_14_id, 
          ubr_level_14_name, 
          ubr_level_14_rpl_code, 
          desk_level_1_id, 
          desk_level_1_name, 
          desk_level_1_rpl_code, 
          desk_level_2_id, 
          desk_level_2_name, 
          desk_level_2_rpl_code, 
          desk_level_3_id, 
          desk_level_3_name, 
          desk_level_3_rpl_code, 
          desk_level_4_id, 
          desk_level_4_name, 
          desk_level_4_rpl_code, 
          desk_level_5_id, 
          desk_level_5_name, 
          desk_level_5_rpl_code, 
          portfolio_id, 
          portfolio_name, 
          portfolio_rpl_code, 
          business, 
          sub_business, 
          create_user, 
          last_modification_user, 
          region, 
          subregion, 
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          acc_treat_category,
          primary_trader,
          primary_book_runner,
          primary_fincon,
          primary_moescalation,
          legal_entity_code,
          book_function_code,
          regulatory_reporting_treatment,
          ubr_ma_code,
          hierarchy_ubr_nodename,
          profit_centre_name,
          non_vtd_code,
          non_vtd_name,
          non_vtd_rpl_code,
          non_vtd_exclusion_type,
           VOLCKER_REPORTABLE_FLAG,
	      non_vtd_division,  
	      non_vtd_pvf, 
	      non_vtd_business 
		  )
  select  asofdate, 
          book_id, 
          volcker_trading_desk, 
          volcker_trading_desk_full, 
          lowest_level_rpl_code, 
          lowest_level_rpl_full_name, 
          lowest_level_rpl, 
          source_system, 
          legal_entity, 
          global_trader_book_id, 
          profit_center_id, 
          comments, 
          data_source, 
          create_date, 
          last_modified_date, 
          charge_reporting_unit_code, 
          charge_reporting_unit, 
          charge_reporting_parent_code, 
          charge_reporting_parent, 
          mi_location, 
          ubr_level_1_id, 
          ubr_level_1_name, 
          ubr_level_1_rpl_code, 
          ubr_level_2_id, 
          ubr_level_2_name, 
          ubr_level_2_rpl_code, 
          ubr_level_3_id, 
          ubr_level_3_name, 
          ubr_level_3_rpl_code, 
          ubr_level_4_id, 
          ubr_level_4_name, 
          ubr_level_4_rpl_code, 
          ubr_level_5_id, 
          ubr_level_5_name, 
          ubr_level_5_rpl_code, 
          ubr_level_6_id, 
          ubr_level_6_name, 
          ubr_level_6_rpl_code, 
          ubr_level_7_id, 
          ubr_level_7_name, 
          ubr_level_7_rpl_code, 
          ubr_level_8_id, 
          ubr_level_8_name, 
          ubr_level_8_rpl_code, 
          ubr_level_9_id, 
          ubr_level_9_name, 
          ubr_level_9_rpl_code, 
          ubr_level_10_id, 
          ubr_level_10_name, 
          ubr_level_10_rpl_code, 
          ubr_level_11_id, 
          ubr_level_11_name, 
          ubr_level_11_rpl_code, 
          ubr_level_12_id, 
          ubr_level_12_name, 
          ubr_level_12_rpl_code, 
          ubr_level_13_id, 
          ubr_level_13_name, 
          ubr_level_13_rpl_code,
          ubr_level_14_id,
          ubr_level_14_name,
          ubr_level_14_rpl_code,
          desk_level_1_id,
          desk_level_1_name,
          desk_level_1_rpl_code,
          desk_level_2_id,
          desk_level_2_name,
          desk_level_2_rpl_code,
          desk_level_3_id,
          desk_level_3_name,
          desk_level_3_rpl_code,
          desk_level_4_id,
          desk_level_4_name,
          desk_level_4_rpl_code,
          desk_level_5_id,
          desk_level_5_name,
          desk_level_5_rpl_code,
          portfolio_id,
          portfolio_name,
          portfolio_rpl_code,
          business,
          sub_business,
          create_user,
          last_modification_user,
          region,
          subregion,
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          acc_treat_category,
          primary_trader,
          primary_book_runner,
          primary_fincon,
          primary_moescalation,
          legal_entity_code,
          book_function_code,
          regulatory_reporting_treatment,
          ubr_ma_code,
          hierarchy_ubr_nodename,
          profit_centre_name,
          non_vtd_code,
          non_vtd_name,
          non_vtd_rpl_code,
          non_vtd_exclusion_type,
           VOLCKER_REPORTABLE_FLAG,
	       non_vtd_division,  
	       non_vtd_pvf, 
	       non_vtd_business 
  from    bh_staging
  where   active_flag = 'Y'
  and     asofdate = v_run_date
  and     ( data_source = 'bRDS'  or ( upper(nvl(data_source, 'MANUAL')) = 'MANUAL' /*and nvl(rpl_load, 'N') != 'Y'*/ ));

  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_rpl', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

  commit;


  -- Flag all Manual entries as loaded so they do not get loaded again if the RPL load process is run more than once in a month: 
  
  update  bh_staging
  set     rpl_load = 'Y'
  where   upper(nvl(data_source, 'MANUAL')) = 'MANUAL'
  and     asofdate = v_run_date
  and     nvl(rpl_load, 'N') != 'Y';

  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_rpl', 'Step 4 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

  commit;

else
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','WARN', 'LOGGING', 'p_brds_etl_load_rpl', 'Data not loaded because BRDS Integration is OFF', 'bRDS');
end if;
end if;

dbms_output.put_line('Finished p_brds_etl_load_rpl');
exception
when others then
  dbms_output.put_line('exception p_brds_etl_load_rpl: '||SQLERRM);
  raise;

end p_brds_etl_load_rpl;




-- ********************************************************************** 
-- Procedure: p_brds_etl
-- ********************************************************************** 

procedure p_brds_etl ( pRunId IN int, pMode IN int DEFAULT 0 )
as
v_run_date              varchar2(20);
c_error_log             varchar2(4000);

begin

dbms_output.put_line('Running p_brds_etl!');
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 1 Start', 'bRDS');



select to_char(last_day(current_date) + 1, 'DD-MON-YYYY') into v_run_date from dual;

-- ************************************************************************ 
-- Main ETL processing: 

-- ************************************************************************ 
-- 1. Clear down Stage and intermediary tables: 


execute immediate 'truncate table bh_staging_intermed'; 
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 2: '||to_char(SQL%ROWCOUNT), 'bRDS');

execute immediate 'truncate table bh_ubr_desk_hierarchy';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 3: '||to_char(SQL%ROWCOUNT), 'bRDS');

execute immediate 'truncate table bh_workflow';
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 4: '||to_char(SQL%ROWCOUNT), 'bRDS');

-- ************************************************************************ 
-- Update AsOfDate to current working date if required for ALL entries



update  bh_staging
set     asofdate = v_run_date
where   asofdate != v_run_date;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 5: '||to_char(SQL%ROWCOUNT), 'bRDS');



-- ************************************************************************ 
-- Update data_source to "MANUAL" for anything other than "bRDS" entries: 


update    bh_staging
set       data_source = 'MANUAL'
where     nvl(data_source, ' ') NOT in ( 'bRDS', 'MANUAL' );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 6: '||to_char(SQL%ROWCOUNT), 'bRDS');



-- ************************************************************************ 
-- Data clear up: Reset value "null" to actual NULL on VTD/CRU/CRP/CFBU
-- This is to clear up an issue with the java process that currently has problems with Null values

update  brds_vw_book
set     volckerTradingDesk = NULL
where   volckerTradingDesk = 'null';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 7: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  brds_vw_book
set     chargeReportingUnitCode = NULL
where   chargeReportingUnitCode = 'null';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 8: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;

update  brds_vw_book
set     chargeReportingParentCode = NULL
where   chargeReportingParentCode = 'null';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 9: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning



-- ************************************************************************ 
-- Cleaning duplicate records from landing tables
pkg_brds_bh_rpl_dups.p_brds_etl_remove_duplicates;

-- ************************************************************************ 
-- Validation on bRDS view data: 

-- 1. book_id must not be greater than 50 chars: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_book_id_length;

-- 2. Duplicate global_trader_book_id: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_dup_gtbid;

-- 3. True Duplicates: Duplicate book_id with different volcker_trading_desk/charge_reporting_unit/charge_reporting_parent: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_dups;

-- 4. Duplicate book_ids: differences in globaltraderBookId

pkg_brds_bh_rpl_wf.p_brds_etl_val_dup_book_id;

-- 5. bRDS book records with no matching entry in hierarchy view: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_no_hierarchy;

-- 6. Duplicated nodeId (globalTraderBookId) and rplCode: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_dup_node_rpl;

-- 7. Null VTD, CRU, CRP, CFBU, Non-book dup rplCode nodes: 
pkg_brds_bh_rpl_wf.p_brds_etl_val_null_vtd;

--Build data in bh_ubr_desk_hierarchy table (only insert) for next validations
pkg_brds_bh_rpl_wf.p_brds_etl_init_hierarchy;

pkg_brds_bh_rpl_wf.p_brds_etl_val_dup_node_rpl_nb;

-- 8. Invalid VolckerTradingDesk: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_vtd;


-- 9. Invalid chargeReportingUnit: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_cru;


-- 10. Invalid chargeReportingParent: 

pkg_brds_bh_rpl_wf.p_brds_etl_val_crp;


-- 11. Invalid coveredFundBusinessUnit:

-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning


-- ************************************************************************ 
-- Insert core data into Pre-Stage table: 

p_brds_etl_load_core_data;



-- ************************************************************************ 
-- UBR/DESK level processing start: 

-- ************************************************************************ 
-- Build hierarchy table, bh_ubr_desk_hierarchy (delete + insert): 

p_brds_etl_build_hierarchy;


-- ************************************************************************ 
-- Set Ubr and Desk hierarchy fields from bh_ubr_desk_hierarchy: 

p_brds_etl_set_hierarchy;


-- ************************************************************************ 
-- Update core data in Pre-Stage table: 

p_brds_etl_update_core_data;

-- ************************************************************************ 
-- Update NON-VTD Data

p_brds_etl_non_vtd;

-- ************************************************************************ 
-- Update any new or changed items in bh_staging from bh_staging_intermed. 
-- Similarly remove any books on bh_staging which are not in bh_staging_intermed


p_brds_etl_apply_deltas;




-- ************************************************************************ 
-- bRDS vs Manual conflicts: 


-- ************************************************************************ 
-- Workflow items: 

-- 1. Manual Overrides entered from the UI: 
-- 2. bRDS overriding previous manual override records (full match): 
-- 3. bRDS overriding previous manual override records (global_trader_book_id match): 
-- 4. bRDS overrides/conflicts with Manual entry (partial match)

pkg_brds_bh_rpl_wf.p_brds_etl_workflow_items;


-- ************************************************************************ 
-- Main processing for conflicts between bRDS and Manual entries: 

-- 1. Update all bRDS items to active where there is no match to a Manual (book_id does not exist in Manual entries): 
-- 2. Full match conflict: bRDS entry supercedes Manual entry: 
-- Match on global_trader_book_id: 
-- 3. Full match conflict: Manual entry supercedes bRDS entry: 
-- Match on global_trader_book_id: 
-- 4. Partial match conflict: bRDS entry conflicts with Manual entry: Manual entry is always active: 

p_brds_etl_manual_conflicts;



-- ************************************************************************ 
-- Final load of active entries in bh_staging into table book_hierarchy_rpl

p_brds_etl_load_rpl(pMode);


--  ************************************************************************ 
-- Process that identifies the new books created in bRDS intra-month (not modifications for previously existing books),
-- and  update the current month�s BOOK_HIERARCHY_RPL table adding just those new books only if the VTD exists in that current month
p_brds_etl_load_rpl_new_books;
-- ************************************************************************ 
-- Final update of status table: 

update  brds_vw_status
set     endTime = current_timestamp,
        result  = 'OK'
where   runId = pRunId;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl', 'Step 10 End: '||to_char(SQL%ROWCOUNT), 'bRDS');

commit;


dbms_output.put_line('Finished p_brds_etl!');

exception
when others then
  rollback;
  c_error_log := SQLERRM;
  update  brds_vw_status
  set     endTime = current_timestamp,
          result  = 'ERROR',
          error_message = SUBSTR(c_error_log, 1, 2500)
  where   runId = pRunId;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','ERROR', 'LOGGING', 'p_brds_etl', SUBSTR(SQLERRM, 1, 2500), 'bRDS');
  commit;
  raise;

end p_brds_etl;

-- ****************************************************************************
-- Process to return the number of proccesses validating or approving currently
-- ****************************************************************************
procedure p_brds_etl_check_ui_status ( p_result out int )
is
begin
  select nvl(count(*), 0) into p_result
    from ref_data_ui_upload
   where status_id in (11, 12);
end p_brds_etl_check_ui_status;


-- *********************************************************************************************
-- Process to return the counts of the books needed to raise alerts to recon (when it is needed)
-- *********************************************************************************************
procedure p_brds_etl_recon_alerts ( p_threshold out number, p_book_count_landing out number, p_book_count_out_of_scope out number, p_book_count_presubmission out number )
is
begin
  --Get threshold
  begin
    select to_number(param_value) into p_threshold
      from master_param
     where metric_id='BRDS' 
       and param_group = 'RECON_ALERTS'
       and param_key = 'THRESHOLD';
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','ERROR', 'LOGGING', 'p_brds_etl_recon_alerts', 'Step 1: '||p_threshold, 'bRDS');
  exception
    when others then
      pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','ERROR', 'LOGGING', 'p_brds_etl_recon_alerts', SUBSTR(SQLERRM, 1, 2500), 'bRDS');
      p_threshold := 0.1; --Threshold default value
  end;
  
  --Get book count landing
  select count(distinct globaltraderbookid) into p_book_count_landing
   from brds_vw_book;
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','ERROR', 'LOGGING', 'p_brds_etl_recon_alerts', 'Step 2: '||p_book_count_landing, 'bRDS');
  
  --Get book count out of scope
  select count(distinct global_trader_book_id) into p_book_count_out_of_scope
   from bh_workflow
   where workflow_type_id in ('1','2','3','4','5','7','26','27','28','29' );
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','ERROR', 'LOGGING', 'p_brds_etl_recon_alerts', 'Step 3: '||p_book_count_out_of_scope, 'bRDS');
  
  --Get book count presubmission
  select count(distinct global_trader_book_id) into p_book_count_presubmission
   from bh_staging where data_source = 'bRDS';
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','ERROR', 'LOGGING', 'p_brds_etl_recon_alerts', 'Step 4 End: '||p_book_count_presubmission, 'bRDS');
end p_brds_etl_recon_alerts;


-- ********************************************************************** 
-- Procedure: p_brds_etl_load_rpl_new_books
-- ********************************************************************** 

procedure p_brds_etl_load_rpl_new_books 
as
v_next_date varchar2(20);
v_prev_date varchar2(20);
begin 

dbms_output.put_line('Running p_brds_etl_load_rpl_new_books');

select to_char(last_day(current_date) + 1, 'DD-MON-YY') into v_next_date from dual;
select to_char(trunc(current_date,'MM'), 'DD-MON-YY') into v_prev_date from dual;


  -- Only insert new entries from bh_staging into book_hierarchy_rpl
  insert into book_hierarchy_rpl
  (       asofdate, 
          book_id, 
          volcker_trading_desk, 
          volcker_trading_desk_full, 
          lowest_level_rpl_code, 
          lowest_level_rpl_full_name, 
          lowest_level_rpl, 
          source_system, 
          legal_entity, 
          global_trader_book_id, 
          profit_center_id, 
          comments, 
          data_source, 
          create_date, 
          last_modified_date, 
          charge_reporting_unit_code, 
          charge_reporting_unit, 
          charge_reporting_parent_code, 
          charge_reporting_parent, 
          mi_location, 
          ubr_level_1_id, 
          ubr_level_1_name, 
          ubr_level_1_rpl_code, 
          ubr_level_2_id, 
          ubr_level_2_name, 
          ubr_level_2_rpl_code, 
          ubr_level_3_id, 
          ubr_level_3_name, 
          ubr_level_3_rpl_code, 
          ubr_level_4_id, 
          ubr_level_4_name, 
          ubr_level_4_rpl_code, 
          ubr_level_5_id, 
          ubr_level_5_name, 
          ubr_level_5_rpl_code, 
          ubr_level_6_id, 
          ubr_level_6_name, 
          ubr_level_6_rpl_code, 
          ubr_level_7_id, 
          ubr_level_7_name, 
          ubr_level_7_rpl_code, 
          ubr_level_8_id, 
          ubr_level_8_name, 
          ubr_level_8_rpl_code, 
          ubr_level_9_id, 
          ubr_level_9_name, 
          ubr_level_9_rpl_code, 
          ubr_level_10_id, 
          ubr_level_10_name, 
          ubr_level_10_rpl_code, 
          ubr_level_11_id, 
          ubr_level_11_name, 
          ubr_level_11_rpl_code, 
          ubr_level_12_id, 
          ubr_level_12_name, 
          ubr_level_12_rpl_code, 
          ubr_level_13_id, 
          ubr_level_13_name, 
          ubr_level_13_rpl_code, 
          ubr_level_14_id, 
          ubr_level_14_name, 
          ubr_level_14_rpl_code, 
          desk_level_1_id, 
          desk_level_1_name, 
          desk_level_1_rpl_code, 
          desk_level_2_id, 
          desk_level_2_name, 
          desk_level_2_rpl_code, 
          desk_level_3_id, 
          desk_level_3_name, 
          desk_level_3_rpl_code, 
          desk_level_4_id, 
          desk_level_4_name, 
          desk_level_4_rpl_code, 
          desk_level_5_id, 
          desk_level_5_name, 
          desk_level_5_rpl_code, 
          portfolio_id, 
          portfolio_name, 
          portfolio_rpl_code, 
          business, 
          sub_business, 
          create_user, 
          last_modification_user, 
          region, 
          subregion, 
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          acc_treat_category,
          primary_trader,
          primary_book_runner,
          primary_fincon,
          primary_moescalation,
          legal_entity_code,
          book_function_code,
          regulatory_reporting_treatment,
          ubr_ma_code,
          hierarchy_ubr_nodename,
          profit_centre_name,
          non_vtd_code,
          non_vtd_name,
          non_vtd_rpl_code,
          non_vtd_exclusion_type,
          volcker_reportable_flag,
	      non_vtd_division,  
	      non_vtd_pvf, 
	      non_vtd_business 
		  )
  select 
    to_char(trunc(current_date,'MM'), 'DD-MON-YY'), 
          book_id, 
          volcker_trading_desk, 
          volcker_trading_desk_full, 
          lowest_level_rpl_code, 
          lowest_level_rpl_full_name, 
          lowest_level_rpl, 
          source_system, 
          legal_entity, 
          global_trader_book_id, 
          profit_center_id, 
          comments, 
          data_source, 
          create_date, 
          last_modified_date, 
          charge_reporting_unit_code, 
          charge_reporting_unit, 
          charge_reporting_parent_code, 
          charge_reporting_parent, 
          mi_location, 
          ubr_level_1_id, 
          ubr_level_1_name, 
          ubr_level_1_rpl_code, 
          ubr_level_2_id, 
          ubr_level_2_name, 
          ubr_level_2_rpl_code, 
          ubr_level_3_id, 
          ubr_level_3_name, 
          ubr_level_3_rpl_code, 
          ubr_level_4_id, 
          ubr_level_4_name, 
          ubr_level_4_rpl_code, 
          ubr_level_5_id, 
          ubr_level_5_name, 
          ubr_level_5_rpl_code, 
          ubr_level_6_id, 
          ubr_level_6_name, 
          ubr_level_6_rpl_code, 
          ubr_level_7_id, 
          ubr_level_7_name, 
          ubr_level_7_rpl_code, 
          ubr_level_8_id, 
          ubr_level_8_name, 
          ubr_level_8_rpl_code, 
          ubr_level_9_id, 
          ubr_level_9_name, 
          ubr_level_9_rpl_code, 
          ubr_level_10_id, 
          ubr_level_10_name, 
          ubr_level_10_rpl_code, 
          ubr_level_11_id, 
          ubr_level_11_name, 
          ubr_level_11_rpl_code, 
          ubr_level_12_id, 
          ubr_level_12_name, 
          ubr_level_12_rpl_code, 
          ubr_level_13_id, 
          ubr_level_13_name, 
          ubr_level_13_rpl_code,
          ubr_level_14_id,
          ubr_level_14_name,
          ubr_level_14_rpl_code,
          desk_level_1_id,
          desk_level_1_name,
          desk_level_1_rpl_code,
          desk_level_2_id,
          desk_level_2_name,
          desk_level_2_rpl_code,
          desk_level_3_id,
          desk_level_3_name,
          desk_level_3_rpl_code,
          desk_level_4_id,
          desk_level_4_name,
          desk_level_4_rpl_code,
          desk_level_5_id,
          desk_level_5_name,
          desk_level_5_rpl_code,
          portfolio_id,
          portfolio_name,
          portfolio_rpl_code,
          business,
          sub_business,
          create_user,
          last_modification_user,
          region,
          subregion,
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          acc_treat_category,
          primary_trader,
          primary_book_runner,
          primary_fincon,
          primary_moescalation,
          legal_entity_code,
          book_function_code,
          regulatory_reporting_treatment,
          ubr_ma_code,
          hierarchy_ubr_nodename,
          profit_centre_name,
          non_vtd_code,
          non_vtd_name,
          non_vtd_rpl_code,
          non_vtd_exclusion_type,
          volcker_reportable_flag,
	      non_vtd_division,  
	      non_vtd_pvf, 
	      non_vtd_business 
  from    bh_staging s
  where   s.active_flag = 'Y'
  and     s.data_source = 'bRDS'
  and     s.asofdate = v_next_date
  and     s.global_trader_book_id not in (select distinct h1.global_trader_book_id 
										from book_hierarchy_rpl h1
										where h1.asofdate = v_prev_date AND upper(h1.DATA_SOURCE) = 'BRDS' and h1.global_trader_book_id is not null) 
  and     s.volcker_trading_desk in (select distinct volcker_trading_desk 
									from book_hierarchy_rpl 
									where asofdate = v_prev_date AND DATA_SOURCE = 'bRDS')
  and not exists (select 1 from book_hierarchy_rpl h where  asofdate = v_prev_date and h.book_id = s.book_id and h.source_system is null)
  ;


  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'bRDS ETL','DEBUG', 'LOGGING', 'p_brds_etl_load_rpl_new_books', 'Step 1 END: '||to_char(SQL%ROWCOUNT), 'bRDS');

  commit;



  
dbms_output.put_line('Finished p_brds_etl_load_rpl_new_books');
exception
when others then
  rollback;
  dbms_output.put_line('exception p_brds_etl_load_rpl_new_books: '||SQLERRM);
  raise;

end p_brds_etl_load_rpl_new_books;




END PKG_BRDS_BH_RPL;
