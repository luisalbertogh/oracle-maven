--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_PR_ETL runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_PR_ETL" as 



-- ********************************************************************** 
-- Procedure: p_prds_init
-- ********************************************************************** 

procedure p_prds_init ( pRunId OUT int ) 
as
debug_flag              integer := 0;
begin 

if ( debug_flag != 0 ) then 
  dbms_output.put_line('Running p_prds_init');
end if; 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_init', 'Step 1 Start', 'pRDS');


select seq_pr_status.NEXTVAL into pRunId from dual;

-- Delete previous entry
delete from pr_status;
-- Insert a new entry into the Status table for this run: 
insert into pr_status 
( runId, startTime)
values ( pRunId, current_timestamp ); 

commit;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_init', 'Step 2 End', 'pRDS');

if ( debug_flag != 0 ) then 
  dbms_output.put_line('Finished p_prds_init');
end if; 

end p_prds_init;





-- ********************************************************************** 
-- Procedure: p_prds_etl_exceptions
-- ********************************************************************** 

procedure p_prds_etl_exceptions 
as
v_run_date              varchar2(20);
debug_flag              integer := 0;
begin 

if ( debug_flag != 0 ) then 
  dbms_output.put_line('Running p_prds_etl_exceptions');
end if; 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_exceptions', 'Step 1 Start', 'pRDS');


select to_char(sysdate, 'DD-MON-YYYY') into v_run_date from dual;


-- ************************************************************************ 
-- EXCEPTION TYPE 1: Duplicate product_uid, same status: 


insert into pr_etl_exceptions (
        id, 
        asofdate, 
        product_uid, 
        product_name, 
        exception_type_id, 
        description, 
        skip )
select  seq_pr_etl_exceptions.nextval id, 
        v_run_date asofdate, 
        p1.product_uid, 
        p1.product_name, 
        1 exception_type_id, 
        'Duplicated products by product UID having same status' description, 
        'N' skip
from    prds_products p1
where   ( p1.product_uid, p1.status ) in ( 
  select  p2.product_uid, 
          p2.status
  from    prds_products p2
  group by p2.product_uid, p2.status
  having count(*) > 1 );

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_exceptions', 'Step 2', 'pRDS');

commit; 



-- ************************************************************************ 
-- EXCEPTION TYPE 2: Duplicate product_uid, different status: 


insert into pr_etl_exceptions (
        id, 
        asofdate, 
        product_uid, 
        product_name, 
        exception_type_id, 
        description, 
        skip )
select  seq_pr_etl_exceptions.nextval id, 
        v_run_date asofdate, 
        p1.product_uid, 
        p1.product_name, 
        2 exception_type_id, 
        'Duplicated products by Product UID having different status' description, 
        'N' skip
from    prds_products p1
where   ( p1.product_uid ) in ( 
  select  p2.product_uid
  from    prds_products p2
  group by p2.product_uid
  having count(distinct p2.status) > 1 ); 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_exceptions', 'Step 3', 'pRDS');

commit; 



-- ************************************************************************ 
-- EXCEPTION TYPE 3: Deactivated products: 


insert into pr_etl_exceptions (
        id, 
        asofdate, 
        product_uid, 
        product_name, 
        exception_type_id, 
        description, 
        skip )
select  seq_pr_etl_exceptions.nextval id, 
        v_run_date asofdate, 
        p1.product_uid, 
        p1.product_name, 
        3 exception_type_id, 
        'Products with status DEACTIVATED' description, 
        'N' skip
from    prds_products p1
where   upper(p1.status) = 'DEACTIVATED'; 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_exceptions', 'Step 4 End', 'pRDS');

commit; 


if ( debug_flag != 0 ) then 
  dbms_output.put_line('Finished p_prds_etl_exceptions');
end if; 

end p_prds_etl_exceptions;






-- ********************************************************************** 
-- Procedure: p_prds_etl_load_core_data
-- ********************************************************************** 

procedure p_prds_etl_load_core_data
as
v_run_date              varchar2(20);
debug_flag              integer := 0;
begin 

if ( debug_flag != 0 ) then 
  dbms_output.put_line('Running p_prds_etl_load_core_data');
end if; 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_load_core_data', 'Step 1 Start', 'pRDS');


select to_char(sysdate, 'DD-MON-YYYY') into v_run_date from dual;


-- ************************************************************************ 
-- Insert core data into Pre-Stage table from prds_products: 



insert into pr_staging_intermediary ( 
        id, 
        asofdate, 
        product_uid, 
        product_name, 
        product_long_name, 
        product_description, 
        product_type, 
        recertification_status, 
        last_recertification_date, 
        valid_start_date, 
        status, 
        last_changed_date )
select  seq_pr_staging_intermediary.nextval id,
        t.*
from    (
select  distinct 
        v_run_date, 
        p.product_uid, 
        p.product_name, 
        p.product_long_name, 
        p.product_description, 
        p.product_type, 
        p.recertification_status, 
        to_date(p.last_recertification_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') last_recertification_date, 
        to_date(p.valid_start_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') valid_start_date, 
        p.status, 
        to_date(p.last_changed_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') last_changed_date        
from    prds_products                 p
order by p.product_uid  ASC) t;  

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_load_core_data', 'Step 2: '||to_char(SQL%ROWCOUNT), 'pRDS');

commit;



-- ************************************************************************ 
-- Update from prds_product_details: 


update    pr_staging_intermediary i
set     ( i.system_name, 
          i.volcker_reportable_flag ) = (   
  select  distinct 
          d.system_name, 
          d.attribution_value
  from    prds_product_details  d
  where   d.product_uid = i.product_uid )
where     i.asofdate = v_run_date;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_load_core_data', 'Step 3: '||to_char(SQL%ROWCOUNT), 'pRDS');

commit;




-- ************************************************************************ 
-- Update from prds_product_flat_hierarchy: 


update    pr_staging_intermediary i
set     ( i.hierarchy_name, 
          i.hierarchy_type, 
          i.level_1, 
          i.level_1_hierarchy_code, 
          i.level_2, 
          i.level_2_hierarchy_code, 
          i.level_3, 
          i.level_3_hierarchy_code, 
          i.level_4, 
          i.level_4_hierarchy_code, 
          i.level_5, 
          i.level_5_hierarchy_code ) = ( 
  select  distinct 
          h.hierarchy_name, 
          h.hierarchy_type, 
          h.level_1, 
          h.level_1_hierarchy_code, 
          h.level_2, 
          h.level_2_hierarchy_code, 
          h.level_3, 
          h.level_3_hierarchy_code, 
          h.level_4, 
          h.level_4_hierarchy_code, 
          h.level_5, 
          h.level_5_hierarchy_code 
  from    prds_product_flat_hierarchy   h
  where   h.product_uid = i.product_uid
  -- GBSVR-33189: Start 1: 
  and     ltrim(rtrim(upper(nvl(h.status, '')))) = 'APPROVED'
  -- GBSVR-33189: End 1:
  and     h.product_uid NOT in ( 
    select  h2.product_uid
    from    prds_product_flat_hierarchy h2
    -- GBSVR-33189: Start 2: 
    where   ltrim(rtrim(upper(nvl(h2.status, '')))) = 'APPROVED'
    -- GBSVR-33189: End 2: 
    group by h2.product_uid
    having  count(*) > 1 ))
where     i.asofdate = v_run_date;
  
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_load_core_data', 'Step 4 End: '||to_char(SQL%ROWCOUNT), 'pRDS');

commit;


if ( debug_flag != 0 ) then 
  dbms_output.put_line('Finished p_prds_etl_load_core_data');
end if; 


end p_prds_etl_load_core_data;





-- ********************************************************************** 
-- Procedure: p_prds_etl_insert_staging
-- ********************************************************************** 

procedure p_prds_etl_insert_staging
as
v_run_date              varchar2(20);
debug_flag              integer := 0;
begin 

if ( debug_flag != 0 ) then 
  dbms_output.put_line('Running p_prds_etl_insert_staging');
end if; 
  
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_insert_staging', 'Step 1 Start', 'pRDS');

select to_char(sysdate, 'DD-MON-YYYY') into v_run_date from dual;


-- ************************************************************************ 
-- 1. Remove all rows on Staging for the current sysdate: 


delete 
from    pr_staging
where   asofdate = v_run_date;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_insert_staging', 'Step 2: '||to_char(SQL%ROWCOUNT), 'pRDS');

commit; 



-- ************************************************************************ 
-- 1. Insert all new rows into pr_staging that do not already exist 
--    based on product_uid


insert into pr_staging (
        id, 
        asofdate, 
        product_uid, 
        product_name, 
        product_long_name, 
        product_description, 
        product_type, 
        recertification_status, 
        last_recertification_date, 
        valid_start_date, 
        status, 
        last_changed_date, 
        system_name, 
        volcker_reportable_flag, 
        hierarchy_name, 
        hierarchy_type, 
        level_1, 
        level_1_hierarchy_code, 
        level_2, 
        level_2_hierarchy_code, 
        level_3, 
        level_3_hierarchy_code, 
        level_4, 
        level_4_hierarchy_code, 
        level_5, 
        level_5_hierarchy_code )
select  seq_pr_staging.nextval id, 
        asofdate, 
        product_uid, 
        product_name, 
        product_long_name, 
        product_description, 
        product_type, 
        recertification_status, 
        last_recertification_date, 
        valid_start_date, 
        status, 
        last_changed_date, 
        system_name, 
        volcker_reportable_flag, 
        hierarchy_name, 
        hierarchy_type, 
        level_1, 
        level_1_hierarchy_code, 
        level_2, 
        level_2_hierarchy_code, 
        level_3, 
        level_3_hierarchy_code, 
        level_4, 
        level_4_hierarchy_code, 
        level_5, 
        level_5_hierarchy_code
from    pr_staging_intermediary i
where   i.asofdate = v_run_date;


pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl_insert_staging', 'Step 3 End: '||to_char(SQL%ROWCOUNT), 'pRDS');

commit; 



if ( debug_flag != 0 ) then 
  dbms_output.put_line('Finished p_prds_etl_insert_staging');
end if; 

end p_prds_etl_insert_staging;






-- ********************************************************************** 
-- Procedure: p_prds_etl
-- ********************************************************************** 

procedure p_prds_etl ( pRunId IN int )
as
v_run_date              varchar2(20);
c_error_log             varchar2(4000);
debug_flag              integer := 0;

begin

if ( debug_flag != 0 ) then 
  dbms_output.put_line('Running p_prds_etl!');
end if; 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl', 'Step 1 Start', 'pRDS');


select to_char(sysdate, 'DD-MON-YYYY') into v_run_date from dual;

-- ************************************************************************ 
-- Main ETL processing: 

-- ************************************************************************ 
-- 1. Clear down Stage and intermediary tables: 



execute immediate 'truncate table pr_staging_intermediary'; 

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl', 'Step 2: '||to_char(SQL%ROWCOUNT), 'pRDS');


execute immediate 'truncate table pr_etl_exceptions';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl', 'Step 3: '||to_char(SQL%ROWCOUNT), 'pRDS');




-- ************************************************************************ 
-- Flag exceptions: 

p_prds_etl_exceptions; 




-- ************************************************************************ 
-- Insert core data to to pr_staging_intermediary: 

p_prds_etl_load_core_data; 




-- ************************************************************************ 
-- Insert data from pr_staging_intermediary to pr_staging for sysdate: 


p_prds_etl_insert_staging; 




---- ************************************************************************ 
---- Final update of status table: 
--
update  pr_status
set     endTime = current_timestamp,
        result  = 'OK'
where   runId = pRunId;
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','DEBUG', 'LOGGING', 'p_prds_etl', 'Step 4 End: '||to_char(SQL%ROWCOUNT), 'pRDS');

commit;


if ( debug_flag != 0 ) then 
  dbms_output.put_line('Finished p_prds_etl!');
end if; 

exception
when others then
  rollback;
  c_error_log := SQLERRM;
  update  pr_status
  set     endTime = current_timestamp,
          result  = 'ERROR',
          error_message = SUBSTR(c_error_log, 1, 2500)
  where   runId = pRunId;
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'pRDS',current_date,'pRDS ETL','ERROR', 'LOGGING', 'p_prds_etl', SUBSTR(SQLERRM, 1, 2500), 'pRDS');
  commit;
  raise;

end p_prds_etl;



END pkg_pr_etl;
