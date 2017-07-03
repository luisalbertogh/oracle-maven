--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_HOUSEKEEPING runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_HOUSEKEEPING" AS

    FUNCTION P_ACTION_GROUP (A_GROUP_NAME VARCHAR2) RETURN NUMBER IS
        v_number number(15);
        days number(3):=365;
        end_of_month date;
    BEGIN
      v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS ('HOUSEKEEPING',null,null,sysdate,'HOUSEKEEPING','INIT', 'INFO', 'INIT', 'HOUSEKEEPING STARTS');
     
        for hk_group in (select table_name,partition_case_id,partition_key,partition_value from housekeeping where housekeeping_group=a_group_name order by partition_case_id) loop
            for hk_fk in (select constraint_name, table_name 
                            from user_constraints a 
                           where  a.constraint_type='R' 
                             and exists(select 1 
                                          from user_constraints b 
                                         where b.table_name=hk_group.table_name
                                           and a.r_constraint_name=b.constraint_name)) loop
                    execute immediate 'alter table '||hk_fk.table_name||' disable constraint '||hk_fk.constraint_name;
            end loop;
            for hk_table in (
                                select partition_name,high_value_in_date_format from (select table_name, partition_name, 
                                to_date(replace (regexp_substr (extractvalue (dbms_xmlgen. getxmltype ('select high_value from user_tab_partitions where table_name=''' || table_name || ''' and partition_name = ''' || partition_name || ''''),'//text()'), '''.*?'''),''''),'syyyy-mm-dd hh24:mi:ss') high_value_in_date_format
                                from user_tab_partitions  where interval='YES')
                                where table_name=hk_group.table_name
                                and high_value_in_date_format<=to_date(to_char(sysdate - hk_group.partition_value,'YYYY-MM-DD'),'YYYY-MM-DD')                                
                                ) loop
                               
                                select last_day( to_date(hk_table.high_value_in_date_format-1,'dd/MM/yy'))
                               - decode(to_char(last_day(to_date(hk_table.high_value_in_date_format-1,'dd/MM/yy')), 'd'), '6', 1, '7', 2, 0) 
                               into end_of_month from dual;                               
                                
                               if(to_date(hk_table.high_value_in_date_format,'dd/MM/yy') <> to_date(end_of_month+1,'dd/MM/yy') 
                                ) then   
                          execute immediate 'alter table '||hk_group.table_name||' drop partition '||hk_table.partition_name;
                                 end if;                              
                                
                                if(to_date(hk_table.high_value_in_date_format,'dd/MM/yy') = to_date(end_of_month+1,'dd/MM/yy')
                                and (to_date(hk_table.high_value_in_date_format,'dd/MM/yy') < to_date(to_char(sysdate - days+1,'dd/MM/yy'),'dd/MM/yy')) 
                                ) then   
                          execute immediate 'alter table '||hk_group.table_name||' drop partition '||hk_table.partition_name;
                                 end if;                              
                                   
            end loop;
            for hk_fk in (select constraint_name, table_name 
                            from user_constraints a 
                           where  a.constraint_type='R' 
                             and exists(select 1 
                                          from user_constraints b 
                                         where b.table_name=hk_group.table_name
                                           and a.r_constraint_name=b.constraint_name)) loop
                    execute immediate 'alter table '||hk_fk.table_name||' enable novalidate constraint '||hk_fk.constraint_name;
            end loop;
        end loop;
        for hk_index in (select index_name from user_indexes where status='UNUSABLE') loop
            execute immediate 'alter index '||hk_index.index_name||' rebuild';
        end loop;
        
       v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS ('HOUSEKEEPING',null,null,sysdate,'HOUSEKEEPING','FINISH', 'INFO', 'FINISH', 'HOUSEKEEPING FINISHED');
        
       RETURN  0;
       EXCEPTION
            WHEN OTHERS THEN
            v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS ('HOUSEKEEPING',null,null,sysdate,'HOUSEKEEPING','ERROR', 'FATAL', 'HOUSEKEEPING ERROR',  DBMS_UTILITY.FORMAT_ERROR_STACK);
            RETURN 1;
       END;
	
END PKG_HOUSEKEEPING;
