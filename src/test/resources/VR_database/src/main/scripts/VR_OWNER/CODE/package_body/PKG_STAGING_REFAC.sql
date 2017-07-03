--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_STAGING_REFAC runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_STAGING_REFAC" as

function f_get_partition_name_REFAC (a_source_system SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,a_asofdate AGE.ASOFDATE%TYPE, a_tablename VARCHAR2) return VARCHAR2
is
v_partition varchar2(10000);
v_number       NUMBER(15);
begin
    select nvl((select partition_name 
    from user_tab_partitions where table_name=a_tablename
    and to_date(replace (regexp_substr (extractvalue (dbms_xmlgen. getxmltype ('select high_value from user_tab_partitions where table_name=''' || table_name || ''' and partition_name = ''' || partition_name || ''''),'//text()'), '''.*?'''),''''),'syyyy-mm-dd hh24:mi:ss')=a_asofdate+1
    ),'ANY') into v_partition
     from dual;

     return v_partition;
     
exception 
    when NO_DATA_FOUND then
    return 'ERROR';
    when others then 
    dbms_output.put_line(SQLERRM);
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS (a_source_system,null,a_source_system,a_asofdate,'PKG_STAGING','ERROR', 'FATAL', 'ERROR', 'f_get_partition_name '||SQLERRM);
    raise;
end;

function f_get_subpartition_name_REFAC (a_source_system SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,a_asofdate AGE.ASOFDATE%TYPE, a_tablename VARCHAR2) return VARCHAR2
is
v_subpartition varchar2(10000);
v_number       NUMBER(15);
begin
    select nvl((select subpartition_name  
    from user_tab_subpartitions where partition_name=( select partition_name 
    from user_tab_partitions where table_name=a_tablename
    and to_date(replace (regexp_substr (extractvalue (dbms_xmlgen. getxmltype ('select high_value from user_tab_partitions where table_name=''' || table_name || ''' and partition_name = ''' || partition_name || ''''),'//text()'), '''.*?'''),''''),'syyyy-mm-dd hh24:mi:ss')=a_asofdate+1
    ) and replace (extractvalue (dbms_xmlgen. getxmltype ('select high_value from user_tab_subpartitions where table_name=''' || table_name || ''' and subpartition_name = ''' || subpartition_name || ''''),'//text()'), '''','')=a_source_system),'ANY') 
    into v_subpartition
    from dual;
     
    return v_subpartition;
     
exception 
    when NO_DATA_FOUND then
    return 'ERROR';
    when others then 
    dbms_output.put_line(SQLERRM);
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS (a_source_system,null,a_source_system,a_asofdate,'PKG_STAGING','ERROR', 'FATAL', 'ERROR', 'f_get_subpartition_name '||SQLERRM);
    raise;
end;

function f_get_max_high_value_REFAC (a_source_system SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,a_asofdate AGE.ASOFDATE%TYPE, a_tablename VARCHAR2) return date
is
v_maxdate date;
v_number       NUMBER(15);
begin

    select max( to_date(replace (regexp_substr (extractvalue (dbms_xmlgen. getxmltype ('select high_value from user_tab_partitions where table_name=''' || table_name || ''' and partition_name = ''' || partition_name || ''''),'//text()'), '''.*?'''),''''),'syyyy-mm-dd hh24:mi:ss'))
    into v_maxdate
    from user_tab_partitions a 
    where table_name=a_tablename;
     
    return v_maxdate;
     
exception 
    when others then 
    dbms_output.put_line(SQLERRM);
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS (a_source_system,null,a_source_system,a_asofdate,'PKG_STAGING','ERROR', 'FATAL', 'ERROR', 'f_get_subpartition_name '||SQLERRM);
    raise;
end;


function f_prepare_aging_tables_REFAC (a_source_system SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,a_asofdate AGE.ASOFDATE%TYPE) return number
is
query varchar2(10000);
v_partition varchar2(10000);
v_subpartition varchar2(10000);
v_maxdate date;
v_number number(15);
CURSOR table_list_cur
     IS
          SELECT table_name
            FROM housekeeping_REFACTORING
           WHERE table_name not in('FX_RATES')
           order by PARTITION_CASE_ID ASC;
 
table_list   table_list_cur%ROWTYPE;

begin    
    OPEN table_list_cur;

     LOOP
        FETCH table_list_cur INTO table_list;
        EXIT WHEN table_list_cur%NOTFOUND;
        dbms_output.put_line(table_list.table_name);     
         v_partition:=PKG_STAGING_REFAC.F_GET_PARTITION_NAME_REFAC (a_source_system,a_asofdate,table_list.table_name);
         v_subpartition:=PKG_STAGING_REFAC.F_GET_SUBPARTITION_NAME_REFAC (a_source_system,a_asofdate,table_list.table_name);
         v_maxdate:=PKG_STAGING_REFAC.F_GET_MAX_HIGH_VALUE_REFAC (a_source_system,a_asofdate,table_list.table_name);
         dbms_output.put_line(v_partition);
         dbms_output.put_line(v_subpartition);
         dbms_output.put_line(v_maxdate);
         if v_partition = 'ANY' and v_maxdate > a_asofdate then
        dbms_output.put_line('if1');
            query:='delete from '||table_list.table_name||' where SOURCE_SYSTEM_ID='''||a_source_system||''' and asofdate=to_date('''||a_asofdate||''',''dd/mm/yy'')';
            dbms_output.put_line(query);
            EXECUTE IMMEDIATE query;

            commit;
            
        END iF;
        
        if v_subpartition = 'ANY' and v_partition <>'ANY' then
        dbms_output.put_line('if2'); 

            query:='alter table '||table_list.table_name||' modify partition '||v_partition||' add subpartition SP_'||to_char(a_asofdate,'yyyymmdd')||'_'||substr(replace(a_source_system,'IMAGINE',''),0,12)||' values('''||a_source_system||''')';
            EXECUTE IMMEDIATE query;
            
            query:='delete from '||table_list.table_name||' where SOURCE_SYSTEM_ID='''||a_source_system||''' and asofdate=to_date('''||a_asofdate||''',''dd/mm/yy'')';
            EXECUTE IMMEDIATE query;

            commit;
            
        END IF;
        
        if v_subpartition <> 'ANY' and table_list.table_name='TRADE' then
        dbms_output.put_line('if3');
            
            query:='alter table '||table_list.table_name||' truncate subpartition '||v_subpartition;
            
            EXECUTE IMMEDIATE query;
            
        END IF;

        if v_subpartition <> 'ANY' and table_list.table_name<>'TRADE' then
        dbms_output.put_line('if4');
            
            query:='delete from '||table_list.table_name||' where SOURCE_SYSTEM_ID='''||a_source_system||''' and asofdate=to_date('''||a_asofdate||''',''dd/mm/yy'')';
        dbms_output.put_line(query);
            EXECUTE IMMEDIATE query;
            
            commit;            
        END IF;
     
    END LOOP;

    CLOSE table_list_cur;

    
    return 0;
exception 
    when NO_DATA_FOUND then
    return 0;
    when others then 
    dbms_output.put_line(SQLERRM);
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS (a_source_system,null,a_source_system,a_asofdate,'PKG_STAGING','ERROR', 'FATAL', 'ERROR', SQLERRM);
    rollback;
    return 1;
    raise;
end;
END pkg_staging_REFAC;
