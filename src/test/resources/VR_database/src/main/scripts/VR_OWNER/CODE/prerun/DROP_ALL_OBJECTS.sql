--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:DROP_ALL_OBJECTS  failOnError:TRUE splitStatements:FALSE
declare
    cursor c_drop_objects is 
    select object_name, object_type
    from user_objects 
    where object_type in ('VIEW','PACKAGE','PROCEDURE','FUNCTION','TYPE','SYNONYM')
    order by object_type;
    r_drop_objects c_drop_objects%rowtype;
begin
    for r_drop_objects in c_drop_objects loop
        execute immediate 'DROP '||r_drop_objects.object_type||' "'||r_drop_objects.object_name||'"'|| case when r_drop_objects.object_type = 'TYPE' then 'FORCE' end;
    end loop;
                begin
                                execute immediate 'update databasechangelog set exectype=''SKIPPED'' where author='||user||'''${vr_owner_user}_CODE''';
                                exception
                                                when others then null;
                end;
end;