--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:RECOMPILE  failOnError:TRUE splitStatements:FALSE
begin
	dbms_utility.compile_schema(schema=> USER);
end;