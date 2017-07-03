--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_RD_DK_LIM_CONF_INSERT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_RD_DK_LIM_CONF_INSERT 
    BEFORE INSERT ON RD_DESK_LIMIT_CONFIG 
    FOR EACH ROW 
DECLARE
	v_username varchar2(20); 
	uniqueentry NUMBER;
	RD_DESK_LIMIT_CONF_DUPLICATE exception;
	
BEGIN

SELECT count(*) c_count into uniqueentry  from RD_DESK_LIMIT_CONFIG 
where 	 VOLCKER_TRADING_DESK = :new.VOLCKER_TRADING_DESK 
	and LIMIT_CONCEPT =:new.LIMIT_CONCEPT
	and asofdate = :new.ASOFDATE;
IF uniqueentry > 0 THEN
  raise RD_DESK_LIMIT_CONF_DUPLICATE;
END IF;


SELECT user INTO v_username FROM dual;
	:new.create_date := systimestamp;  
	:new.create_user := v_username;
	:new.action := 'INSERT';

EXCEPTION
    WHEN RD_DESK_LIMIT_CONF_DUPLICATE THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
		DBMS_OUTPUT.put_line(SQLERRM);

END;
