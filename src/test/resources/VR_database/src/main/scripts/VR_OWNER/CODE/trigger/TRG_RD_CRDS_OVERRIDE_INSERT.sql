--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_RD_CRDS_OVERRIDE_INSERT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_RD_CRDS_OVERRIDE_INSERT
    BEFORE INSERT ON CRDS_OVERRIDE
    FOR EACH ROW 
DECLARE
	v_username varchar2(20); 
	
BEGIN

SELECT user INTO v_username FROM dual;
	:new.create_date := systimestamp;  
	:new.create_user := v_username;
	
EXCEPTION
	WHEN OTHERS THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
		DBMS_OUTPUT.put_line(SQLERRM);
END;
