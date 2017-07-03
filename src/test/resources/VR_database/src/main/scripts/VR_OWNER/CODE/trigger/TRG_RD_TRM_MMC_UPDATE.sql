--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_RD_TRM_MMC_UPDATE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_RD_TRM_MMC_UPDATE 
    BEFORE UPDATE ON RD_TRM_MMC 
    FOR EACH ROW 
DECLARE
	v_username varchar2(20); 
BEGIN

SELECT user INTO v_username FROM dual;
	:new.MODIFY_DATE := systimestamp;  
	:new.MODIFY_USER := v_username;
	:new.action := 'UPDATE';
	
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
		DBMS_OUTPUT.put_line(SQLERRM);
	
	
END;
