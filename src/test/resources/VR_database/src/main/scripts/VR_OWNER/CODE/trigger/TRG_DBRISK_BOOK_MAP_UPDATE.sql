--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_DBRISK_BOOK_MAP_UPDATE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_DBRISK_BOOK_MAP_UPDATE
    BEFORE UPDATE ON DBRISK_BOOK_MAP
    FOR EACH ROW 
DECLARE
	v_username varchar2(20); 
BEGIN
:new.MODIFY_DATE := systimestamp;  
	
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
		DBMS_OUTPUT.put_line(SQLERRM);
	
END;
