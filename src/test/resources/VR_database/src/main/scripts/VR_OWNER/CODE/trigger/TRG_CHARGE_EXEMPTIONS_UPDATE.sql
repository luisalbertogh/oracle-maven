--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_CHARGE_EXEMPTIONS_UPDATE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_CHARGE_EXEMPTIONS_UPDATE 
    BEFORE UPDATE ON CHARGE_EXEMPTIONS 
    FOR EACH ROW 
DECLARE
   v_username varchar2(20);  
   
BEGIN
SELECT user INTO v_username FROM dual;
:new.last_modified_date := systimestamp;  
:new.last_modification_user := v_username;   
EXCEPTION
     WHEN others THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
        DBMS_OUTPUT.put_line(SQLERRM);
END;
