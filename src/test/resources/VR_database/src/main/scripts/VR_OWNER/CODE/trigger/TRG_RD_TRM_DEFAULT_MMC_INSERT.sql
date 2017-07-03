--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_RD_TRM_DEFAULT_MMC_INSERT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_RD_TRM_DEFAULT_MMC_INSERT 
    BEFORE INSERT ON RD_TRM_DEFAULT_MMC 
    FOR EACH ROW 
DECLARE
	v_username varchar2(20); 
	uniqueentry NUMBER;
	RD_TRM_MMC_FLAG_DUPLICATE exception;
BEGIN

SELECT count(*) c_count into uniqueentry  from RD_TRM_DEFAULT_MMC 
where 	 VOLCKER_TRADING_DESK = :new.VOLCKER_TRADING_DESK 
	and DF_MM_CLASSIFICATION = :new.DF_MM_CLASSIFICATION 
	and asofdate = :new.ASOFDATE;
IF uniqueentry > 0 THEN
  raise RD_TRM_MMC_FLAG_DUPLICATE;
END IF;


SELECT user INTO v_username FROM dual;
	:new.create_date := systimestamp;  
	:new.create_user := v_username;
	:new.action := 'INSERT';

EXCEPTION
    WHEN RD_TRM_MMC_FLAG_DUPLICATE THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
		DBMS_OUTPUT.put_line(SQLERRM);

END;
