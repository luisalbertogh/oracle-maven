--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_RD_MM_OVERRIDE_INSERT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_RD_MM_OVERRIDE_INSERT 
    BEFORE INSERT ON RD_MM_OVERRIDE 
    FOR EACH ROW 
DECLARE
	v_username varchar2(20); 
	uniqueentry NUMBER;
	RD_MM_OVERRIDE_DUPLICATE exception;
	
BEGIN

SELECT count(*) c_count into uniqueentry  from RD_MM_OVERRIDE 
where 	 VOLCKER_TRADING_DESK = :new.VOLCKER_TRADING_DESK 
	and SOURCE_SYSTEM = :new.SOURCE_SYSTEM
	and BOOK_ID =:new.BOOK_ID
	and PRDS_CODE = :new.PRDS_CODE
	and PRDS_NAME = :new.PRDS_NAME
	and PRODUCT_TYPE_1 = :new.PRODUCT_TYPE_1
	and PRODUCT_TYPE_2 = :new.PRODUCT_TYPE_2
	and INSTRUMENT_ID = :new.INSTRUMENT_ID
	and MM_OVERRIDE_CLASSIFICATION = :new.MM_OVERRIDE_CLASSIFICATION 
	and asofdate = :new.ASOFDATE;
IF uniqueentry > 0 THEN
  raise RD_MM_OVERRIDE_DUPLICATE;
END IF;


SELECT user INTO v_username FROM dual;
	:new.create_date := systimestamp;  
	:new.create_user := v_username;
	:new.action := 'INSERT';

EXCEPTION
    WHEN RD_MM_OVERRIDE_DUPLICATE THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
		DBMS_OUTPUT.put_line(SQLERRM);

END;
