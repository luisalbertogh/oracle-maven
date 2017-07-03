--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_RD_CUSTOM_RFS_INSERT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_RD_CUSTOM_RFS_INSERT
    BEFORE INSERT ON RD_CUSTOM_RFS 
    FOR EACH ROW 
DECLARE

   v_username varchar2(50);
   l_count binary_integer;
   l_error binary_integer;
   v_field varchar2(50);
   v_field_check varchar2(50);
   RD_INVALID_OPERATOR exception;
   RD_INVALID_RF exception;
   RD_INVALID_PARAM_NUMBER exception;
  
BEGIN
  --discard formulas which ends up with a comma (last field is empty)
  SELECT    REGEXP_COUNT(:new.FORMULA, '+;$') into l_error FROM DUAL;
  IF(l_error=1) THEN raise RD_INVALID_PARAM_NUMBER; END IF;
  --discard formulas which has empty fields between comma
  SELECT    REGEXP_COUNT(:new.FORMULA, '+;;+') into l_error FROM DUAL;
  IF(l_error=1) THEN raise RD_INVALID_PARAM_NUMBER; END IF;
  --discard formulas which starts with a comma (first field is empty)
  SELECT    REGEXP_COUNT(:new.FORMULA, '^;+') into l_error FROM DUAL;
  IF(l_error=1) THEN raise RD_INVALID_PARAM_NUMBER; END IF;
   SELECT    REGEXP_COUNT(:new.FORMULA, '[^;]+') into l_count FROM DUAL;
   --number of fields must be odd
   IF(MOD(l_count,2)=0) THEN
   raise RD_INVALID_PARAM_NUMBER;
   END IF;
    
    for i in 1 .. l_count
    loop
     v_field_check:=null;
     SELECT  REGEXP_SUBSTR(:new.FORMULA, '[^;]+', i, i) into v_field FROM dual;
     IF(MOD(i,2)=0) THEN
       
       SELECT  REGEXP_SUBSTR(v_field, '\+|\-', 1,1) into v_field_check FROM dual;
       --operator must be + or -
       IF v_field_check is NULL THEN
         raise RD_INVALID_OPERATOR;
       END IF;
     ELSE
       SELECT MAX(factor_name) into v_field_check FROM DB_RISK_STORE_CONF where factor_name=v_field;
	   --standard risk must exist in DB_RISK_STORE_CONF
        IF v_field_check is NULL THEN
         raise RD_INVALID_RF;
       END IF;
     END IF;
    end loop;
	SELECT user INTO v_username FROM dual;
	:new.create_date := systimestamp;  
	:new.create_user := v_username;
	:new.action := 'INSERT';
   EXCEPTION
    WHEN RD_INVALID_OPERATOR THEN
		    raise_application_error (-20001,'Not valid operator: Must be + or -.');
     WHEN RD_INVALID_RF THEN
        raise_application_error (-20002,'Not valid risk factor');
     WHEN RD_INVALID_PARAM_NUMBER THEN 
	 raise_application_error (-20003,'Not correct number of aruguments in FORMULA');
END;
