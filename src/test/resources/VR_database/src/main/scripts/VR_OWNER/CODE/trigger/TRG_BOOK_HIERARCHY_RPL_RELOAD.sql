--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_BOOK_HIERARCHY_RPL_RELOAD runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_BOOK_HIERARCHY_RPL_RELOAD 
BEFORE DELETE OR INSERT OR UPDATE ON BOOK_HIERARCHY_RPL 
FOR EACH ROW
DECLARE
  v_quantity NUMBER;
  v_asofdate DATE;
BEGIN
  IF DELETING THEN
    v_asofdate := :old.asofdate;
  ELSE
    v_asofdate := :new.asofdate;
  END IF;
  
  --If there is a request pending to start for all the reloads of the same asofdate, there is no need to insert a new request
  SELECT count(*) into v_quantity  
    FROM book_hierarchy_rpl_reload
   WHERE asofdate = v_asofdate
     AND (reload_hive_start IS NULL AND reload_qv_start IS NULL);
     
  IF v_quantity = 0 THEN
    INSERT INTO BOOK_HIERARCHY_RPL_RELOAD VALUES (
        SEQ_BOOK_HIERARCHY_RPL_RELOAD.NEXTVAL,
        v_asofdate,
        systimestamp,
        null, null, null, null);
  END IF;
  
  --If the asofdate was changed, the book hierarchy of the previous asofdate must also be updated
  IF UPDATING AND :old.asofdate <> :new.asofdate THEN
    v_asofdate := :old.asofdate;
    
    --If there is a request pending to start for all the reloads of the same asofdate, there is no need to insert a new request
    SELECT count(*) into v_quantity  
      FROM book_hierarchy_rpl_reload
     WHERE asofdate = v_asofdate
       AND (reload_hive_start IS NULL AND reload_qv_start IS NULL);
       
    IF v_quantity = 0 THEN
      INSERT INTO BOOK_HIERARCHY_RPL_RELOAD VALUES (
          SEQ_BOOK_HIERARCHY_RPL_RELOAD.NEXTVAL,
          v_asofdate,
          systimestamp,
          null, null, null, null);
    END IF;
  END IF;
  
END;
