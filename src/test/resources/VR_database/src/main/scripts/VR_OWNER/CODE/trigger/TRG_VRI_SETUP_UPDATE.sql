--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_VRI_SETUP_UPDATE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_VRI_SETUP_UPDATE
  BEFORE UPDATE ON VRI_SETUP 
  FOR EACH ROW 
  BEGIN

  IF :new.status = 'DONE' or :new.status = 'FAILED' THEN
    insert into mis_pending_jobs (VERSION_ID,INITIAL_DATE,END_DATE,VRI_STATUS,MIS_ORACLE_STATUS,MIS_QV_STATUS,MIS_QV_IA_STATUS,RERUN_FLG,SCOPE,MIS_PDF_STATUS) values(:new.version_id, :new.initial_date, :new.end_date, '0', '0', '0','0','0', :new.scope, '0');
  end if;

EXCEPTION
     WHEN others THEN
        DBMS_OUTPUT.put_line('Error:'||TO_CHAR(SQLCODE));
        DBMS_OUTPUT.put_line(SQLERRM);
END;
