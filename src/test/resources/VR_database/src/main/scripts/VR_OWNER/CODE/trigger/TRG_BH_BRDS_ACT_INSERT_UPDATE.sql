--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_BH_BRDS_ACT_INSERT_UPDATE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_BH_BRDS_ACT_INSERT_UPDATE
  BEFORE INSERT OR UPDATE ON BH_BRDS_ACTIVATION
  FOR EACH ROW 
DECLARE
  v_unique_row NUMBER;
  BH_PK_VIOLATION   EXCEPTION;
  BH_PK_VIOLATION_2 EXCEPTION;
BEGIN
    IF INSERTING THEN
      SELECT COUNT(*) quantity INTO v_unique_row FROM bh_brds_activation;
      IF v_unique_row > 0 THEN
        raise BH_PK_VIOLATION;
      END IF;
    END IF;
    
    IF :new.activation NOT IN (0, 1) THEN
      raise BH_PK_VIOLATION_2;
    END IF;
EXCEPTION
	WHEN BH_PK_VIOLATION THEN
	  PKG_MONITORING.PR_INSERT_LOG_JOBS_QV('bRDS', NULL, NULL, CURRENT_DATE, 'TRG_BH_BRDS_ACTIVATION_INSERT_UPDATE', 'ERROR', 'FATAL', 'Only one row is allowed.', 'bRDS');
	  RAISE;
	WHEN BH_PK_VIOLATION_2 THEN
	  PKG_MONITORING.PR_INSERT_LOG_JOBS_QV('bRDS', NULL, NULL, CURRENT_DATE, 'TRG_BH_BRDS_ACTIVATION_INSERT_UPDATE', 'ERROR', 'FATAL', 'Only [0,1] are allowed values.', 'bRDS');
	  RAISE;
END;