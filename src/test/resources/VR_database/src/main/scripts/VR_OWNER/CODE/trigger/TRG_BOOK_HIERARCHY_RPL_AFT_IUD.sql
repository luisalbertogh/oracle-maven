--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:TRIGGER_TRG_BOOK_HIERARCHY_RPL_AFT_IUD runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE

CREATE OR REPLACE TRIGGER TRG_BOOK_HIERARCHY_RPL_AFT_IUD
   AFTER INSERT OR UPDATE OR DELETE
   ON BOOK_HIERARCHY_RPL
   FOR EACH ROW
BEGIN
   IF (DELETING OR UPDATING)
   THEN
      BEGIN
         INSERT INTO BOOK_HIERARCHY_RPL_OUT_OF_DATE
              VALUES (:old.ASOFDATE);
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;
   END IF;

   IF (INSERTING OR UPDATING)
   THEN
      BEGIN
         INSERT INTO BOOK_HIERARCHY_RPL_OUT_OF_DATE
              VALUES (:new.ASOFDATE);
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;
   END IF;
END;