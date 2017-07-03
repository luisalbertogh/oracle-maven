--liquibase formatted sql
--changeset P17.05.30:GBSVR-34712-1 runAlways:true
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (1,'register2','colum3text');
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (2,'registre2','colun4text2');
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=1 OR COLUMN1=2
--changeset P17.05.30:GBSVR-34712-2
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (3,'registre2','colun4text2');
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=3
--this coment belongs to previous changeset GBSVR-34712-2

--changeset P17.05.30:GBSVR-34712-3 runOnChange:false
--If we want to be sure that our record is inserted/updated, use MERGE
--also can use runonchange
MERGE into DUMMY_TABLE USING dual ON (column1=3)
WHEN MATCHED THEN UPDATE set "COLUMN2"='registre3',"COLUMN3"='newtextcolum7'
WHEN NOT MATCHED THEN INSERT (COLUMN1,COLUMN2,COLUMN3) values (3,'registre2','newtextcolum7');
-- the rollback would be deletion
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=3

--changeset P17.05.30:GBSVR-34712-4 runOnChange:true
--option 2, add a precondition
--preconditions onFail:MARK_RAN onError:HALT
--precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM DUMMY_TABLE WHERE COLUMN1=1
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (1,'register2','colum3text');
-- the rollback would be deletion
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=1
