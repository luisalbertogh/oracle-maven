--liquibase formatted sql
--changeset P17.05.30:GBSVR-34712 
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (1,'register2','colum3text');
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (2,'registre2','colun4text2');
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=1 OR COLUMN1=2
--changeset P17.05.30:GBSVR-34712-2
Insert into DUMMY_TABLE (COLUMN1,COLUMN2,COLUMN3) values (3,'registre2','colun4text2');
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=3
--If we want to be sure that our record is inserted/updated, use MERGE
--changeset P17.05.30:GBSVR-34712-3
MERGE into DUMMY_TABLE USING dual ON (column1=3)
WHEN MATCHED THEN UPDATE set "column2"='registre2',"column3"='newtextcolum'
WHEN NOT MATCHED THEN INSERT (COLUMN1,COLUMN2,COLUMN3) values (3,'registre2','newtextcolum');
-- the rollback would be deletion
--rollback DELETE FROM DUMMY_TABLE WHERE COLUMN1=3

