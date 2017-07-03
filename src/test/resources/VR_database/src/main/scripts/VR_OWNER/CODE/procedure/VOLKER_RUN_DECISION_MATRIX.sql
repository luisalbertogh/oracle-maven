-- headers from liquibase removed as changeset now declared in install-changelog.xml
  CREATE OR REPLACE PROCEDURE "VOLKER_RUN_DECISION_MATRIX" (V_RUNID NUMBER)
AS 
V_RUNNING INT;
P_RUNID NUMBER;
BEGIN
    SELECT NVL(V_RUNID, GETRUNID) INTO P_RUNID FROM DUAL;
--- carlos made a change
    VOLKER_RUN_UPDATE_STATUS(P_RUNID, 'WAITING', 'Awaiting reference data');
    COMMIT;

    SELECT COUNT(1) INTO V_RUNNING FROM VOLKER_IS_RUNNING WHERE RUNNING = 1;

    IF V_RUNNING <> 0 THEN

        DBMS_OUTPUT.PUT_LINE('There is a process already running');
        VOLKER_RUN_UPDATE_STATUS(P_RUNID, 'FAILED', 'There is a process already running');
        raise_application_error(-20000,'There is a process already running');
        GOTO THEEND;

    ELSE

        VOLKER_RUN_UPDATE_STATUS(P_RUNID, 'RUNNING', 'Process started');
        DBMS_OUTPUT.PUT_LINE('starting volker decision matrix run');
        INSERT INTO VOLKER_IS_RUNNING VALUES ( 1 ) ;
        COMMIT;

        --   CR 21 Sep 2015 -- AS removed as theis processing is now being perforemed outside this proc.      

        --DBMS_OUTPUT.PUT_LINE('preprocess intex');
        --VOLKER_INTEX_XREF(P_RUNID);
        --COMMIT;
        --VOLKER_PROCESS_INTEX_DIM(P_RUNID,GETCOBDATE));          
         --COMMIT;
        --DBMS_OUTPUT.PUT_LINE('preprocess Bloomberg');
        --VOLKER_PROCESS_BLOOMBERG_DIM(P_RUNID,GETCOBDATE);
        --COMMIT;

        --   CR 21 Sep 2015 -- AS 

        DBMS_OUTPUT.PUT_LINE('preprocess dbVolt');
        VOLKER_PROCESS_VOLT(P_RUNID);
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('preprocessing complete');
        VOLKER_RUN_UPDATE_STATUS(P_RUNID, 'WAITING', 'Reference data complete');
        VOLKER_RUN_DECISION_MATRIX_P3(P_RUNID);
        DBMS_OUTPUT.PUT_LINE('ending decision matrix run');
        DELETE FROM VOLKER_IS_RUNNING;
        COMMIT;
    END IF;

<<THEEND>>
    DBMS_OUTPUT.PUT_LINE('end procedure');
END;
