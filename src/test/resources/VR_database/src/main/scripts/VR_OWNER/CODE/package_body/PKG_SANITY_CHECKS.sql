--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_SANITY_CHECKS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_SANITY_CHECKS" AS

    FUNCTION F_CHECK_TABLES_ACCESS(
        a_table_name    VARCHAR2
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;
        V_QUERY        VARCHAR2(200);
    BEGIN    
        V_RESULT := 1;
        
        BEGIN
            V_QUERY := 'SELECT 1 FROM ' || a_table_name;
            EXECUTE IMMEDIATE V_QUERY;
            DBMS_OUTPUT.PUT_LINE('INFO - TABLE ' || a_table_name || ' IS ACCESIBLE');
        EXCEPTION
            WHEN OTHERS THEN            
                V_RESULT := 0;
                DBMS_OUTPUT.PUT_LINE('ERROR - TABLE ' || a_table_name || ' IS NOT ACCESIBLE');
        END;

        RETURN V_RESULT;
    END F_CHECK_TABLES_ACCESS;

    /********************************************************************************************/

    FUNCTION F_CHECK_DATA_MASTER(
        a_table_name    VARCHAR2,
        a_rows_expected    NUMBER
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;    
        V_NUM_ROWS    NUMBER;    
        V_QUERY        VARCHAR2(200);
    BEGIN    
        V_RESULT := 1;    
        
        BEGIN
            V_QUERY := 'SELECT COUNT(*) INTO :a FROM ' || a_table_name;            
            EXECUTE IMMEDIATE V_QUERY INTO V_NUM_ROWS;

            
            IF V_NUM_ROWS <> a_rows_expected
            THEN                
                V_RESULT := 0;
                DBMS_OUTPUT.PUT_LINE('ERROR - TABLE ' || a_table_name || ' HAS ' || V_NUM_ROWS || ' ROWS WHEN EXPECTED IS ' || a_rows_expected);
            ELSE
                DBMS_OUTPUT.PUT_LINE('INFO - TABLE ' || a_table_name || ' CHECKED SUCCESFULLY');
            END IF;        
        EXCEPTION
            WHEN OTHERS THEN    
                DBMS_OUTPUT.PUT_LINE('ERROR CHECKING DATA MASTER FOR ' || a_table_name || ' TABLE. ' || SQLERRM);
                RETURN 0;
        END;        
        
        RETURN V_RESULT;
    END F_CHECK_DATA_MASTER;

    /********************************************************************************************************************/

    FUNCTION F_CHECK_PROCEDURES_ACCESS(
        a_package_name    VARCHAR2,
        a_procedure_name    VARCHAR2,
        a_owner        VARCHAR2
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;    
    BEGIN            
        SELECT COUNT(*) INTO V_RESULT FROM ALL_PROCEDURES
        WHERE OWNER = a_owner
        AND OBJECT_NAME = a_package_name
        AND PROCEDURE_NAME = a_procedure_name;
        
        IF V_RESULT = 1
        THEN
            DBMS_OUTPUT.PUT_LINE('INFO - PROCEDURE ' || a_package_name || '.' || a_procedure_name || ' IS ACCESSIBLE.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR - PROCEDURE ' || a_package_name || '.' || a_procedure_name || ' IS NOT ACCESSIBLE.');
        END IF;
        
        RETURN V_RESULT;    
    END F_CHECK_PROCEDURES_ACCESS;

    /********************************************************************************************************************/

    FUNCTION F_CHECK_VIEWS_ACCESS(
        a_view_name    VARCHAR2,
        a_owner        VARCHAR2        
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;    
    BEGIN                
        SELECT COUNT(*) INTO V_RESULT FROM ALL_VIEWS
        WHERE  OWNER = a_owner
        AND    VIEW_NAME = a_view_name;
        
        IF V_RESULT = 1
        THEN
            DBMS_OUTPUT.PUT_LINE('INFO - VIEW ' || a_view_name || ' IS ACCESSIBLE.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR - VIEW ' || a_view_name || ' IS NOT ACCESSIBLE.');
        END IF;
        
        RETURN V_RESULT;    
    END F_CHECK_VIEWS_ACCESS;
    
    /****************************************************************************************************************/
    
    FUNCTION F_CHECK_INVALID_OBJECTS(
        a_owner    VARCHAR2
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;
        V_OBJ_NAME    VARCHAR2(50);
        V_OBJ_TYPE    VARCHAR2(50);        
        
    BEGIN
        V_RESULT := 1;            
        
        FOR item IN (
            SELECT OBJECT_NAME, OBJECT_TYPE         
            FROM    ALL_OBJECTS 
            WHERE   OWNER = a_owner
            AND STATUS = 'INVALID'
        )            
        LOOP
            DBMS_OUTPUT.PUT_LINE('ERROR - OBJECT ' || item.object_name || ' OF TYPE ' || item.object_type || ' HAS INVALID STATUS.');
            V_RESULT := 0;    
        END LOOP;
        IF V_RESULT =1 THEN
            DBMS_OUTPUT.PUT_LINE('INFO - NO INVALID OBJECTS FOUND.');
        END IF;
        
        RETURN V_RESULT;
    EXCEPTION
        WHEN OTHERS THEN    
            DBMS_OUTPUT.PUT_LINE('ERROR CHECKING THE OBJECTS STATUS.' || SQLERRM);
            RETURN 0;
    END F_CHECK_INVALID_OBJECTS;
    
    /*****************************************************************************************************************/
    
    FUNCTION F_CHECK_TABLESPACE_FOR_INDEXES(
        a_tablespace_name    VARCHAR2,
        a_owner            VARCHAR2
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;
    BEGIN
        V_RESULT := 1;            
        
        FOR item IN (
            SELECT INDEX_NAME, TABLESPACE_NAME 
            FROM ALL_INDEXES
            WHERE  OWNER = a_owner
            AND TABLESPACE_NAME <> a_tablespace_name
        )            
        LOOP
            DBMS_OUTPUT.PUT_LINE('ERROR - INDEX ' || item.index_name || ' LOCATED IN TABLESPACE ' || item.tablespace_name || ' INSTEAD OF ' || a_tablespace_name);
            V_RESULT := 0;    
        END LOOP;
        IF V_RESULT =1 THEN
            DBMS_OUTPUT.PUT_LINE('INFO - INDEXES OK');
        END IF;
        
        RETURN V_RESULT;
    EXCEPTION
        WHEN OTHERS THEN    
            DBMS_OUTPUT.PUT_LINE('ERROR CHECKING THE TABLESPACE ASSIGNED TO INDEXES.' || SQLERRM);
            RETURN 0;
    END F_CHECK_TABLESPACE_FOR_INDEXES;
    
    /*****************************************************************************************************************/
    
    FUNCTION F_CHECK_TABLESPACE_FOR_TABLES(
        a_tablespace_name    VARCHAR2,
        a_owner            VARCHAR2
    ) RETURN NUMBER
    IS
        V_RESULT    NUMBER;
    BEGIN
        V_RESULT := 1;            
        
        FOR item IN (
            SELECT TABLE_NAME, TABLESPACE_NAME 
            FROM ALL_TABLES 
            WHERE OWNER = a_owner
            AND TABLESPACE_NAME <> a_tablespace_name
        )            
        LOOP
            DBMS_OUTPUT.PUT_LINE('ERROR - TABLE ' || item.table_name || ' LOCATED IN TABLESPACE ' || item.tablespace_name || ' INSTEAD OF ' || a_tablespace_name);
            V_RESULT := 0;    
        END LOOP;
        IF V_RESULT =1 THEN
            DBMS_OUTPUT.PUT_LINE('INFO - TABLES OK');
        END IF;
        
        RETURN V_RESULT;
    EXCEPTION
        WHEN OTHERS THEN    
            DBMS_OUTPUT.PUT_LINE('ERROR CHECKING THE TABLESPACE ASSIGNED TO TABLES.' || SQLERRM);
            RETURN 0;
    END F_CHECK_TABLESPACE_FOR_TABLES;    
    
END PKG_SANITY_CHECKS;
