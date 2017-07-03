--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_SANITY_CHECKS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_SANITY_CHECKS" AUTHID CURRENT_USER AS    

    FUNCTION F_CHECK_TABLES_ACCESS(
        a_table_name    VARCHAR2
    ) RETURN NUMBER;
    
    FUNCTION F_CHECK_DATA_MASTER(
        a_table_name    VARCHAR2,
        a_rows_expected    NUMBER
    ) RETURN NUMBER;
        
    FUNCTION F_CHECK_PROCEDURES_ACCESS(
        a_package_name    VARCHAR2,
        a_procedure_name    VARCHAR2,
        a_owner        VARCHAR2
    ) RETURN NUMBER;
    
    FUNCTION F_CHECK_VIEWS_ACCESS(
        a_view_name    VARCHAR2,
        a_owner        VARCHAR2        
    ) RETURN NUMBER;
    
    FUNCTION F_CHECK_INVALID_OBJECTS(
        a_owner    VARCHAR2
    ) RETURN NUMBER;
    
    FUNCTION F_CHECK_TABLESPACE_FOR_INDEXES(
        a_tablespace_name    VARCHAR2,
        a_owner                VARCHAR2
    ) RETURN NUMBER;
    
    FUNCTION F_CHECK_TABLESPACE_FOR_TABLES(
        a_tablespace_name    VARCHAR2,
        a_owner                VARCHAR2
    ) RETURN NUMBER;

END PKG_SANITY_CHECKS;
