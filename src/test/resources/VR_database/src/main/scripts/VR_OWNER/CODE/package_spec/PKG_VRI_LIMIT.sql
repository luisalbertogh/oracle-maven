--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_VRI_LIMIT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_VRI_LIMIT" AS 
/*****************************************************/
/*Split CSV file into lines, check format for fields */
/*Insert into intermediary table.                    */
/*****************************************************/
PROCEDURE P_MAIN_PROCEDURE_LIMIT(idUpload IN NUMBER);
PROCEDURE P_SPLIT_CSV (idUpload IN NUMBER);  
PROCEDURE P_CHECK_FORMAT_FIELD(field_name IN VARCHAR2, field_value IN OUT VARCHAR2, C_SIZE_PARAM IN NUMBER, cont_error IN OUT VARCHAR2, validation_msg IN OUT VARCHAR ); 
PROCEDURE P_CHECK_FUNCIONAL_LIMITS(idUpload IN NUMBER);
PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR);
PROCEDURE P_REJECT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR);

END PKG_VRI_LIMIT;
