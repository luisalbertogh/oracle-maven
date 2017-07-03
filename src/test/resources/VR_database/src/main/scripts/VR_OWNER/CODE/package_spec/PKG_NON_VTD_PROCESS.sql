--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_NON_VTD_PROCESS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


CREATE OR REPLACE PACKAGE PKG_NON_VTD_PROCESS AS
type      csv_table_array IS VARRAY (23) OF VARCHAR2 (4000);
type      csv_table_type IS TABLE OF csv_table_array;
function  f_is_number(str in varchar2) return BOOLEAN;
function  f_get_validation_msg(code IN NUMBER) RETURN VARCHAR2;
function  f_is_empty_record (p_excel_record IN csv_table_array, p_total_columns IN NUMBER) RETURN BOOLEAN;
procedure p_split_csv_process (idUpload IN NUMBER);
procedure p_update_staging (idUpload IN NUMBER);
procedure p_accept_upload (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR);
procedure p_reject_upload ( P_IDUPLOAD IN NUMBER, P_IDUSER IN VARCHAR, P_COMMENTS IN VARCHAR );

end PKG_NON_VTD_PROCESS;
