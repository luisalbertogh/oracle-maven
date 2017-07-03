--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_CRDS_OVERRIDE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_CRDS_OVERRIDE" AS 

/*****************************************************/
/*Split CSV file into lines, check format for fields */
/*Insert into intermediary table.                    */
/*****************************************************/
PROCEDURE P_SPLIT_CSV_CRDS (idUpload IN NUMBER);

PROCEDURE P_CHECK_ACTION_VALUE (field_name IN VARCHAR2,field IN OUT VARCHAR2, cont_error IN OUT NUMBER, validation_msg IN OUT VARCHAR2, ref_table IN VARCHAR2);

PROCEDURE P_CHECK_CUSTUMER_CLASSIF_VALUE (field_name IN VARCHAR2,field IN OUT VARCHAR2, cont_error IN OUT NUMBER, validation_msg IN OUT VARCHAR2, ref_table IN VARCHAR2);

PROCEDURE P_IS_EMPTY_VALUE (field_name IN VARCHAR2,field IN OUT VARCHAR2);

PROCEDURE P_CHECK_SOURCE_SYSTEM_VALUE (field_name IN VARCHAR2,field IN OUT VARCHAR2, cont_error IN OUT NUMBER, validation_msg IN OUT VARCHAR2, ref_table IN VARCHAR2);

PROCEDURE P_CHECK_NO_DUPLICATES_INT(idUpload IN NUMBER);

PROCEDURE P_IS_SAME_ASOFDATE(P_UPLOAD_ID IN NUMBER);

PROCEDURE P_CHECK_ASOFDATE_CRDS_OVERRIDE (asofdate IN VARCHAR2, cont_error IN OUT NUMBER, validation_msg IN OUT VARCHAR2, ref_table IN VARCHAR2,p_status IN VARCHAR2,p_source_system_crds IN VARCHAR2,p_counter_party_id IN VARCHAR2,p_volcker_trading_desk IN VARCHAR2,p_book_id IN VARCHAR2,p_position_id IN VARCHAR2,p_trade_id IN VARCHAR2);

PROCEDURE P_MAIN_PROCEDURE(P_UPLOAD_ID IN NUMBER);

FUNCTION F_INTERMEDIARY_TO_STAGING(idUpload NUMBER) RETURN BOOLEAN;

PROCEDURE P_CHECK_VALIDATION(idUpload IN NUMBER);
--Jira. GBSVR-19417 Developer: Victor Rodriguez Date: 03/12/2015
PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR);

PROCEDURE P_REJECT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR);
--END Jira. GBSVR-19417 Developer: Victor Rodriguez Date: 03/12/2015
END PKG_CRDS_OVERRIDE;
