--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_SDATA_AUDIT_HISTORY_PKG runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "SDATA_AUDIT_HISTORY_PKG" 
IS
   PROCEDURE SDATA_AUDIT_HISTORY_PROC (
   	p_audit_detail_arr 	IN 	SDATA_AUDIT_HISTORY_TYPE_TABLE
   	,p_widget_id 		IN 	Number
   	,p_action_type 		IN 	varchar2
   	,p_email_id 		IN 	varchar2
   	,p_approver_email_id	IN 	varchar2
   );

END SDATA_AUDIT_HISTORY_PKG;
