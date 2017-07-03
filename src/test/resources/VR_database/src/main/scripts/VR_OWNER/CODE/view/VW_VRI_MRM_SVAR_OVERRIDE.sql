--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_VRI_MRM_SVAR_OVERRIDE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_VRI_MRM_SVAR_OVERRIDE" ("ID", "DESK_ID", "VAR", "SVAR", "START_DATE", "END_DATE", "LOADDATE") AS 
  select svar."ID",svar."DESK_ID",svar."VAR",svar."SVAR",svar."START_DATE",svar."END_DATE",svar."LOADDATE"
from vri_mrm_svar_override svar,vw_vri_setup s
where (svar.start_date>=s.initial_date or svar.end_date>=s.initial_date) and
(svar.start_date<=s.end_date or svar.end_date<=s.end_date);
