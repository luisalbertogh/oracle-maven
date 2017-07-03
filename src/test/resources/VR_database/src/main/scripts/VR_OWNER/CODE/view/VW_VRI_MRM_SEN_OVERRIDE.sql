--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_VRI_MRM_SEN_OVERRIDE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_VRI_MRM_SEN_OVERRIDE" ("ID", "DESK_ID", "RISK_FACTOR_NAME", "CHANGE_RISK_FACTOR", "RISK_FACTOR_UNITS", "AGGREGATE_CHANGE_VALUE", "START_DATE", "END_DATE", "LOADDATE") AS 
  select sen."ID",sen."DESK_ID",sen."RISK_FACTOR_NAME",sen."CHANGE_RISK_FACTOR",sen."RISK_FACTOR_UNITS",sen."AGGREGATE_CHANGE_VALUE",sen."START_DATE",sen."END_DATE",sen."LOADDATE"
from vri_mrm_sen_override sen,vw_vri_setup s
where (sen.start_date>=s.initial_date or sen.end_date>=s.initial_date) and
(sen.start_date<=s.end_date or sen.end_date<=s.end_date);
