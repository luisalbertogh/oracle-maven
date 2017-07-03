--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_VRI_MRM_LIMIT_OVERRIDE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_VRI_MRM_LIMIT_OVERRIDE" ("ID", "DESK_ID", "TYPE_OF_LIMIT", "LIMIT_SIZE", "VALUE_USAGE", "LIMIT_USAGE", "MEASUREMENT_UNIT", "START_DATE", "END_DATE", "LOADDATE") AS 
  select lim."ID",lim."DESK_ID",lim."TYPE_OF_LIMIT",lim."LIMIT_SIZE",lim."VALUE_USAGE",lim."LIMIT_USAGE",lim."MEASUREMENT_UNIT",lim."START_DATE",lim."END_DATE",lim."LOADDATE"
from vri_mrm_limit_override lim,vw_vri_setup s
where (lim.start_date>=s.initial_date or lim.end_date>=s.initial_date) and
(lim.start_date<=s.end_date or lim.end_date<=s.end_date);
