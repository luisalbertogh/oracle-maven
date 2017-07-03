--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_VRI_SHAREPOINT_PATHS runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_VRI_SHAREPOINT_PATHS" ("ITEMTYPE", "ITEMID", "FILEPATH", "ASOFDATE") AS 
  SELECT "ITEMTYPE","ITEMID","FILEPATH","ASOFDATE"
FROM VRI_SHAREPOINT_PATHS sh
WHERE sh.ASOFDATE =
(SELECT MAX(sh1.ASOFDATE)
FROM VRI_SHAREPOINT_PATHS sh1
where sh1.asofdate <= (select max(s.end_date) from vw_vri_setup s));
