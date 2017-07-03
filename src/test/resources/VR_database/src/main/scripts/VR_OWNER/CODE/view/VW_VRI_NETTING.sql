--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_VRI_NETTING runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_VRI_NETTING" ("ID", "VERSION_ID", "METRIC_ID", "GROUP_ID", "DESK_ID", "SOURCE_SYSTEM_ID", "PRODUCT_ID", "NETTING_TYPE") AS 
  SELECT distinct n.ID,
n.VERSION_ID,         
n.metric_id,
n.group_id,
n.desk_id,
n.source_system_id,
n.product_id,
n.netting_type
FROM VRI_NETTING n,VW_VRI_SETUP s
WHERE (n.version_id = s.subversion_id and s.scope = 'manual') or 
n.version_id =  s.scope
ORDER BY version_id,metric_id, group_id, source_system_id, product_id;
