--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_VRI_NA_TEMPLATE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_VRI_NA_TEMPLATE" ("ID", "VERSION", "TRADING_DESK", "PANEL_ID", "SUBPANEL_ID", "PARAM_ID", "COBDATE", "DEFAULT_VALUE") AS 
  SELECT distinct na.ID,
na.VERSION,
na.TRADING_DESK,         
na.PANEL_ID,
na.SUBPANEL_ID,
na.PARAM_ID,          
na.COBDATE,
na.DEFAULT_VALUE
FROM VRI_NA_TEMPLATE na,VW_VRI_SETUP s
WHERE (na.version = s.version_id and s.scope = 'manual') or 
version = s.scope;
