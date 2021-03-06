--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_BRDS_REP_DUPLICATE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_BRDS_REP_DUPLICATE" ("ID", "ASOFDATE", "WORKFLOW_TYPE_ID", "BOOK_ID", "GLOBAL_TRADER_BOOK_ID", "VOLCKER_TRADING_DESK", "CHARGE_REPORTING_UNIT", "CHARGE_REPORTING_PARENT", 
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  "DATA_SOURCE", "SOURCE_SYSTEM_ID", "COMMENTS") AS 
  SELECT ROWNUM AS ID,asofdate,workflow_type_id,book_id,global_trader_book_id,volcker_trading_desk,charge_reporting_unit,charge_reporting_parent, 
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  data_source,source_system_id, comments 
FROM bh_workflow WHERE workflow_type_id in (2,3,4,6) order by workflow_type_id, book_id, global_trader_book_id;
